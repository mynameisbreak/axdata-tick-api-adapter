from __future__ import annotations

import pytest
from axdata_core.source_errors import SourceRequestValidationError
from fastapi.testclient import TestClient

import apps.api.tick_adapter_routes as tick_routes
from apps.api.main import app


class FakeClient:
    def __init__(self, result=None, *, fail_with_report_type=False):
        self.result = result or []
        self.fail_with_report_type = fail_with_report_type
        self.calls = []

    def call(self, interface_name, **kwargs):
        self.calls.append((interface_name, kwargs))
        if interface_name == "stock_intraday_sina":
            page = kwargs.get("page", 1)
            day = kwargs["date"]
            if page == 1:
                return [
                    {
                        "tick_time": f"{day} 09:{minute:02}:00",
                        "price": 10,
                        "volume": 1,
                    }
                    for minute in range(60)
                ]
            return []
        if self.fail_with_report_type and "report_type" in kwargs:
            raise ValueError("unsupported report_type")
        return self.result

    def daily(self, **kwargs):
        return self.result

    def adj_factor(self, **kwargs):
        return self.result

    def stock_basic_exchange(self, **kwargs):
        return self.result


@pytest.fixture
def fake_client(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(tick_routes, "_get_client", lambda: client)
    return client


def test_tick_routes_require_token_when_api_is_remote(tmp_path, monkeypatch):
    monkeypatch.setenv("AXDATA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("AXDATA_API_HOST", "0.0.0.0")
    monkeypatch.setenv("AXDATA_API_TOKEN", "secret")
    client = TestClient(app, client=("192.168.1.10", 5000))

    assert client.get("/tick/health").status_code == 401
    assert client.get("/tick/health", headers={"Authorization": "Bearer secret"}).status_code == 200


def test_single_minute_filters_inclusive_start_and_end_times(fake_client):
    response = TestClient(app).get(
        "/tick/minute",
        params={"code": "000001.SZ", "start": "2026-06-01 09:30", "end": "2026-06-01 09:30"},
    )

    assert response.status_code == 200
    assert [bar["datetime"] for bar in response.json()["data"]] == ["2026-06-01 09:30:00"]


def test_batch_minute_rejects_ranges_longer_than_40_days(fake_client):
    response = TestClient(app).post(
        "/tick/minute",
        json={
            "symbols": ["000001.SZ"],
            "start_time": "2026-01-01T00:00:00",
            "end_time": "2026-02-11T00:00:00",
        },
    )

    assert response.status_code == 422
    assert "40" in response.text


def test_batch_endpoints_reject_too_many_symbols(fake_client):
    response = TestClient(app).post(
        "/tick/daily",
        json={"symbols": [f"000{i:03}.SZ" for i in range(101)]},
    )

    assert response.status_code == 422


def test_financial_report_type_fallback_is_shared_safely(fake_client, monkeypatch):
    from threading import Event, Lock
    from time import sleep

    client = FakeClient(result=[{"ok": True}])
    calls_lock = Lock()
    report_type_calls = []
    first_probe_failed = Event()

    def call(interface_name, **kwargs):
        with calls_lock:
            client.calls.append((interface_name, kwargs))
        if "report_type" in kwargs:
            report_type_calls.append(kwargs["stock"])
            sleep(0.02)
            first_probe_failed.set()
            raise SourceRequestValidationError("report_type is unsupported")
        if first_probe_failed.is_set():
            sleep(0.02)
        return [{"stock": kwargs["stock"]}]

    client.call = call
    monkeypatch.setattr(tick_routes, "_get_client", lambda: client)
    response = TestClient(app).post(
        "/tick/financial",
        json={"symbols": ["000001.SZ", "000002.SZ"], "report_type": "income"},
    )

    assert response.status_code == 200
    assert len(response.json()["data"]) == 2
    assert len(report_type_calls) == 1


def test_financial_keeps_report_type_when_source_fails(fake_client, monkeypatch):
    client = FakeClient(result=[{"stock": "000002.SZ"}])
    calls = []

    def call(interface_name, **kwargs):
        calls.append(kwargs)
        if kwargs["stock"] == "000001.SZ":
            raise RuntimeError("upstream temporarily unavailable")
        return [{"stock": kwargs["stock"]}]

    client.call = call
    monkeypatch.setattr(tick_routes, "_get_client", lambda: client)
    response = TestClient(app).post(
        "/tick/financial",
        json={"symbols": ["000001.SZ", "000002.SZ"], "report_type": "income"},
    )

    assert response.status_code == 200
    assert response.json()["data"] == [{"stock": "000002.SZ"}]
    assert all(kwargs.get("report_type") == "income" for kwargs in calls)


def test_batch_minute_fetches_all_intraday_pages(fake_client):
    response = TestClient(app).post(
        "/tick/minute",
        json={
            "symbols": ["000001.SZ"],
            "start_time": "2026-06-01T09:00:00",
            "end_time": "2026-06-01T16:00:00",
        },
    )

    assert response.status_code == 200
    pages = [
        kwargs.get("page")
        for name, kwargs in fake_client.calls
        if name == "stock_intraday_sina"
    ]
    assert pages == [1, 2]


def test_single_minute_fetches_cross_day_ranges(fake_client):
    response = TestClient(app).get(
        "/tick/minute",
        params={"code": "000001.SZ", "start": "2026-06-01 23:59", "end": "2026-06-02 00:00"},
    )

    assert response.status_code == 200
    queried_dates = sorted(
        {
            kwargs["date"]
            for name, kwargs in fake_client.calls
            if name == "stock_intraday_sina"
        }
    )
    assert queried_dates == ["20260601", "20260602"]


def test_realtime_rejects_too_many_symbols(fake_client):
    response = TestClient(app).get(
        "/tick/realtime",
        params={"symbols": ",".join(f"000{i:03}.SZ" for i in range(101))},
    )

    assert response.status_code == 422


def test_single_minute_rejects_unsupported_period(fake_client):
    response = TestClient(app).get(
        "/tick/minute",
        params={"code": "000001.SZ", "start": "2026-06-01", "end": "2026-06-01", "period": "5m"},
    )

    assert response.status_code == 422


def test_single_minute_full_returns_more_than_800_bars(fake_client, monkeypatch):
    monkeypatch.setattr(
        tick_routes,
        "_fetch_intraday_day",
        lambda client, symbol, day: [
            {
                "tick_time": (
                f"{day} {hour:02}:{minute:02}:00"
                if minute_of_day < 60
                else f"{day} {10 + minute_of_day // 60:02}:{minute_of_day % 60:02}:00"
            ),
                "price": 10,
                "volume": 1,
            }
            for minute_of_day in range(240)
            for hour, minute in [(9 + minute_of_day // 60, minute_of_day % 60)]
        ],
    )
    response = TestClient(app).get(
        "/tick/minute",
        params={
            "code": "000001.SZ",
            "start": "2026-06-01",
            "end": "2026-06-05",
            "full": "true",
        },
    )

    assert response.status_code == 200
    assert len(response.json()["data"]) == 1200


def test_single_minute_interprets_timezone_aware_bounds_in_shanghai(fake_client):
    response = TestClient(app).get(
        "/tick/minute",
        params={
            "code": "000001.SZ",
            "start": "2026-06-01T01:30:00Z",
            "end": "2026-06-01T01:30:00Z",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["datetime"] == "2026-06-01 09:30:00"
