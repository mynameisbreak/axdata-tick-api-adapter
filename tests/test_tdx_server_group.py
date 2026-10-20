from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TDX_PACKAGE_SRC = REPO_ROOT / "packages" / "axdata-source-tdx" / "src"
sys.path.insert(0, str(TDX_PACKAGE_SRC))

from axdata_core.source_errors import SourceRequestValidationError  # noqa: E402
from axdata_source_tdx.options import (  # noqa: E402
    tdx_request_option_hosts,
    tdx_request_option_pool_size,
    tdx_server_group_options,
)
from axdata_source_tdx.server_group import (  # noqa: E402
    ServerGroupOptions,
    execute_server_group_batches,
    plan_server_group,
)


class FakeGroupClient:
    def __init__(self, host: str, pool_size: int, *, connect_error: bool = False) -> None:
        self.host = host
        self.pool_size = pool_size
        self.connect_error = connect_error
        self.connected = False
        self.closed = False
        self.calls: list[int] = []

    def connect(self) -> None:
        if self.connect_error:
            raise OSError(f"cannot connect {self.host}")
        self.connected = True

    def close(self) -> None:
        self.closed = True


def test_server_group_scales_actor_pools_to_workload_and_keeps_batch_order() -> None:
    clients: list[FakeGroupClient] = []

    def create_client(*, hosts, pool_size, heartbeat_interval):
        assert heartbeat_interval is None
        client = FakeGroupClient(hosts[0], pool_size)
        clients.append(client)
        return client

    result = execute_server_group_batches(
        list(range(20)),
        options=ServerGroupOptions(
            hosts=tuple(f"10.0.0.{index}:7709" for index in range(1, 11)),
            connections_per_server=8,
        ),
        create_client=create_client,
        request_batch=lambda client, value: (client.host, value),
        client_meta=lambda client: {"tdx_pool_size": client.pool_size},
    )

    assert len(clients) == 10
    assert {client.pool_size for client in clients} == {2}
    assert [value for _host, value in result.values] == list(range(20))
    assert result.configured_host_count == 10
    assert result.active_host_count == 10
    assert result.healthy_host_count == 10
    assert result.max_connections_per_server == 8
    assert result.connections_per_server == 2
    assert result.concurrency_capacity == 20
    assert result.concurrency_limit == 20
    assert all(client.connected and client.closed for client in clients)


def test_server_group_drops_connect_failure_and_retries_batch_on_another_server() -> None:
    clients: list[FakeGroupClient] = []
    failed_once = False

    def create_client(*, hosts, pool_size, heartbeat_interval):
        client = FakeGroupClient(
            hosts[0],
            pool_size,
            connect_error=hosts[0] == "bad:7709",
        )
        clients.append(client)
        return client

    def request_batch(client: FakeGroupClient, value: int):
        nonlocal failed_once
        client.calls.append(value)
        if client.host == "first:7709" and value == 0 and not failed_once:
            failed_once = True
            raise OSError("temporary request failure")
        return (client.host, value)

    result = execute_server_group_batches(
        [0, 1, 2],
        options=ServerGroupOptions(
            hosts=("bad:7709", "first:7709", "second:7709"),
            connections_per_server=2,
        ),
        create_client=create_client,
        request_batch=request_batch,
        client_meta=lambda client: {"host": client.host},
    )

    assert result.failed_hosts == ("bad:7709",)
    assert result.active_host_count == 3
    assert result.healthy_host_count == 2
    assert result.max_connections_per_server == 2
    assert result.connections_per_server == 1
    assert result.concurrency_capacity == 2
    assert result.retry_count == 1
    assert result.values[0] == ("second:7709", 0)
    assert [value for _host, value in result.values] == [0, 1, 2]
    assert all(client.closed for client in clients)


def test_server_group_uses_reserve_host_without_exceeding_active_limit() -> None:
    clients: list[FakeGroupClient] = []

    def create_client(*, hosts, pool_size, heartbeat_interval):
        client = FakeGroupClient(
            hosts[0],
            pool_size,
            connect_error=hosts[0] == "bad:7709",
        )
        clients.append(client)
        return client

    result = execute_server_group_batches(
        [0, 1],
        options=ServerGroupOptions(
            hosts=("bad:7709", "first:7709", "reserve:7709", "unused:7709"),
            connections_per_server=8,
        ),
        create_client=create_client,
        request_batch=lambda client, value: (client.host, value),
        client_meta=lambda client: {"host": client.host},
    )

    assert [client.host for client in clients] == ["bad:7709", "first:7709", "reserve:7709"]
    assert all(client.pool_size == 1 for client in clients)
    assert result.active_host_count == 2
    assert result.healthy_host_count == 2
    assert result.failed_hosts == ("bad:7709",)
    assert {host for host, _value in result.values} == {"first:7709", "reserve:7709"}


def test_server_group_closes_every_client_when_retry_budget_is_exhausted() -> None:
    clients: list[FakeGroupClient] = []

    def create_client(*, hosts, pool_size, heartbeat_interval):
        client = FakeGroupClient(hosts[0], pool_size)
        clients.append(client)
        return client

    def request_batch(_client, _value):
        raise OSError("still failing")

    with pytest.raises(OSError, match="still failing"):
        execute_server_group_batches(
            [0, 1],
            options=ServerGroupOptions(hosts=("one:7709", "two:7709"), connections_per_server=1),
            create_client=create_client,
            request_batch=request_batch,
            client_meta=lambda _client: {},
        )

    assert len(clients) == 2
    assert all(client.closed for client in clients)


@pytest.mark.parametrize(
    ("work_item_count", "expected_hosts", "expected_connections"),
    [
        (0, 0, 0),
        (1, 1, 1),
        (3, 3, 1),
        (20, 10, 2),
        (65, 10, 7),
        (80, 10, 8),
        (100, 10, 8),
    ],
)
def test_server_group_plan_treats_user_topology_as_limits(
    work_item_count,
    expected_hosts,
    expected_connections,
) -> None:
    plan = plan_server_group(
        ServerGroupOptions(
            hosts=tuple(f"host-{index}:7709" for index in range(10)),
            connections_per_server=8,
        ),
        work_item_count,
    )

    assert len(plan.hosts) == expected_hosts
    assert plan.connections_per_server == expected_connections


def test_connection_options_keep_hosts_and_connections_per_server_separate() -> None:
    def configured(_options):
        return ["one:7709", "two:7709", "three:7709"]

    assert tdx_request_option_pool_size(
        {"source_server_count": 3, "connections_per_server": 4}
    ) == 4
    assert tdx_request_option_hosts(
        {"hosts": ["one:7709", "two:7709"], "source_server_count": 1},
        configured_hosts=configured,
    ) == ["one:7709"]
    server_group = tdx_server_group_options(
        {"source_server_count": 3, "connections_per_server": 4},
        configured_hosts=configured,
    )
    assert server_group.hosts == ("one:7709", "two:7709", "three:7709")
    assert server_group.connections_per_server == 4

    legacy_group = tdx_server_group_options(
        {"pool_size": 6},
        configured_hosts=configured,
    )
    assert legacy_group.hosts == ("one:7709",)
    assert legacy_group.connections_per_server == 6


def test_server_group_rejects_more_than_128_actor_slots() -> None:
    with pytest.raises(SourceRequestValidationError, match="must be <= 128"):
        tdx_server_group_options(
            {"source_server_count": 20, "connections_per_server": 8},
            configured_hosts=lambda _options: [f"host-{index}:7709" for index in range(20)],
        )
