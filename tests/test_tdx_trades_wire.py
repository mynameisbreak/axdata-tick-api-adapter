from __future__ import annotations

import sys
from pathlib import Path

_TDX_PROVIDER_SRC = str(Path(__file__).resolve().parents[1] / "packages" / "axdata-source-tdx" / "src")
sys.path.insert(0, _TDX_PROVIDER_SRC)

import struct

from axdata_source_tdx._tdx_wire.protocol.commands import build_command_frame, parse_command_response
from axdata_source_tdx._tdx_wire.protocol.constants import TYPE_HISTORICAL_TRADES, TYPE_TODAY_TRADES
from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame

if sys.path and sys.path[0] == _TDX_PROVIDER_SRC:
    sys.path.pop(0)


# Real 0x0FC5 payload: its first multi-byte volume field, a601, decodes to 102.
LIVE_TRADE_PAYLOAD_HEX = (
    "0a008003bb10a6010b00008003008803060000800300b104080000800300110200008003003a010000"
    "80034198020601008003019f02070000800341860107010081030109010000840341b0699f040200"
)


def test_build_today_trades_frame_uses_0fc5_payload():
    frame = build_command_frame(
        TYPE_TODAY_TRADES,
        {"code": "000001.SZ", "start": 115, "count": 1800},
        13,
    )

    assert frame.msg_type == TYPE_TODAY_TRADES
    assert frame.data.hex() == "000030303030303173000807"


def test_build_historical_trades_frame_uses_0fc6_payload():
    frame = build_command_frame(
        TYPE_HISTORICAL_TRADES,
        {"code": "000001.SZ", "trade_date": "20260511", "start": 0, "count": 900},
        13,
    )

    assert frame.msg_type == TYPE_HISTORICAL_TRADES
    assert frame.data.hex() == "9f263501000030303030303100008403"


def test_parse_today_trades_payload_decodes_price_volume_and_side():
    payload = (
        (3).to_bytes(2, "little")
        + _trade_record(14 * 60 + 8, 108600, 89, 9, 0, 0)
        + _trade_record(14 * 60 + 8, 0, 22, 5, 0, 0)
        + _trade_record(14 * 60 + 8, -100, 86, 8, 1, 0)
    )
    response = ResponseFrame(
        control=0,
        msg_id=1,
        msg_type=TYPE_TODAY_TRADES,
        zip_length=len(payload),
        length=len(payload),
        data=payload,
        raw=b"",
    )

    series = parse_command_response(
        TYPE_TODAY_TRADES,
        response,
        {"code": "sz000001", "start": 115, "count": 1800},
    )

    assert series.full_code == "sz000001"
    assert series.trade_date is None
    assert series.count == 3
    assert [record.trade_time.isoformat(timespec="minutes") for record in series.records] == [
        "14:08",
        "14:08",
        "14:08",
    ]
    assert [record.absolute_index for record in series.records] == [115, 116, 117]
    assert [record.price for record in series.records] == [10.86, 10.86, 10.85]
    assert [record.volume for record in series.records] == [89, 22, 86]
    assert [record.order_count for record in series.records] == [9, 5, 8]
    assert [record.side for record in series.records] == ["buy", "buy", "sell"]


def test_parse_today_trades_payload_matches_real_tdx_compact_integer_encoding():
    payload = bytes.fromhex(LIVE_TRADE_PAYLOAD_HEX)
    response = ResponseFrame(
        control=0x1C,
        msg_id=4,
        msg_type=TYPE_TODAY_TRADES,
        zip_length=len(payload),
        length=len(payload),
        data=payload,
        raw=b"",
    )

    series = parse_command_response(
        TYPE_TODAY_TRADES,
        response,
        {"code": "sz000001", "start": 0, "count": 10},
    )

    assert series.count == 10
    assert series.records[0].trade_time.isoformat(timespec="minutes") == "14:56"
    assert series.records[0].price_acc_raw == 1083
    assert series.records[0].volume == 102
    assert series.records[0].order_count == 11
    assert series.records[0].status_raw == 0


def test_parse_historical_trades_payload_decodes_trade_datetime_and_status_5():
    payload = (
        (2).to_bytes(2, "little")
        + struct.pack("<f", 886.0)
        + _trade_record(14 * 60 + 12, 9439100, 15, 13, 1, 0)
        + _trade_record(15 * 60 + 5, 100, 32, 23, 5, 0)
    )
    response = ResponseFrame(
        control=0,
        msg_id=1,
        msg_type=TYPE_HISTORICAL_TRADES,
        zip_length=len(payload),
        length=len(payload),
        data=payload,
        raw=b"",
    )

    series = parse_command_response(
        TYPE_HISTORICAL_TRADES,
        response,
        {"code": "sz300308", "trade_date": "20260511"},
    )

    assert series.full_code == "sz300308"
    assert series.trade_date.isoformat() == "2026-05-11"
    assert series.price_base_raw_f32 == 886.0
    assert series.count == 2
    assert [record.trade_datetime.isoformat(timespec="seconds") for record in series.records] == [
        "2026-05-11T14:12:00+08:00",
        "2026-05-11T15:05:00+08:00",
    ]
    assert [record.price for record in series.records] == [943.91, 943.92]
    assert [record.side for record in series.records] == ["sell", "status_5"]


def test_parse_historical_trades_empty_payload_keeps_price_base():
    payload = (0).to_bytes(2, "little") + struct.pack("<f", 35.5)
    response = ResponseFrame(
        control=0,
        msg_id=1,
        msg_type=TYPE_HISTORICAL_TRADES,
        zip_length=len(payload),
        length=len(payload),
        data=payload,
        raw=b"",
    )

    series = parse_command_response(
        TYPE_HISTORICAL_TRADES,
        response,
        {"code": "sz300302", "trade_date": "20260511"},
    )

    assert series.count == 0
    assert series.price_base_raw_f32 == 35.5


def _trade_record(
    time_minutes: int,
    price_delta_raw: int,
    volume: int,
    order_count: int,
    status_raw: int,
    tail_raw: int,
) -> bytes:
    return (
        time_minutes.to_bytes(2, "little")
        + _signed_varint(price_delta_raw)
        + _signed_varint(volume)
        + _signed_varint(order_count)
        + _signed_varint(status_raw)
        + _signed_varint(tail_raw)
    )


def _signed_varint(value: int) -> bytes:
    sign = 0x40 if value < 0 else 0
    remaining = abs(value)
    first_value = remaining & 0x3F
    remaining >>= 6
    first = first_value | sign
    if remaining:
        first |= 0x80
    out = [first]
    while remaining:
        byte = remaining & 0x7F
        remaining >>= 7
        if remaining:
            byte |= 0x80
        out.append(byte)
    return bytes(out)
