from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from axdata_source_tdx._tdx_wire.transport import pool as pool_module
from axdata_source_tdx._tdx_wire.transport import socket as socket_module


class _FakeSlotTransport:
    def __init__(self, *, label: str) -> None:
        self.label = label
        self.connected_host = None
        self.calls: list[int] = []
        self.active = 0
        self.max_active = 0
        self.closed = False
        self.started = threading.Event()
        self.release = threading.Event()

    def connect(self) -> None:
        self.connected_host = self.label

    def close(self) -> None:
        self.closed = True
        self.release.set()

    def execute(self, command: int, payload=None, *, timeout=None):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls.append(command)
        self.started.set()
        try:
            self.release.wait(timeout=timeout)
            return {"slot": self.label, "command": command}
        finally:
            self.active -= 1

    def request(self, command: str) -> str:
        return command


def test_pool_admission_is_fifo_and_never_shares_a_slot() -> None:
    pool = pool_module.PooledSocketTransport(hosts=["a:7709"], pool_size=1, heartbeat_interval=None)
    first = _FakeSlotTransport(label="slot-0")
    pool._transports[0] = first

    results: list[dict[str, object]] = []

    def run(command: int) -> None:
        results.append(pool.execute(command))

    first.release.clear()
    one = threading.Thread(target=run, args=(1,))
    two = threading.Thread(target=run, args=(2,))
    one.start()
    assert first.started.wait(timeout=1)
    two.start()
    time.sleep(0.05)
    assert first.max_active == 1
    first.release.set()
    one.join(timeout=2)
    two.join(timeout=2)
    assert [item["command"] for item in results] == [1, 2]
    assert first.max_active == 1
    pool.close()


def test_pool_close_wakes_admission_waiters() -> None:
    pool = pool_module.PooledSocketTransport(hosts=["a:7709"], pool_size=1, heartbeat_interval=None)
    first = _FakeSlotTransport(label="slot-0")
    pool._transports[0] = first
    first.release.clear()
    holder = threading.Thread(target=lambda: pool.execute(1))
    holder.start()
    assert first.started.wait(timeout=1)

    outcome: list[type[BaseException]] = []

    def waiting() -> None:
        try:
            pool.execute(2)
        except BaseException as exc:  # noqa: BLE001 - assertion target
            outcome.append(type(exc))

    waiter = threading.Thread(target=waiting)
    waiter.start()
    time.sleep(0.05)
    pool.close()
    waiter.join(timeout=2)
    holder.join(timeout=2)
    assert outcome == [socket_module.ConnectionClosedError]


def test_socket_actor_is_single_owner_and_can_reopen(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = socket_module.SocketTransport(hosts=["unused:7709"], heartbeat_interval=None)
    calls: list[int] = []
    active = 0
    max_active = 0
    gate = threading.Event()

    def fake_execute(ticket, *, stop_event):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        calls.append(ticket.command)
        gate.wait(timeout=1)
        active -= 1
        return ticket.command

    monkeypatch.setattr(transport, "_ensure_socket_actor", lambda **_: None)
    monkeypatch.setattr(transport, "_execute_actor", fake_execute)
    first = transport.execute(1, timeout=2)
    assert first == 1
    assert max_active == 1
    actor_before = transport.actor_thread
    transport.close()
    assert not actor_before.is_alive()
    second = transport.execute(2, timeout=2)
    assert second == 2
    assert transport.actor_thread is not actor_before
    transport.close()


def test_actor_push_queue_is_bounded() -> None:
    transport = socket_module.SocketTransport(hosts=["unused:7709"], heartbeat_interval=None)
    response = SimpleNamespace(msg_id=1, msg_type=2)
    for _ in range(socket_module._PUSH_QUEUE_SIZE + 10):
        transport._publish_push_actor(response)
    assert len(transport.drain_push()) == socket_module._PUSH_QUEUE_SIZE
    transport.close()
