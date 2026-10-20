"""Bounded FIFO connection pool for Actor-owned 7709 transports."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from importlib import import_module
from time import monotonic
from typing import TYPE_CHECKING, Any

from axdata_source_tdx._tdx_wire._connection_defaults import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_PROBE_TIMEOUT,
    DEFAULT_PROBE_WORKERS,
)
from axdata_source_tdx._tdx_wire._host_utils import unique_hosts

if TYPE_CHECKING:
    from .socket import SocketTransport

_STDLIB_EXPORTS = {"itertools", "threading"}


class PooledSocketTransport:
    """FIFO admission pool where each slot owns one Actor and one socket."""

    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        pool_size: int = 2,
        probe_hosts: bool = False,
        probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
        probe_workers: int = DEFAULT_PROBE_WORKERS,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
        max_pending_requests: int | None = None,
    ) -> None:
        resolved_hosts = _resolve_hosts(hosts)
        if not resolved_hosts:
            raise ValueError("at least one host is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if probe_hosts and len(resolved_hosts) > 1:
            from axdata_source_tdx._tdx_wire._host_probe import sort_hosts_by_latency

            resolved_hosts = sort_hosts_by_latency(resolved_hosts, timeout=probe_timeout, max_workers=probe_workers)
        self._hosts = resolved_hosts
        self._timeout = float(timeout)
        self._pool_size = max(1, int(pool_size))
        self._heartbeat_interval = heartbeat_interval
        self._max_pending_requests = max(
            self._pool_size,
            int(max_pending_requests) if max_pending_requests is not None else self._pool_size * 4,
        )
        self._transports: list[SocketTransport | None] = [None] * self._pool_size
        _itertools_module()
        self._idle_slots: deque[int] = deque(range(self._pool_size))
        self._waiters: deque[object] = deque()
        self._condition = _threading_module().Condition(_threading_module().Lock())
        self._closed = False
        self._generation = 0

    @property
    def hosts(self) -> tuple[str, ...]:
        return tuple(self._hosts)

    @property
    def pool_size(self) -> int:
        return self._pool_size

    @property
    def heartbeat_interval(self) -> float | None:
        return self._heartbeat_interval

    @property
    def max_pending_requests(self) -> int:
        return self._max_pending_requests

    @property
    def connected_hosts(self) -> tuple[str | None, ...]:
        with self._condition:
            transports = tuple(self._transports)
        return tuple(transport.connected_host if transport is not None else None for transport in transports)

    @property
    def connected_host(self) -> str | None:
        return next((host for host in self.connected_hosts if host is not None), None)

    def connect(self) -> None:
        self._reopen_if_closed()
        for index in range(self._pool_size):
            self._transport_at(index).connect()

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._generation += 1
            transports = tuple(transport for transport in self._transports if transport is not None)
            self._idle_slots.clear()
            self._condition.notify_all()
        for transport in transports:
            transport.close()

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        self._reopen_if_closed()
        deadline = monotonic() + self._timeout
        index, generation = self._acquire_slot(deadline)
        try:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise _wire_exceptions()[2]("7709 request timed out during pool admission")
            transport = self._transport_for_generation(index, generation)
            return transport.execute(command, payload, timeout=remaining)
        finally:
            self._release_slot(index, generation)

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        self._reopen_if_closed()
        index, generation = self._acquire_slot(monotonic() + self._timeout)
        try:
            return self._transport_for_generation(index, generation).request(command)
        finally:
            self._release_slot(index, generation)

    def _acquire_slot(self, deadline: float) -> tuple[int, int]:
        waiter = object()
        with self._condition:
            if self._closed:
                raise _wire_exceptions()[0]("7709 pool is closed")
            if len(self._waiters) >= self._max_pending_requests:
                raise _wire_exceptions()[3]("7709 pool admission queue is full")
            self._waiters.append(waiter)
            try:
                while True:
                    if self._closed:
                        raise _wire_exceptions()[0]("7709 pool closed during admission")
                    if self._waiters[0] is waiter and self._idle_slots:
                        self._waiters.popleft()
                        return self._idle_slots.popleft(), self._generation
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        self._waiters.remove(waiter)
                        raise _wire_exceptions()[2]("7709 request timed out during pool admission")
                    self._condition.wait(timeout=remaining)
            except BaseException:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass
                self._condition.notify_all()
                raise

    def _release_slot(self, index: int, generation: int) -> None:
        with self._condition:
            if generation == self._generation and index not in self._idle_slots and not self._closed:
                self._idle_slots.append(index)
            self._condition.notify_all()

    def _reopen_if_closed(self) -> None:
        with self._condition:
            if not self._closed:
                return
            self._closed = False
            self._generation += 1
            self._transports = [None] * self._pool_size
            self._idle_slots = deque(range(self._pool_size))
            self._waiters.clear()
            self._condition.notify_all()

    def _transport_at(self, index: int) -> SocketTransport:
        with self._condition:
            return self._transport_at_locked(index)

    def _transport_for_generation(self, index: int, generation: int) -> SocketTransport:
        with self._condition:
            if self._closed or generation != self._generation:
                raise _wire_exceptions()[0]("7709 pool generation is closed")
            return self._transport_at_locked(index)

    def _transport_at_locked(self, index: int) -> SocketTransport:
        if self._closed:
            raise _wire_exceptions()[0]("7709 pool is closed")
        transport = self._transports[index]
        if transport is None:
            from .socket import SocketTransport

            transport = SocketTransport(
                hosts=_rotate_hosts(self._hosts, index),
                timeout=self._timeout,
                heartbeat_interval=self._heartbeat_interval,
            )
            self._transports[index] = transport
        return transport


def _rotate_hosts(hosts: list[str], offset: int) -> list[str]:
    if not hosts:
        return []
    index = offset % len(hosts)
    return hosts[index:] + hosts[:index]


def _resolve_hosts(hosts: Sequence[str] | None) -> list[str]:
    if hosts:
        return unique_hosts(list(hosts))
    from axdata_source_tdx._tdx_wire._host_resource import DEFAULT_HOSTS
    return unique_hosts(list(DEFAULT_HOSTS))


def _wire_exceptions() -> tuple[type[BaseException], type[BaseException], type[BaseException], type[BaseException]]:
    from axdata_source_tdx._tdx_wire.exceptions import ConnectionClosedError, ProtocolError, ResponseTimeoutError, TransportError
    return ConnectionClosedError, ProtocolError, ResponseTimeoutError, TransportError


def _threading_module():
    module = import_module("threading")
    globals()["threading"] = module
    return module


def _itertools_module():
    module = import_module("itertools")
    globals()["itertools"] = module
    return module


def __getattr__(name: str) -> Any:
    if name == "itertools":
        return _itertools_module()
    if name == "threading":
        return _threading_module()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STDLIB_EXPORTS)
