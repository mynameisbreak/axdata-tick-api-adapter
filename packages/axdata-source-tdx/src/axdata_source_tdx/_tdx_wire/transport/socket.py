"""Actor-owned 7709 socket transport.

Each transport owns one actor thread.  The actor is the only code allowed to
touch the socket; callers submit bounded request tickets and wait for their
result.  This keeps request ordering, reconnects, heartbeats, and shutdown in
one place while preserving the synchronous transport API used by AxData.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from importlib import import_module
from time import monotonic
from typing import TYPE_CHECKING, Any

from axdata_source_tdx._tdx_wire._connection_defaults import DEFAULT_HEARTBEAT_INTERVAL
from axdata_source_tdx._tdx_wire._host_utils import unique_hosts

if TYPE_CHECKING:
    from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame

_FRAME_EXPORTS = {"ResponseFrame", "decode_response", "read_response_frame"}
_EXCEPTION_EXPORTS = {"ConnectionClosedError", "ProtocolError", "ResponseTimeoutError", "TransportError"}
_SESSION_COMMAND_EXPORTS = {"TYPE_HANDSHAKE", "TYPE_HEARTBEAT"}
_STDLIB_EXPORTS = {"Empty", "Queue", "socket", "threading"}
_ACTOR_POLL_SECONDS = 0.1
_MAILBOX_SIZE = 1
_PUSH_QUEUE_SIZE = 1024
_CONNECT_COMMAND = -1

__all__ = [
    "Any",
    "ConnectionClosedError",
    "DEFAULT_HEARTBEAT_INTERVAL",
    "Empty",
    "ProtocolError",
    "Queue",
    "ResponseTimeoutError",
    "Sequence",
    "SocketTransport",
    "TYPE_CHECKING",
    "TYPE_HANDSHAKE",
    "TYPE_HEARTBEAT",
    "TransportError",
    "import_module",
    "socket",
    "threading",
    "unique_hosts",
]


@dataclass(slots=True)
class _RequestTicket:
    command: int
    payload: dict[str, Any]
    deadline: float
    done: Any = field(default_factory=lambda: _threading_module().Event())
    result: Any = None
    error: BaseException | None = None
    cancelled: bool = False


class _StopActor:
    pass


_STOP_ACTOR = _StopActor()


class SocketTransport:
    """Synchronous facade backed by one socket-owning actor thread."""

    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
    ) -> None:
        self._hosts = _resolve_hosts(hosts)
        if not self._hosts:
            raise ValueError("at least one host is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._timeout = float(timeout)
        self._heartbeat_interval = heartbeat_interval
        threading_module = _threading_module()
        queue_cls, _, _ = _queue_exports()
        self._push_queue: Queue[ResponseFrame] = queue_cls(maxsize=_PUSH_QUEUE_SIZE)
        self._lifecycle_lock = threading_module.Lock()
        self._state_lock = threading_module.Lock()
        self._socket: socket.socket | None = None
        self._connected_host: str | None = None
        self._reader_error: BaseException | None = None
        self._handshaken = False
        self._msg_id = 1
        self._receive_buffer = bytearray()
        self._mailbox: Queue[_RequestTicket | _StopActor]
        self._stop_event: Any
        self._actor_thread: Any = None
        self.last_handshake: Any = None
        self.last_heartbeat: Any = None
        self._start_actor_locked()

    @property
    def connected_host(self) -> str | None:
        with self._state_lock:
            return self._connected_host

    @property
    def heartbeat_interval(self) -> float | None:
        return self._heartbeat_interval

    @property
    def actor_thread(self) -> Any:
        """Expose the actor for diagnostics without exposing socket state."""

        return self._actor_thread

    def connect(self) -> None:
        self._submit(_RequestTicket(_CONNECT_COMMAND, {}, monotonic() + self._timeout), timeout=self._timeout)

    def close(self) -> None:
        with self._lifecycle_lock:
            actor = self._actor_thread
            if actor is None:
                return
            self._stop_event.set()
            try:
                self._mailbox.put_nowait(_STOP_ACTOR)
            except Exception:
                pass
        if actor is not _threading_module().current_thread():
            actor.join(timeout=self._timeout + 0.5)
        with self._lifecycle_lock:
            if self._actor_thread is actor:
                self._actor_thread = None
        with self._state_lock:
            self._connected_host = None

    def execute(
        self,
        command: int,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        effective_timeout = self._timeout if timeout is None else float(timeout)
        if effective_timeout <= 0:
            raise _wire_exceptions()[2](f"7709 response timed out: 0x{command:04x}")
        ticket = _RequestTicket(
            command=command,
            payload=dict(payload or {}),
            deadline=monotonic() + effective_timeout,
        )
        return self._submit_ticket(ticket)

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    def poll_push(self, *, timeout: float | None = None) -> ResponseFrame | None:
        _, empty_error, _ = _queue_exports()
        try:
            return self._push_queue.get(timeout=timeout) if timeout is not None else self._push_queue.get_nowait()
        except empty_error:
            return None

    def drain_push(self, *, limit: int | None = None) -> tuple[ResponseFrame, ...]:
        items: list[ResponseFrame] = []
        while limit is None or len(items) < max(0, int(limit)):
            item = self.poll_push()
            if item is None:
                break
            items.append(item)
        return tuple(items)

    def _submit_ticket(self, ticket: _RequestTicket) -> Any:
        _, _, full_error = _queue_exports()
        remaining = max(0.0, ticket.deadline - monotonic())
        with self._lifecycle_lock:
            actor = self._actor_thread
            if actor is None:
                self._start_actor_locked()
                actor = self._actor_thread
            mailbox = self._mailbox
            stop_event = self._stop_event
        if stop_event.is_set() or actor is None:
            raise _wire_exceptions()[0]("7709 transport is closing")
        try:
            mailbox.put(ticket, timeout=remaining)
        except full_error as exc:
            ticket.cancelled = True
            raise _wire_exceptions()[2]("7709 request timed out during actor admission") from exc
        if not ticket.done.wait(max(0.0, ticket.deadline - monotonic())):
            ticket.cancelled = True
            raise _wire_exceptions()[2](f"7709 response timed out: 0x{ticket.command:04x}")
        if ticket.error is not None:
            raise ticket.error
        return ticket.result

    def _start_actor_locked(self) -> None:
        queue_cls, _, _ = _queue_exports()
        threading_module = _threading_module()
        self._mailbox = queue_cls(maxsize=_MAILBOX_SIZE)
        self._stop_event = threading_module.Event()
        actor = threading_module.Thread(
            target=self._actor_loop,
            args=(self._mailbox, self._stop_event),
            name="axdata_source_tdx._tdx_wire-7709-actor",
            daemon=True,
        )
        self._actor_thread = actor
        actor.start()

    def _submit(self, ticket: _RequestTicket, *, timeout: float) -> Any:
        ticket.deadline = monotonic() + timeout
        return self._submit_ticket(ticket)

    def _actor_loop(self, mailbox: Any, stop_event: Any) -> None:
        _, empty_error, _ = _queue_exports()
        next_heartbeat = monotonic() + self._heartbeat_interval if self._heartbeat_interval and self._heartbeat_interval > 0 else None
        while not stop_event.is_set():
            wait_timeout = _ACTOR_POLL_SECONDS
            if next_heartbeat is not None:
                wait_timeout = min(wait_timeout, max(0.0, next_heartbeat - monotonic()))
            try:
                ticket = mailbox.get(timeout=wait_timeout)
            except empty_error:
                if next_heartbeat is not None and monotonic() >= next_heartbeat:
                    heartbeat_command = _session_command_codes()[1]
                    heartbeat = _RequestTicket(heartbeat_command, {}, monotonic() + self._timeout)
                    self._run_ticket(heartbeat, stop_event=stop_event, is_heartbeat=True)
                    next_heartbeat = monotonic() + self._heartbeat_interval
                continue
            if ticket is _STOP_ACTOR:
                break
            if ticket.cancelled or ticket.deadline <= monotonic():
                self._finish_ticket(ticket, error=_wire_exceptions()[2]("7709 request cancelled before execution"))
                continue
            self._run_ticket(ticket, stop_event=stop_event)
        self._close_socket_actor()
        self._fail_mailbox_actor(mailbox)

    def _run_ticket(self, ticket: _RequestTicket, *, stop_event: Any, is_heartbeat: bool = False) -> None:
        connection_closed_error, protocol_error, response_timeout_error, transport_error = _wire_exceptions()
        try:
            result = self._execute_actor(ticket, stop_event=stop_event)
        except protocol_error as exc:
            self._finish_ticket(ticket, error=exc)
        except response_timeout_error as exc:
            self._close_socket_actor()
            self._finish_ticket(ticket, error=exc)
        except (OSError, connection_closed_error) as exc:
            self._close_socket_actor()
            if not stop_event.is_set() and not ticket.cancelled and ticket.deadline > monotonic():
                try:
                    result = self._execute_actor(ticket, stop_event=stop_event)
                except BaseException as retry_exc:
                    self._finish_ticket(ticket, error=transport_error(f"7709 request failed: 0x{ticket.command:04x}"))
                    ticket.error.__cause__ = retry_exc  # type: ignore[union-attr]
                else:
                    self._finish_ticket(ticket, result=result)
            else:
                self._finish_ticket(ticket, error=connection_closed_error("7709 actor connection closed"))
        except BaseException as exc:
            self._reader_error = exc
            self._finish_ticket(ticket, error=exc)
        else:
            self._finish_ticket(ticket, result=result)

        if is_heartbeat and ticket.error is not None:
            self._reader_error = ticket.error

    def _execute_actor(self, ticket: _RequestTicket, *, stop_event: Any) -> Any:
        self._ensure_socket_actor(deadline=ticket.deadline)
        if ticket.command == _CONNECT_COMMAND:
            return None
        handshake_command, heartbeat_command = _session_command_codes()
        if ticket.command != handshake_command and not self._handshaken:
            self.last_handshake = self._request_actor(handshake_command, {}, ticket, stop_event=stop_event)
            self._handshaken = True
        result = self._request_actor(ticket.command, ticket.payload, ticket, stop_event=stop_event)
        if ticket.command == handshake_command:
            self.last_handshake = result
            self._handshaken = True
        elif ticket.command == heartbeat_command:
            self.last_heartbeat = result
        return result

    def _request_actor(
        self,
        command: int,
        payload: dict[str, Any],
        ticket: _RequestTicket,
        *,
        stop_event: Any,
    ) -> Any:
        from axdata_source_tdx._tdx_wire._command_codec import build_command_frame, parse_command_response

        _, _, response_timeout_error, _ = _wire_exceptions()
        frame = build_command_frame(command, payload, self._next_msg_id())
        assert self._socket is not None
        self._socket.settimeout(max(0.01, ticket.deadline - monotonic()))
        self._socket.sendall(frame.to_bytes())
        response = self._read_response_actor(
            deadline=ticket.deadline,
            expected_msg_id=frame.msg_id,
            expected_msg_type=frame.msg_type,
            stop_event=stop_event,
        )
        if ticket.cancelled:
            raise response_timeout_error(f"7709 response timed out: 0x{command:04x}")
        return parse_command_response(command, response, payload)

    def _read_response_actor(
        self,
        *,
        deadline: float,
        expected_msg_id: int,
        expected_msg_type: int,
        stop_event: Any,
    ) -> ResponseFrame:
        from axdata_source_tdx._tdx_wire.protocol.frame import decode_response

        assert self._socket is not None
        while True:
            raw = self._pop_raw_frame_actor()
            if raw is not None:
                response = decode_response(raw)
                if response.msg_id == expected_msg_id and response.msg_type == expected_msg_type:
                    return response
                self._publish_push_actor(response)
                continue
            if stop_event.is_set():
                raise _wire_exceptions()[0]("7709 actor stopped")
            if monotonic() >= deadline:
                raise _wire_exceptions()[2]("7709 response timed out")
            self._socket.settimeout(min(_ACTOR_POLL_SECONDS, max(0.01, deadline - monotonic())))
            try:
                chunk = self._socket.recv(65536)
            except (_socket_module().timeout, TimeoutError):
                continue
            if not chunk:
                raise _wire_exceptions()[0]("socket closed by remote peer")
            self._receive_buffer.extend(chunk)

    def _pop_raw_frame_actor(self) -> bytes | None:
        prefix = b"\xb1\xcb\x74\x00"
        prefix_index = self._receive_buffer.find(prefix)
        if prefix_index < 0:
            if len(self._receive_buffer) > len(prefix) - 1:
                del self._receive_buffer[: -(len(prefix) - 1)]
            return None
        if prefix_index:
            del self._receive_buffer[:prefix_index]
        if len(self._receive_buffer) < 16:
            return None
        payload_length = int.from_bytes(self._receive_buffer[12:14], "little", signed=False)
        frame_length = 16 + payload_length
        if len(self._receive_buffer) < frame_length:
            return None
        raw = bytes(self._receive_buffer[:frame_length])
        del self._receive_buffer[:frame_length]
        return raw

    def _publish_push_actor(self, response: ResponseFrame) -> None:
        try:
            self._push_queue.put_nowait(response)
            return
        except _queue_exports()[2]:
            pass
        try:
            self._push_queue.get_nowait()
        except _queue_exports()[1]:
            pass
        try:
            self._push_queue.put_nowait(response)
        except _queue_exports()[2]:
            pass

    def _ensure_socket_actor(self, *, deadline: float | None = None) -> None:
        if self._stop_event.is_set():
            raise _wire_exceptions()[0]("7709 actor is stopping")
        if self._socket is not None and self._reader_error is None:
            return
        self._close_socket_actor()
        last_error: OSError | None = None
        socket_module = _socket_module()
        for host in self._hosts:
            address, port_text = host.rsplit(":", 1)
            try:
                connect_timeout = self._timeout
                if deadline is not None:
                    connect_timeout = min(connect_timeout, max(0.01, deadline - monotonic()))
                sock = socket_module.create_connection((address, int(port_text)), timeout=connect_timeout)
                sock.settimeout(_ACTOR_POLL_SECONDS)
            except OSError as exc:
                last_error = exc
                continue
            self._socket = sock
            self._reader_error = None
            self._handshaken = False
            self._receive_buffer.clear()
            with self._state_lock:
                self._connected_host = host
            return
        raise _wire_exceptions()[0]("unable to connect to any 7709 host") from last_error

    def _close_socket_actor(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
                self._handshaken = False
                self._receive_buffer.clear()
        with self._state_lock:
            self._connected_host = None

    def _next_msg_id(self) -> int:
        value = self._msg_id
        self._msg_id = 1 if self._msg_id >= 0xFFFFFFFF else self._msg_id + 1
        return value

    def _finish_ticket(self, ticket: _RequestTicket, *, result: Any = None, error: BaseException | None = None) -> None:
        if ticket.cancelled and error is None:
            error = _wire_exceptions()[2]("7709 request cancelled")
        ticket.result = result
        ticket.error = error
        ticket.done.set()

    def _fail_mailbox_actor(self, mailbox: Any) -> None:
        _, empty_error, _ = _queue_exports()
        while True:
            try:
                ticket = mailbox.get_nowait()
            except empty_error:
                return
            if ticket is _STOP_ACTOR:
                continue
            self._finish_ticket(ticket, error=_wire_exceptions()[0]("7709 actor closed before execution"))


def _resolve_hosts(hosts: Sequence[str] | None) -> list[str]:
    if hosts:
        return unique_hosts(list(hosts))
    from axdata_source_tdx._tdx_wire._host_resource import DEFAULT_HOSTS
    return unique_hosts(list(DEFAULT_HOSTS))


def _session_command_codes() -> tuple[int, int]:
    from axdata_source_tdx._tdx_wire._command_codes import TYPE_HANDSHAKE, TYPE_HEARTBEAT
    globals()["TYPE_HANDSHAKE"] = TYPE_HANDSHAKE
    globals()["TYPE_HEARTBEAT"] = TYPE_HEARTBEAT
    return TYPE_HANDSHAKE, TYPE_HEARTBEAT


def _wire_exceptions() -> tuple[type[BaseException], type[BaseException], type[BaseException], type[BaseException]]:
    from axdata_source_tdx._tdx_wire.exceptions import ConnectionClosedError, ProtocolError, ResponseTimeoutError, TransportError
    globals()["ConnectionClosedError"] = ConnectionClosedError
    globals()["ProtocolError"] = ProtocolError
    globals()["ResponseTimeoutError"] = ResponseTimeoutError
    globals()["TransportError"] = TransportError
    return ConnectionClosedError, ProtocolError, ResponseTimeoutError, TransportError


def _queue_exports():
    module = import_module("queue")
    globals()["Empty"] = module.Empty
    globals()["Queue"] = module.Queue
    return module.Queue, module.Empty, module.Full


def _socket_module():
    module = import_module("socket")
    globals()["socket"] = module
    return module


def _threading_module():
    module = import_module("threading")
    globals()["threading"] = module
    return module


def __getattr__(name: str) -> Any:
    if name == "socket":
        return _socket_module()
    if name == "threading":
        return _threading_module()
    if name in {"Empty", "Queue"}:
        _queue_exports()
        return globals()[name]
    if name in _EXCEPTION_EXPORTS:
        _wire_exceptions()
        return globals()[name]
    if name in _SESSION_COMMAND_EXPORTS:
        _session_command_codes()
        return globals()[name]
    if name not in _FRAME_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module("axdata_source_tdx._tdx_wire.protocol.frame")
    value = getattr(module, name)
    globals()[name] = value
    return value
