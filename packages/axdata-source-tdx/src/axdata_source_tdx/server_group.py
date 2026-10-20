"""Multi-server scheduling above the Actor-owned TDX connection pools."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import ExitStack, suppress
from dataclasses import dataclass
from math import ceil
from typing import Any, Generic, TypeVar

from axdata_core.source_errors import SourceUnavailableError

BatchT = TypeVar("BatchT")
ValueT = TypeVar("ValueT")


@dataclass(frozen=True)
class ServerGroupOptions:
    """Upper bounds for a hosts-by-connections batch topology."""

    hosts: tuple[str, ...]
    connections_per_server: int

    @property
    def capacity(self) -> int:
        return len(self.hosts) * self.connections_per_server


@dataclass(frozen=True)
class ServerGroupBatch(Generic[BatchT]):
    index: int
    value: BatchT


@dataclass(frozen=True)
class ServerGroupResult(Generic[ValueT]):
    values: list[ValueT]
    client_metas: list[dict[str, Any]]
    configured_host_count: int
    active_host_count: int
    healthy_host_count: int
    failed_hosts: tuple[str, ...]
    max_connections_per_server: int
    connections_per_server: int
    concurrency_capacity: int
    concurrency_limit: int
    retry_count: int


@dataclass(frozen=True)
class _ConnectedClient:
    host: str
    client: Any


def plan_server_group(
    options: ServerGroupOptions,
    work_item_count: int,
) -> ServerGroupOptions:
    """Shrink configured limits to the resources needed by this workload."""

    item_count = max(0, int(work_item_count))
    if item_count == 0 or not options.hosts:
        return ServerGroupOptions(hosts=(), connections_per_server=0)
    host_count = min(item_count, len(options.hosts))
    connections_per_server = min(
        options.connections_per_server,
        ceil(item_count / host_count),
    )
    return ServerGroupOptions(
        hosts=options.hosts[:host_count],
        connections_per_server=connections_per_server,
    )


def execute_server_group_batches(
    batches: Sequence[BatchT],
    *,
    options: ServerGroupOptions,
    create_client: Callable[..., Any],
    request_batch: Callable[[Any, BatchT], ValueT],
    client_meta: Callable[[Any], Mapping[str, Any]],
    max_retries: int = 1,
) -> ServerGroupResult[ValueT]:
    """Run indexed batches on independent per-host pools with bounded retry."""

    indexed_batches = [
        ServerGroupBatch(index=index, value=value)
        for index, value in enumerate(batches)
    ]
    if not indexed_batches:
        return ServerGroupResult(
            values=[],
            client_metas=[],
            configured_host_count=len(options.hosts),
            active_host_count=0,
            healthy_host_count=0,
            failed_hosts=(),
            max_connections_per_server=options.connections_per_server,
            connections_per_server=0,
            concurrency_capacity=0,
            concurrency_limit=0,
            retry_count=0,
        )
    if not options.hosts:
        raise SourceUnavailableError("No TDX 7709 hosts are configured for this batch request.")

    plan = plan_server_group(options, len(indexed_batches))

    created_clients: list[_ConnectedClient] = []
    connected: list[_ConnectedClient] = []
    failed_hosts: list[str] = []
    first_error: Exception | None = None
    try:
        candidate_index = 0
        target_host_count = len(plan.hosts)
        while len(connected) < target_host_count and candidate_index < len(options.hosts):
            needed = target_host_count - len(connected)
            candidate_hosts = options.hosts[candidate_index : candidate_index + needed]
            candidate_index += len(candidate_hosts)
            wave: list[_ConnectedClient] = []
            for host in candidate_hosts:
                try:
                    client = create_client(
                        hosts=[host],
                        pool_size=plan.connections_per_server,
                        heartbeat_interval=None,
                    )
                except Exception as exc:
                    failed_hosts.append(host)
                    if first_error is None:
                        first_error = exc
                    continue
                item = _ConnectedClient(host=host, client=client)
                created_clients.append(item)
                wave.append(item)
            if not wave:
                continue
            with ThreadPoolExecutor(
                max_workers=len(wave),
                thread_name_prefix="axdata-tdx-server-connect",
            ) as executor:
                futures = {
                    executor.submit(_connect_client, item.client): item
                    for item in wave
                }
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        failed_hosts.append(item.host)
                        if first_error is None:
                            first_error = exc
                    else:
                        connected.append(item)

        connected.sort(key=lambda item: options.hosts.index(item.host))
        if not connected:
            if first_error is not None:
                raise SourceUnavailableError(
                    f"None of the {len(options.hosts)} configured TDX servers could connect."
                ) from first_error
            raise SourceUnavailableError("No TDX 7709 server is available for this batch request.")

        capacity = len(connected) * plan.connections_per_server
        concurrency_limit = min(len(indexed_batches), capacity)
        results, retry_count = _execute_connected_batches(
            indexed_batches,
            connected=connected,
            connections_per_server=plan.connections_per_server,
            request_batch=request_batch,
            max_retries=max(0, int(max_retries)),
        )
        metas = [dict(client_meta(item.client)) for item in connected]
        return ServerGroupResult(
            values=[results[index] for index in range(len(indexed_batches))],
            client_metas=metas,
            configured_host_count=len(options.hosts),
            active_host_count=target_host_count,
            healthy_host_count=len(connected),
            failed_hosts=tuple(host for host in options.hosts if host in failed_hosts),
            max_connections_per_server=options.connections_per_server,
            connections_per_server=plan.connections_per_server,
            concurrency_capacity=capacity,
            concurrency_limit=concurrency_limit,
            retry_count=retry_count,
        )
    finally:
        for item in created_clients:
            with suppress(Exception):
                _close_client(item.client)


def _execute_connected_batches(
    batches: Sequence[ServerGroupBatch[BatchT]],
    *,
    connected: Sequence[_ConnectedClient],
    connections_per_server: int,
    request_batch: Callable[[Any, BatchT], ValueT],
    max_retries: int,
) -> tuple[dict[int, ValueT], int]:
    results: dict[int, ValueT] = {}
    retry_count = 0
    with ExitStack() as stack:
        executors = [
            stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=min(connections_per_server, len(batches)),
                    thread_name_prefix=f"axdata-tdx-server-{index}",
                )
            )
            for index, _item in enumerate(connected)
        ]
        pending: dict[Future[ValueT], tuple[ServerGroupBatch[BatchT], int, int]] = {}
        for index, batch in enumerate(batches):
            host_index = index % len(connected)
            future = executors[host_index].submit(
                request_batch,
                connected[host_index].client,
                batch.value,
            )
            pending[future] = (batch, host_index, 0)

        while pending:
            future = next(as_completed(tuple(pending)))
            batch, host_index, attempt = pending.pop(future)
            try:
                results[batch.index] = future.result()
            except Exception:
                if attempt >= max_retries or len(connected) < 2:
                    raise
                retry_host_index = (host_index + attempt + 1) % len(connected)
                retry = executors[retry_host_index].submit(
                    request_batch,
                    connected[retry_host_index].client,
                    batch.value,
                )
                pending[retry] = (batch, retry_host_index, attempt + 1)
                retry_count += 1
    return results, retry_count


def _connect_client(client: Any) -> None:
    if hasattr(client, "connect"):
        client.connect()


def _close_client(client: Any) -> None:
    if hasattr(client, "close"):
        client.close()
