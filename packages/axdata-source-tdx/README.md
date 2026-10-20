# axdata-source-tdx

TDX quote source Provider for AxData.

This package provides the TDX data source plugin shape for AxData. It exposes the ordinary TDX quote interfaces through the `axdata.providers` entry point and embeds `axdata-provider.json` as package data.

The current implementation owns the TDX catalog, adapter, wire client, server configuration, connection options, F10 helpers, caches, and downloader profile projection. When installed as part of the official `axdata` package, this Provider is available by default and can still be disabled through AxData plugin configuration. If this package is missing or explicitly disabled, AxData reports that the TDX plugin should be checked instead of running a core fallback.

## Connection topology

TDX request options use these meanings:

- `hosts` selects the candidate TDX servers. `source_server_count` is the maximum server count.
- `connections_per_server` is the maximum Actor/socket count in each selected server's independent pool.
- `pool_size` is a compatibility option for one ordinary failover client. It does not multiply by `source_server_count`.

Large realtime snapshot requests (more than one 80-code protocol batch) use the explicit server group when connection options are supplied. The scheduler shrinks the configured limits to the current protocol-batch count: it uses at most one server per batch and opens only enough connections per active server to cover the work. For example, three batches with a `10 x 8` limit use `3 x 1`, while 65 batches use `10 x 7`. A server that cannot connect is replaced from the remaining candidate list when possible, and a failed batch is retried once on another healthy server. Returned rows are restored to input-code order. Small requests and adapters constructed with an injected client keep the ordinary single-client failover path.

The configured explicit server-group ceiling is capped at 128 Actor/socket slots; actual allocation is capped again by the current workload. Start with a conservative limit such as 5 servers by 4 connections and increase it only after measuring the selected public servers.
