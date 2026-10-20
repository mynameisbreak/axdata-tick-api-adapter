"""TDX data-root scoped cache helpers owned by the provider package."""

from __future__ import annotations

import os
from pathlib import Path


def tdx_server_cache_root(data_root: str | Path | None) -> str | None:
    """Return the data-root scoped TDX server cache directory."""

    if data_root in (None, ""):
        return None
    return str(Path(data_root).expanduser().resolve() / "cache" / "tdx_servers")


def tdx_stats_cache_root(data_root: str | Path | None) -> str | None:
    """Return the environment override or data-root scoped stats cache."""

    configured = os.getenv("AXDATA_TDX_STATS_ROOT", "").strip()
    if configured:
        return str(Path(configured).expanduser().resolve())
    if data_root in (None, ""):
        return None
    return str(Path(data_root).expanduser().resolve() / "cache" / "tdx" / "stats")
