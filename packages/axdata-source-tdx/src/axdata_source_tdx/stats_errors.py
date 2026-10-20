"""Structured failures for TDX statistics-backed interfaces."""

from __future__ import annotations

from typing import Any

from axdata_core.source_errors import SourceUnavailableError


class TdxStatsError(SourceUnavailableError):
    """Base failure for the TDX ``zhb.zip`` statistics resource."""

    code = "TDX_STATS_ERROR"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.details = details


class TdxStatsDownloadError(TdxStatsError):
    """Raised when the statistics resource cannot be downloaded."""

    code = "TDX_STATS_DOWNLOAD_FAILED"


class TdxStatsValidationError(TdxStatsError):
    """Raised when a downloaded statistics resource is incomplete or malformed."""

    code = "TDX_STATS_RESOURCE_INVALID"


class TdxStatsDateError(TdxStatsError):
    """Raised when the upstream resource is too old for the target session."""

    code = "TDX_STATS_DATE_UNAVAILABLE"


class TdxStatsRefreshDeferredError(TdxStatsError):
    """Raised while a recent failed refresh is inside its retry delay."""

    code = "TDX_STATS_REFRESH_DEFERRED"


class TdxAuctionNotReadyError(SourceUnavailableError):
    """Raised before TDX has published the target session's auction snapshot."""

    code = "TDX_AUCTION_NOT_READY"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.details = details
