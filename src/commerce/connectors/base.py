from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class ConnectorCapabilities:
    read_metrics: bool = False
    read_entities: bool = False
    read_orders: bool = False
    write_pause: bool = False
    write_budget: bool = False
    write_bid: bool = False
    write_negatives: bool = False
    read_creatives: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "read_metrics": self.read_metrics,
            "read_entities": self.read_entities,
            "read_orders": self.read_orders,
            "write_pause": self.write_pause,
            "write_budget": self.write_budget,
            "write_bid": self.write_bid,
            "write_negatives": self.write_negatives,
            "read_creatives": self.read_creatives,
        }


@dataclass(frozen=True)
class ConnectorContext:
    connector_id: str
    platform: str
    name: str
    config: dict[str, Any]


class BaseConnector(Protocol):
    capabilities: ConnectorCapabilities

    async def health_check(self) -> tuple[bool, str | None]:
        """Return (ok, error). Must never raise."""

    async def sync_entities(self) -> None:
        """Fetch accounts/campaigns/adsets/keywords metadata and upsert to DB."""

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        """Fetch daily metrics and upsert to DB."""

    async def fetch_metrics_intraday(self, day: str) -> None:
        """Fetch intraday metrics (hour buckets) for a KST day and upsert to DB."""

    async def apply_action(self, proposal: dict[str, Any]) -> dict[str, Any]:
        """Apply an approved action. Must raise on hard failure, return result JSON on success."""
