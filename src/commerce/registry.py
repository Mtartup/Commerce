from __future__ import annotations

import json
from typing import Any

from commerce.connectors.base import ConnectorContext
from commerce.connectors.demo import DemoConnector
from commerce.connectors.google_ads import GoogleAdsConnector
from commerce.connectors.meta_ads import MetaAdsConnector
from commerce.connectors.naver_searchad import NaverSearchAdConnector
from commerce.connectors.tiktok_ads import TikTokAdsConnector
from commerce.connectors.coupang import CoupangConnector
from commerce.connectors.smartstore import SmartStoreConnector
from commerce.connectors.cafe24_analytics import Cafe24AnalyticsConnector


class _ConnectorScopedRepo:
    """
    Inject connector_id into connector-owned entity/metric upserts.
    Other repository APIs are transparently forwarded.
    """

    def __init__(self, repo, connector_id: str):
        self._repo = repo
        self._connector_id = connector_id

    def upsert_entity(self, **kwargs: Any) -> None:
        if "connector_id" not in kwargs or kwargs["connector_id"] is None:
            kwargs["connector_id"] = self._connector_id
        self._repo.upsert_entity(**kwargs)

    def upsert_metric_daily(self, **kwargs: Any) -> None:
        if "connector_id" not in kwargs or kwargs["connector_id"] is None:
            kwargs["connector_id"] = self._connector_id
        self._repo.upsert_metric_daily(**kwargs)

    def upsert_metric_intraday(self, **kwargs: Any) -> None:
        if "connector_id" not in kwargs or kwargs["connector_id"] is None:
            kwargs["connector_id"] = self._connector_id
        self._repo.upsert_metric_intraday(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repo, name)


def build_connector(
    platform: str,
    *,
    connector_id: str,
    name: str,
    config_json: str,
    repo,
    demo_mode: bool,
):
    config = json.loads(config_json or "{}")
    ctx = ConnectorContext(connector_id=connector_id, platform=platform, name=name, config=config)
    scoped_repo = _ConnectorScopedRepo(repo, connector_id=connector_id)

    if demo_mode:
        return DemoConnector(ctx, scoped_repo)

    if platform == "naver":
        return NaverSearchAdConnector(ctx, scoped_repo)
    if platform == "meta":
        return MetaAdsConnector(ctx, scoped_repo)
    if platform == "google":
        return GoogleAdsConnector(ctx, scoped_repo)
    if platform == "tiktok":
        return TikTokAdsConnector(ctx, scoped_repo)
    if platform == "coupang":
        return CoupangConnector(ctx, scoped_repo)
    if platform == "smartstore":
        return SmartStoreConnector(ctx, scoped_repo)
    if platform == "cafe24_analytics":
        return Cafe24AnalyticsConnector(ctx, scoped_repo)

    raise ValueError(f"Unknown platform: {platform}")

