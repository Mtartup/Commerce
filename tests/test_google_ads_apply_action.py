from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from commerce.connectors.base import ConnectorContext
from commerce.connectors.google_ads import GoogleAdsConnector

CID = "8666829099"


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _make_connector(mode: str = "api") -> GoogleAdsConnector:
    ctx = ConnectorContext(
        connector_id="con_google_test",
        platform="google",
        name="Google Ads Test",
        config={"mode": mode},
    )
    repo = MagicMock()
    return GoogleAdsConnector(ctx, repo)


def _proposal(action_type: str, entity_type: str, entity_id: str, payload: dict) -> dict:
    return {
        "action_type": action_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "payload_json": json.dumps(payload),
    }


def _mock_row(**attrs):
    """Create a mock GAQL row with nested attribute access."""
    row = MagicMock()
    for path, val in attrs.items():
        parts = path.split(".")
        obj = row
        for part in parts[:-1]:
            obj = getattr(obj, part)
        setattr(obj, parts[-1], val)
    return row


def _setup_client(search_rows_sequence=None):
    """
    Build a mock Google Ads client with pre-wired services.

    search_rows_sequence: list of lists — each inner list is the rows
    returned by successive ga_service.search() calls.
    """
    client = MagicMock()
    # Enum constants used throughout
    client.enums.CampaignStatusEnum.PAUSED = "PAUSED"
    client.enums.CampaignStatusEnum.ENABLED = "ENABLED"
    client.enums.AdGroupStatusEnum.PAUSED = "PAUSED"
    client.enums.AdGroupStatusEnum.ENABLED = "ENABLED"
    client.enums.AdGroupCriterionStatusEnum.PAUSED = "PAUSED"
    client.enums.AdGroupCriterionStatusEnum.ENABLED = "ENABLED"
    client.enums.KeywordMatchTypeEnum.EXACT = "EXACT"
    client.enums.KeywordMatchTypeEnum.BROAD = "BROAD"
    client.enums.KeywordMatchTypeEnum.PHRASE = "PHRASE"

    ga_service = MagicMock()
    if search_rows_sequence is not None:
        ga_service.search.side_effect = [list(rows) for rows in search_rows_sequence]

    services: dict[str, MagicMock] = {
        "GoogleAdsService": ga_service,
        "CampaignService": MagicMock(),
        "AdGroupService": MagicMock(),
        "AdGroupCriterionService": MagicMock(),
        "CampaignBudgetService": MagicMock(),
        "CampaignCriterionService": MagicMock(),
    }

    def get_service(name: str) -> MagicMock:
        return services.get(name, MagicMock())

    client.get_service.side_effect = get_service
    return client, services


# ------------------------------------------------------------------ #
# Tests                                                                #
# ------------------------------------------------------------------ #


def test_pause_campaign():
    connector = _make_connector()

    before_row = _mock_row(**{"campaign.status": "ENABLED"})
    client, services = _setup_client(search_rows_sequence=[[before_row]])

    mutate_result = MagicMock()
    mutate_result.resource_name = f"customers/{CID}/campaigns/111"
    services["CampaignService"].mutate_campaigns.return_value.results = [mutate_result]

    proposal = _proposal("pause_entity", "campaign", "111", {"op": "pause"})

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "pause_entity"
    assert result["entity_type"] == "campaign"
    assert result["entity_id"] == "111"
    assert result["before"]["status"] == "ENABLED"
    assert result["after"]["status"] == "PAUSED"
    assert result["resource_name"] == f"customers/{CID}/campaigns/111"
    services["CampaignService"].mutate_campaigns.assert_called_once()
    call_args = services["CampaignService"].mutate_campaigns.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_pause_adgroup():
    connector = _make_connector()

    before_row = _mock_row(**{"ad_group.status": "ENABLED"})
    client, services = _setup_client(search_rows_sequence=[[before_row]])

    mutate_result = MagicMock()
    mutate_result.resource_name = f"customers/{CID}/adGroups/222"
    services["AdGroupService"].mutate_ad_groups.return_value.results = [mutate_result]

    proposal = _proposal("pause_entity", "adgroup", "222", {"op": "pause"})

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "pause_entity"
    assert result["entity_type"] == "adgroup"
    assert result["before"]["status"] == "ENABLED"
    assert result["after"]["status"] == "PAUSED"
    services["AdGroupService"].mutate_ad_groups.assert_called_once()
    call_args = services["AdGroupService"].mutate_ad_groups.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_pause_keyword_with_parent():
    """parent_id in payload → only one _query_single call for before-status."""
    connector = _make_connector()

    before_row = _mock_row(**{"ad_group_criterion.status": "ENABLED"})
    client, services = _setup_client(search_rows_sequence=[[before_row]])

    proposal = _proposal(
        "pause_entity", "keyword", "333", {"op": "pause", "parent_id": "222"}
    )

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "pause_entity"
    assert result["entity_type"] == "keyword"
    assert result["entity_id"] == "333"
    assert result["before"]["status"] == "ENABLED"
    assert result["after"]["status"] == "PAUSED"
    assert "222~333" in result["resource_name"]
    services["AdGroupCriterionService"].mutate_ad_group_criteria.assert_called_once()
    call_args = services["AdGroupCriterionService"].mutate_ad_group_criteria.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_set_budget():
    connector = _make_connector()

    budget_rn = f"customers/{CID}/campaignBudgets/999"
    row_campaign = _mock_row(**{"campaign.campaign_budget": budget_rn})
    row_budget = _mock_row(**{"campaign_budget.amount_micros": 30_000_000})
    client, services = _setup_client(
        search_rows_sequence=[[row_campaign], [row_budget]]
    )

    proposal = _proposal("set_budget", "campaign", "111", {"budget": 50000})

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "set_budget"
    assert result["entity_type"] == "campaign"
    assert result["before"]["amount_micros"] == 30_000_000
    assert result["before"]["budget_krw"] == 30
    assert result["after"]["budget_krw"] == 50000
    assert result["after"]["amount_micros"] == 50_000_000_000
    assert result["resource_name"] == budget_rn
    services["CampaignBudgetService"].mutate_campaign_budgets.assert_called_once()
    call_args = services["CampaignBudgetService"].mutate_campaign_budgets.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_set_bid_adgroup():
    connector = _make_connector()

    before_row = _mock_row(**{"ad_group.cpc_bid_micros": 500_000_000})
    client, services = _setup_client(search_rows_sequence=[[before_row]])

    mutate_result = MagicMock()
    mutate_result.resource_name = f"customers/{CID}/adGroups/222"
    services["AdGroupService"].mutate_ad_groups.return_value.results = [mutate_result]

    proposal = _proposal("set_bid", "adgroup", "222", {"bid": 700})

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "set_bid"
    assert result["entity_type"] == "adgroup"
    assert result["before"]["cpc_bid_micros"] == 500_000_000
    assert result["before"]["bid_krw"] == 500
    assert result["after"]["bid_krw"] == 700
    assert result["after"]["cpc_bid_micros"] == 700_000_000
    services["AdGroupService"].mutate_ad_groups.assert_called_once()
    call_args = services["AdGroupService"].mutate_ad_groups.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_add_negatives_campaign():
    connector = _make_connector()
    client, services = _setup_client()

    neg_result = MagicMock()
    neg_result.resource_name = f"customers/{CID}/campaignCriteria/111~12345"
    services["CampaignCriterionService"].mutate_campaign_criteria.return_value.results = [
        neg_result
    ]

    proposal = _proposal(
        "add_negatives",
        "campaign",
        "111",
        {"keywords": [{"text": "free trial", "match_type": "EXACT"}]},
    )

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            result = asyncio.run(connector.apply_action(proposal))

    assert result["action"] == "add_negatives"
    assert result["entity_type"] == "campaign"
    assert result["after"]["count"] == 1
    assert result["after"]["added"] == [f"customers/{CID}/campaignCriteria/111~12345"]
    services["CampaignCriterionService"].mutate_campaign_criteria.assert_called_once()
    call_args = services["CampaignCriterionService"].mutate_campaign_criteria.call_args
    assert call_args.kwargs.get("customer_id") == CID


def test_simulated_mode():
    """import/fixture modes return simulated=True without touching the API."""
    for mode in ("import", "fixture"):
        connector = _make_connector(mode=mode)
        proposal = _proposal("pause_entity", "campaign", "111", {"op": "pause"})
        result = asyncio.run(connector.apply_action(proposal))
        assert result["simulated"] is True
        assert result["mode"] == mode
        assert result["action_type"] == "pause_entity"


def test_unknown_action_raises():
    """Unrecognised action_type raises ValueError (propagated from thread)."""
    connector = _make_connector()
    client, _ = _setup_client()

    proposal = _proposal("do_magic", "campaign", "111", {})

    with patch.object(connector, "_google_client", return_value=client):
        with patch.object(connector, "_google_customer_id", return_value=CID):
            with pytest.raises(ValueError, match="Unsupported action_type"):
                asyncio.run(connector.apply_action(proposal))
