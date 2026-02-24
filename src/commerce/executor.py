from __future__ import annotations

import json
from typing import Any

from commerce.config import Settings
from commerce.registry import build_connector
from commerce.repo import Repo
from commerce.util import now_utc_iso


class ExecutionError(RuntimeError):
    pass


async def execute_proposal(
    settings: Settings,
    *,
    repo: Repo,
    proposal_id: str,
    actor: str,
) -> dict[str, Any]:
    """
    Execute an action proposal via its connector and write an audit log.

    Rules:
    - If requires_approval=1, proposal must be status=approved.
    - If requires_approval=0, proposal can be executed from proposed/approved.
    """
    proposal = repo.get_proposal(proposal_id)
    if not proposal:
        raise ExecutionError(f"proposal not found: {proposal_id}")

    requires_approval = int(proposal.get("requires_approval") or 1) == 1
    if requires_approval and proposal.get("status") != "approved":
        raise ExecutionError("proposal requires approval before execution")

    connector_id = proposal.get("connector_id")
    if not connector_id:
        raise ExecutionError("proposal missing connector_id")

    connector_row = repo.get_connector(str(connector_id))
    if not connector_row:
        raise ExecutionError("connector not found for proposal")

    connector = build_connector(
        connector_row["platform"],
        connector_id=connector_row["id"],
        name=connector_row["name"],
        config_json=connector_row["config_json"],
        repo=repo,
        demo_mode=settings.demo_mode,
    )

    exec_id = repo.create_execution(proposal_id)
    started = now_utc_iso()
    try:
        result = await connector.apply_action(proposal)
        repo.finish_execution(exec_id, status="success", before_json=proposal, after_json=result, error=None)
        repo.set_proposal_result(
            proposal_id,
            status="executed",
            executed_at=started,
            result_json={"actor": actor, "result": result},
            error=None,
        )
        return result
    except Exception as e:  # noqa: BLE001 - record error, do not crash caller
        err = f"{type(e).__name__}: {e}"
        repo.finish_execution(exec_id, status="failed", before_json=proposal, after_json=None, error=err)
        repo.set_proposal_result(
            proposal_id,
            status="failed",
            executed_at=started,
            result_json={"actor": actor},
            error=err,
        )
        raise ExecutionError(err) from e


def proposal_payload(proposal: dict[str, Any]) -> dict[str, Any]:
    raw = proposal.get("payload_json") or "{}"
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}

