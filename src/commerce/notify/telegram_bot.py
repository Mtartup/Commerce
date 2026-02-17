from __future__ import annotations

import asyncio
from typing import Any

import httpx

from commerce.config import Settings
from commerce.db import AdsDB
from commerce.executor import ExecutionError, execute_proposal
from commerce.repo import Repo


TELEGRAM_API = "https://api.telegram.org"


def _kb_for_proposal(proposal_id: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "Approve", "callback_data": f"approve:{proposal_id}"},
                {"text": "Reject", "callback_data": f"reject:{proposal_id}"},
            ]
        ]
    }


async def _send_message(
    client: httpx.AsyncClient,
    *,
    token: str,
    chat_id: int,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = await client.post(f"{TELEGRAM_API}/bot{token}/sendMessage", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


async def _answer_callback(
    client: httpx.AsyncClient,
    *,
    token: str,
    callback_query_id: str,
    text: str,
) -> None:
    r = await client.post(
        f"{TELEGRAM_API}/bot{token}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id, "text": text},
        timeout=20,
    )
    r.raise_for_status()


async def _poll_updates(
    client: httpx.AsyncClient,
    *,
    token: str,
    offset: int | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"timeout": 30}
    if offset is not None:
        params["offset"] = offset
    r = await client.get(f"{TELEGRAM_API}/bot{token}/getUpdates", params=params, timeout=40)
    r.raise_for_status()
    data = r.json()
    return data.get("result", [])


async def _run(settings: Settings) -> None:
    if not settings.telegram_bot_token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in .env")

    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)

    allowed_chat_id = settings.telegram_allowed_chat_id
    if allowed_chat_id is None:
        saved = repo.get_meta("telegram_allowed_chat_id")
        if saved:
            try:
                allowed_chat_id = int(saved)
            except ValueError:
                allowed_chat_id = None

    last_offset_raw = repo.get_meta("telegram_update_offset")
    offset = int(last_offset_raw) if last_offset_raw else None

    async with httpx.AsyncClient() as client:
        while True:
            updates = await _poll_updates(client, token=settings.telegram_bot_token, offset=offset)
            for u in updates:
                offset = int(u["update_id"]) + 1
                repo.set_meta("telegram_update_offset", str(offset))

                msg = u.get("message")
                if msg:
                    chat_id = int(msg["chat"]["id"])
                    if allowed_chat_id is None:
                        # Auto-lock to the first chat that messages the bot.
                        allowed_chat_id = chat_id
                        repo.set_meta("telegram_allowed_chat_id", str(chat_id))
                        await _send_message(
                            client,
                            token=settings.telegram_bot_token,
                            chat_id=chat_id,
                            text=f"Locked to this chat (chat_id={chat_id}).",
                        )
                    if chat_id != allowed_chat_id:
                        # Silent ignore to avoid leaking anything to other chats.
                        continue
                    text = (msg.get("text") or "").strip()
                    if text == "/status":
                        pending = repo.list_pending_proposals(limit=10)
                        await _send_message(
                            client,
                            token=settings.telegram_bot_token,
                            chat_id=chat_id,
                            text=f"Pending proposals: {len(pending)}",
                        )
                    elif text.startswith("/execute"):
                        parts = text.split()
                        if len(parts) != 2:
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text="Usage: /execute <proposal_id>",
                            )
                            continue
                        proposal_id = parts[1].strip()
                        prop = repo.get_proposal(proposal_id)
                        if not prop:
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text="Proposal not found.",
                            )
                            continue
                        try:
                            await execute_proposal(settings, repo=repo, proposal_id=proposal_id, actor="telegram")
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text="Executed.",
                            )
                        except ExecutionError as e:
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text=f"Execution failed: {e}",
                            )
                    elif text == "/pending":
                        pending = repo.list_pending_proposals(limit=10)
                        if not pending:
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text="No pending proposals.",
                            )
                        else:
                            lines = [
                                f"- {p['id']} [{p['platform']}] {p['action_type']} {p['entity_type']}:{p['entity_id']}"
                                for p in pending
                            ]
                            await _send_message(
                                client,
                                token=settings.telegram_bot_token,
                                chat_id=chat_id,
                                text="Pending:\n" + "\n".join(lines),
                            )
                    continue

                cb = u.get("callback_query")
                if cb:
                    msg = cb.get("message") or {}
                    chat_id = int((msg.get("chat") or {}).get("id") or 0)
                    if allowed_chat_id is None:
                        await _answer_callback(
                            client,
                            token=settings.telegram_bot_token,
                            callback_query_id=cb.get("id"),
                            text="Bot not locked yet. Send any message to the bot first.",
                        )
                        continue
                    if chat_id != allowed_chat_id:
                        continue
                    cb_id = cb.get("id")
                    data = cb.get("data") or ""
                    if ":" not in data:
                        await _answer_callback(
                            client,
                            token=settings.telegram_bot_token,
                            callback_query_id=cb_id,
                            text="Bad callback data",
                        )
                        continue
                    verb, proposal_id = data.split(":", 1)
                    if verb == "approve":
                        repo.set_proposal_status(proposal_id, "approved", actor="telegram")
                        await _answer_callback(
                            client,
                            token=settings.telegram_bot_token,
                            callback_query_id=cb_id,
                            text="Approved",
                        )
                    elif verb == "reject":
                        repo.set_proposal_status(proposal_id, "rejected", actor="telegram")
                        await _answer_callback(
                            client,
                            token=settings.telegram_bot_token,
                            callback_query_id=cb_id,
                            text="Rejected",
                        )
                    else:
                        await _answer_callback(
                            client,
                            token=settings.telegram_bot_token,
                            callback_query_id=cb_id,
                            text="Unknown action",
                        )
                    continue

            await asyncio.sleep(0.1)


def run_telegram_bot(settings: Settings) -> None:
    asyncio.run(_run(settings))


def _resolve_allowed_chat_id(settings: Settings) -> int | None:
    if settings.telegram_allowed_chat_id:
        return int(settings.telegram_allowed_chat_id)
    try:
        saved = Repo(settings.db_path).get_meta("telegram_allowed_chat_id")
        return int(saved) if saved else None
    except Exception:
        return None


async def notify_new_proposal(settings: Settings, proposal: dict[str, Any]) -> tuple[int, int] | None:
    """
    Worker helper: send a proposal into Telegram with inline approve/reject buttons.
    """
    if not settings.telegram_bot_token:
        return None
    chat_id = _resolve_allowed_chat_id(settings)
    if not chat_id:
        return None

    text = (
        f"[{proposal['platform']}] {proposal['action_type']}\n"
        f"{proposal['entity_type']}:{proposal['entity_id']}\n"
        f"risk={proposal.get('risk','low')} approval={proposal.get('requires_approval', 1)}\n"
        f"reason: {proposal.get('reason','')}"
    )
    async with httpx.AsyncClient() as client:
        res = await _send_message(
            client,
            token=settings.telegram_bot_token,
            chat_id=int(chat_id),
            text=text,
            reply_markup=_kb_for_proposal(proposal["id"]),
        )
        msg = res.get("result") or {}
        return int(msg.get("chat", {}).get("id")), int(msg.get("message_id"))


async def notify_auto_pause(settings: Settings, proposal: dict[str, Any]) -> tuple[int, int] | None:
    """
    Send an auto-guardrail message (no buttons).
    """
    if not settings.telegram_bot_token:
        return None
    chat_id = _resolve_allowed_chat_id(settings)
    if not chat_id:
        return None

    text = (
        f"[AUTO-PAUSE] [{proposal['platform']}] {proposal['action_type']}\n"
        f"{proposal['entity_type']}:{proposal['entity_id']}\n"
        f"reason: {proposal.get('reason','')}\n"
        f"status: {proposal.get('status','')}"
    )
    async with httpx.AsyncClient() as client:
        res = await _send_message(
            client,
            token=settings.telegram_bot_token,
            chat_id=int(chat_id),
            text=text,
            reply_markup=None,
        )
        msg = res.get("result") or {}
        return int(msg.get("chat", {}).get("id")), int(msg.get("message_id"))
