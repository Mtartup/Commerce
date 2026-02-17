from __future__ import annotations

import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import typer

from commerce.config import Settings
from commerce.db import AdsDB
from commerce.executor import ExecutionError, execute_proposal
from commerce.importers.cafe24_orders import Cafe24OrdersImportOptions, import_cafe24_orders_csv
from commerce.importers.google_export import GoogleImportOptions, import_google_ads_csv
from commerce.importers.meta_export import MetaImportOptions, import_meta_ads_csv
from commerce.importers.naver_searchad import NaverImportOptions, import_naver_searchad_csv
from commerce.importers.standard import import_daily_csv, import_intraday_csv
from commerce.repo import Repo
from commerce.notify.telegram_bot import run_telegram_bot
from commerce.registry import build_connector
from commerce.web.app import run_web
from commerce.worker import run_tick, run_worker

app = typer.Typer(no_args_is_help=True)
import_app = typer.Typer(no_args_is_help=True)
app.add_typer(import_app, name="import")


@app.command("db")
def db_cmd(
    action: str = typer.Argument(..., help="init|seed"),
) -> None:
    settings = Settings.load()
    db = AdsDB(settings.db_path)
    if action == "init":
        db.init()
        typer.echo(f"OK db init: {settings.db_path}")
        return
    if action == "seed":
        db.seed_default_connectors()
        typer.echo("OK db seeded default connectors")
        return
    raise typer.BadParameter("action must be one of: init, seed")


@app.command("web")
def web_cmd() -> None:
    settings = Settings.load()
    run_web(settings)


@app.command("bot")
def bot_cmd() -> None:
    settings = Settings.load()
    run_telegram_bot(settings)


@app.command("worker")
def worker_cmd() -> None:
    settings = Settings.load()
    run_worker(settings)


@app.command("tick")
def tick_cmd() -> None:
    settings = Settings.load()
    run_tick(settings)


@app.command("backfill")
def backfill_cmd(
    platform: str = typer.Option(..., help="naver|meta|google"),
    days: int | None = typer.Option(None, help="Backfill N days ending at `until` (KST)."),
    since: str | None = typer.Option(None, help="YYYY-MM-DD (KST). Overrides `days` if set."),
    until: str | None = typer.Option(
        None,
        help="YYYY-MM-DD (KST). Defaults to today (or yesterday if include_today=0).",
    ),
    chunk_days: int = typer.Option(7, help="Chunk size in days (avoid huge API calls)."),
    include_today: bool = typer.Option(
        False,
        help="Include today's partial data (not recommended unless you need intraday-ish reporting).",
    ),
    connector_name: str | None = typer.Option(None, help="Optional connector name match if multiple connectors exist."),
) -> None:
    """
    Backfill historical daily metrics into SQLite.

    Defaults (when `days`/`since` are omitted) interpret 'all' as a sensible platform window:
    - naver: 730d
    - meta: 1095d
    - google: 1460d
    """
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)

    p = (platform or "").strip().lower()
    if p not in {"naver", "meta", "google"}:
        typer.echo("ERROR: platform must be one of: naver|meta|google")
        raise typer.Exit(code=2)

    connectors = [c for c in repo.list_connectors() if c["platform"] == p]
    if connector_name:
        connectors = [c for c in connectors if str(c.get("name") or "").strip() == connector_name.strip()]
    if not connectors:
        typer.echo(f"ERROR: no connector found for platform={p}")
        raise typer.Exit(code=2)
    if len(connectors) > 1 and not connector_name:
        choices = ", ".join(f"{x['name']} ({x['id']})" for x in connectors)
        typer.echo(
            "ERROR: multiple connectors found. Use --connector-name to pick one.\n"
            f"candidates: {choices}"
        )
        raise typer.Exit(code=2)
    c = connectors[0]

    tz = ZoneInfo(settings.timezone)
    end_str = until.strip() if until else datetime.now(tz=tz).date().isoformat()
    try:
        end_d = datetime.fromisoformat(end_str).date()
    except Exception:
        typer.echo("ERROR: until must be YYYY-MM-DD")
        raise typer.Exit(code=2)

    if not include_today:
        today_kst = datetime.now(tz=tz).date()
        if end_d >= today_kst:
            end_d = today_kst - timedelta(days=1)

    if since:
        try:
            start_d = datetime.fromisoformat(since.strip()).date()
        except Exception:
            typer.echo("ERROR: since must be YYYY-MM-DD")
            raise typer.Exit(code=2)
    else:
        if days is None:
            days = {"naver": 730, "meta": 1095, "google": 1460}.get(p, 365)
        if days <= 0:
            typer.echo("ERROR: days must be > 0")
            raise typer.Exit(code=2)
        start_d = end_d - timedelta(days=int(days) - 1)

    if end_d < start_d:
        typer.echo("Nothing to backfill (end < start).")
        return
    if chunk_days <= 0:
        chunk_days = 7

    connector = build_connector(
        p,
        connector_id=str(c["id"]),
        name=str(c["name"]),
        config_json=str(c.get("config_json") or "{}"),
        repo=repo,
        demo_mode=settings.demo_mode,
    )

    async def _run() -> None:
        ok, err = await connector.health_check()
        if not ok and not settings.demo_mode:
            raise RuntimeError(err or "health_check failed")

        try:
            await connector.sync_entities()
        except NotImplementedError:
            pass

        cur = start_d
        key = f"{p}:{c['id']}:last_fetch_daily"
        while cur <= end_d:
            chunk_end = min(cur + timedelta(days=int(chunk_days) - 1), end_d)
            repo.set_meta(key, "")
            await connector.fetch_metrics_daily(cur.isoformat(), chunk_end.isoformat())
            typer.echo(f"OK {p} {cur.isoformat()} ~ {chunk_end.isoformat()}")
            cur = chunk_end + timedelta(days=1)

    try:
        asyncio.run(_run())
    except Exception as e:  # noqa: BLE001
        typer.echo(f"ERROR: {type(e).__name__}: {e}")
        raise typer.Exit(code=2) from e


@app.command("execute")
def execute_cmd(
    proposal_id: str = typer.Argument(..., help="action_proposals.id"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    try:
        asyncio.run(execute_proposal(settings, repo=repo, proposal_id=proposal_id, actor="cli"))
        typer.echo("OK executed")
    except ExecutionError as e:
        raise typer.Exit(code=2) from e


@import_app.command("naver")
def import_naver_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="CSV export path"),
    product_type: str = typer.Option(
        "powerlink",
        help="powerlink|powercontent|shoppingsearch",
    ),
    level: str = typer.Option(
        "campaign",
        help="campaign|adgroup|keyword",
    ),
    day: str | None = typer.Option(None, help="Override day (YYYY-MM-DD) if CSV lacks date column"),
    account_id: str | None = typer.Option(None, help="Optional account id label"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)

    pt = product_type.strip().lower()
    if pt not in {"powerlink", "powercontent", "shoppingsearch"}:
        typer.echo("ERROR: product_type must be one of: powerlink, powercontent, shoppingsearch")
        raise typer.Exit(code=2)

    opts = NaverImportOptions(
        product_type=pt,
        level=level.strip().lower(),
        account_id=account_id,
        day_override=day,
    )
    res = import_naver_searchad_csv(repo, path=file, opts=opts)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


@import_app.command("intraday")
def import_intraday_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="Commerce standard intraday CSV"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    res = import_intraday_csv(repo, path=file)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


@import_app.command("daily")
def import_daily_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="Commerce standard daily CSV"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    res = import_daily_csv(repo, path=file)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


@import_app.command("meta")
def import_meta_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="Meta Ads Manager CSV export path"),
    level: str = typer.Option("campaign", help="campaign|adset|ad"),
    day: str | None = typer.Option(None, help="Override day (YYYY-MM-DD) if CSV lacks date column"),
    account_id: str | None = typer.Option(None, help="Optional account id label"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)

    lvl = level.strip().lower()
    if lvl not in {"campaign", "adset", "ad"}:
        typer.echo("ERROR: level must be one of: campaign, adset, ad")
        raise typer.Exit(code=2)

    opts = MetaImportOptions(level=lvl, account_id=account_id, day_override=day)
    res = import_meta_ads_csv(repo, path=file, opts=opts)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


@import_app.command("google")
def import_google_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="Google Ads CSV export path"),
    level: str = typer.Option("campaign", help="campaign|adgroup|keyword"),
    day: str | None = typer.Option(None, help="Override day (YYYY-MM-DD) if CSV lacks date column"),
    account_id: str | None = typer.Option(None, help="Optional account id label"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)

    lvl = level.strip().lower()
    if lvl not in {"campaign", "adgroup", "keyword"}:
        typer.echo("ERROR: level must be one of: campaign, adgroup, keyword")
        raise typer.Exit(code=2)

    opts = GoogleImportOptions(level=lvl, account_id=account_id, day_override=day)
    res = import_google_ads_csv(repo, path=file, opts=opts)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


@import_app.command("cafe24-orders")
def import_cafe24_orders_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="Cafe24 orders export CSV"),
    day: str | None = typer.Option(None, help="Override day (YYYY-MM-DD) if CSV lacks date column"),
) -> None:
    settings = Settings.load()
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    opts = Cafe24OrdersImportOptions(
        store="cafe24",
        timezone=settings.timezone,
        day_override=day,
    )
    res = import_cafe24_orders_csv(repo, path=file, opts=opts)
    if not res.get("ok"):
        typer.echo(f"ERROR: {res.get('error')}")
        raise typer.Exit(code=2)
    typer.echo(json_dumps(res))


def json_dumps(obj) -> str:
    import json

    return json.dumps(obj, ensure_ascii=True, indent=2)
