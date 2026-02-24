"""
Google Ads OAuth2 Refresh Token 발급 스크립트

사용 전 준비:
  1. https://console.cloud.google.com/apis/credentials 접속
  2. Google Ads API 활성화 (APIs & Services > Library)
  3. OAuth 2.0 클라이언트 ID 생성 (유형: 데스크톱 앱)
  4. Client ID, Client Secret을 .env 또는 인자로 제공

사용법:
  uv run python scripts/google_oauth.py
  uv run python scripts/google_oauth.py --client-id CLIENT_ID --client-secret CLIENT_SECRET
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Load .env from project root (two levels up from scripts/)
try:
    from dotenv import load_dotenv as _load_dotenv

    def load_dotenv(path: Path) -> None:
        _load_dotenv(path)
except ImportError:
    def load_dotenv(path: Path) -> None:  # type: ignore[misc]
        pass

load_dotenv(Path(__file__).parent.parent / ".env")

SCOPES = ["https://www.googleapis.com/auth/adwords"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Ads OAuth2 Refresh Token 발급")
    parser.add_argument("--client-id", default=None, help="OAuth2 Client ID")
    parser.add_argument("--client-secret", default=None, help="OAuth2 Client Secret")
    parser.add_argument("--port", type=int, default=8080, help="로컬 콜백 포트 (기본 8080)")
    args = parser.parse_args()

    client_id = args.client_id or os.getenv("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = args.client_secret or os.getenv("GOOGLE_ADS_CLIENT_SECRET", "").strip()

    if not client_id:
        print("[ERROR] Client ID가 필요합니다.")
        print("  .env 파일에 GOOGLE_ADS_CLIENT_ID=... 를 입력하거나")
        print("  --client-id 인자로 전달하세요.")
        sys.exit(1)

    if not client_secret:
        print("[ERROR] Client Secret이 필요합니다.")
        print("  .env 파일에 GOOGLE_ADS_CLIENT_SECRET=... 를 입력하거나")
        print("  --client-secret 인자로 전달하세요.")
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
    except ImportError:
        print("[ERROR] google-auth-oauthlib 패키지가 없습니다.")
        print("  uv add google-auth-oauthlib")
        sys.exit(1)

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uris": ["http://localhost", f"http://localhost:{args.port}"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    print("=" * 60)
    print("Google Ads OAuth2 인증")
    print("=" * 60)
    print(f"Client ID    : {client_id[:20]}...")
    print(f"Callback Port: {args.port}")
    print()
    print("브라우저가 열립니다. Google 계정으로 로그인 후 권한을 허용하세요.")
    print()

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    creds = flow.run_local_server(port=args.port, prompt="consent", access_type="offline")

    refresh_token = creds.refresh_token
    if not refresh_token:
        print("[ERROR] Refresh Token을 받지 못했습니다.")
        print("  브라우저에서 기존 권한을 취소 후 재시도하세요:")
        print("  https://myaccount.google.com/permissions")
        sys.exit(1)

    print()
    print("=" * 60)
    print("인증 성공! .env 파일에 아래 값을 추가하세요:")
    print("=" * 60)
    print(f"GOOGLE_ADS_REFRESH_TOKEN={refresh_token}")
    print("=" * 60)
    print()
    print("다음 단계:")
    print("  1. 위 Refresh Token을 .env에 입력")
    print("  2. uv run python scripts/google_oauth.py --activate-db  (DB 활성화)")
    print("  3. uv run commerce tick  (동기화 테스트)")


GOOGLE_CONNECTOR_ID = "con_GacnbKEYKytK9Q"


def activate_db() -> None:
    """DB의 Google Ads 커넥터를 API 모드로 활성화한다."""
    # Must be run from project root so Commerce imports resolve.
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / "src"))

    try:
        from commerce.repo import Repo  # type: ignore
        from commerce.config import Settings  # type: ignore
    except ImportError as e:
        print(f"[ERROR] Commerce 모듈 임포트 실패: {e}")
        print("  프로젝트 루트에서 실행하세요: uv run python scripts/google_oauth.py --activate-db")
        sys.exit(1)

    settings = Settings.load()
    repo = Repo(settings.db_path)

    connector = repo.get_connector(GOOGLE_CONNECTOR_ID)
    if connector is None:
        print(f"[ERROR] 커넥터를 찾을 수 없습니다: {GOOGLE_CONNECTOR_ID}")
        sys.exit(1)

    print(f"커넥터 발견: {connector['name']} (현재 enabled={connector['enabled']})")

    repo.set_connector_enabled(GOOGLE_CONNECTOR_ID, True)
    repo.update_connector_config(GOOGLE_CONNECTOR_ID, {"mode": "api"})

    print("[OK] Google Ads 커넥터 활성화 완료 (enabled=1, mode=api)")
    print()
    print("다음 단계: uv run commerce tick")


def run_with_args() -> None:
    # Quick check for --activate-db before full arg parsing to keep it simple.
    if "--activate-db" in sys.argv:
        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(env_path)
        activate_db()
    else:
        main()


if __name__ == "__main__":
    run_with_args()
