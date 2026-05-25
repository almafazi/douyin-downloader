"""Standalone entrypoint for tiktok API server using Granian (Rust HTTP server)."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import ConfigLoader
from server.app import build_app

config_path = os.environ.get("CONFIG_PATH", "config.yml")
host = os.environ.get("SERVER_HOST", "0.0.0.0")
port = int(os.environ.get("SERVER_PORT", "8089"))

config = ConfigLoader(config_path)
app = build_app(config)


def main():
    from granian import Granian
    server = Granian(
        target="server.run_tiktok_server:app",
        address=host,
        port=port,
        interface="asgi",
        log_enabled=True,
        log_access=True,
    )
    server.serve()


if __name__ == "__main__":
    main()
