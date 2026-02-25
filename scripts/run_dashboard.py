#!/usr/bin/env python3
"""Step 5: Launch the Streamlit dashboard."""

import subprocess
import sys
from pathlib import Path

APP_PATH = Path(__file__).resolve().parent.parent / "ai_opportunity_index" / "dashboard" / "app.py"


def main():
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(APP_PATH), "--server.headless", "true"],
        check=True,
    )


if __name__ == "__main__":
    main()
