"""DuckDB AI Pipeline — Entry point.

Usage:
    uv run python main.py              # Run full pipeline
    uv run python main.py generate     # Generate fake data only
    uv run python main.py serve        # Start FastAPI server
    uv run python main.py test         # Run tests
"""

from __future__ import annotations

import sys


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "pipeline"

    if cmd == "generate":
        from scripts.generate_data import main as gen_main
        gen_main()
    elif cmd == "serve":
        from app.api import run as serve_run
        serve_run()
    elif cmd == "test":
        import subprocess
        subprocess.run(["uv", "run", "pytest", "tests/", "-v", "--tb=short"])
    elif cmd == "pipeline":
        from scripts.run_pipeline import main as pipe_main
        pipe_main()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: uv run python main.py [generate|pipeline|serve|test]")


if __name__ == "__main__":
    main()
