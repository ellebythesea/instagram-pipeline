"""Streamlit entrypoint for the workspace UI."""

from pathlib import Path
import runpy


runpy.run_path(Path(__file__).parent / "pages" / "workspace.py", run_name="__main__")
