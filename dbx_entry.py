import os
import sys

# Ensure repo src/ is on path when executed as a Databricks job
repo_root = os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv and sys.argv[0] else os.getcwd()
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from gbdp.cli import main

if __name__ == "__main__":
    main()
