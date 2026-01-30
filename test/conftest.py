# test/conftest.py
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SUBMODULE_PATH = REPO_ROOT / "bdit_dag_utils"

if str(SUBMODULE_PATH) not in sys.path:
    sys.path.insert(0, str(SUBMODULE_PATH))
