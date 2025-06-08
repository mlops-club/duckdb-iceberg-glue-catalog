"""Specify fixtures and constants used during pytest tests."""

import sys
from pathlib import Path

THIS_DIR = Path(__file__).parent
TESTS_DIR_PARENT = (THIS_DIR / "..").resolve()

# add the parent directory of tests/ to PYTHONPATH
# so that we can use "from tests.<module> import ..." in our tests and fixtures
sys.path.insert(0, str(TESTS_DIR_PARENT))


# Tell pytest where fixtures are located
# There should be single entry for each fixture (python) file in tests/fixtures
pytest_plugins = [
    "fixtures.general_fixtures",
]
