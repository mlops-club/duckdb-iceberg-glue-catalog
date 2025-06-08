"""
Non specific fixtures for use across all tests.

This fixture is accessible to all tests due to its inclusion in conftest.py.

see: https://docs.pytest.org/en/6.2.x/fixture.html
"""

import pytest


@pytest.fixture()
def sample_fixture():
    """Place holder, replace with custom fixtures as needed."""
    # setup code goes here -- This runs before the test
    # e.g. create temporary files, set up database connections, etc.

    # yield is used to return the fixture value
    # e.g. return a database connection, a temporary file, etc.
    yield True

    # teardown code goes here
    # e.g. close database connection, delete temporary files, etc.
