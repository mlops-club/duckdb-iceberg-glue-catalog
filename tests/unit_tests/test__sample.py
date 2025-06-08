"""
A minimal test file to ensure CI passes even without user-added tests.

This serves as both an example and a placeholder.
"""


def test_dummy():
    """A simple test that always passes. Ensures CI pipeline succeeds with no user tests."""
    assert True


def test_with_fixture(sample_fixture):
    """Example of using a fixture. Always passes."""
    assert sample_fixture is True
