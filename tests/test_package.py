"""Test suite for the package-level items."""

from dabapush_gbq import __version__


def test_version():
    """Should be the correct version of the package."""
    assert __version__ == "0.1.1a3"
