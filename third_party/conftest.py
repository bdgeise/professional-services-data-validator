from pathlib import Path
import pytest
import os


@pytest.fixture(scope='session')
def script_directory() -> Path:
    """Return the test script directory.
    Returns
    -------
    Path
        Test script directory
    """
    return Path(__file__).absolute().parents[1] / "ci"


@pytest.fixture(scope='session')
def data_directory() -> Path:
    """Return the test data directory.
    Returns
    -------
    Path
        Test data directory
    """
    root = Path(__file__).absolute().parents[1]

    return Path(
        os.environ.get(
            "IBIS_TEST_DATA_DIRECTORY",
            root / "ci" / "ibis-testing-data",
        )
    )