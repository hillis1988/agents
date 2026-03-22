"""Shared pytest fixtures for the snowflake-analyst-ide test suite."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_connection():
    """Return a MagicMock SnowflakeConnection with a mock cursor that returns empty results.

    The cursor's fetchall() returns [] and description is set to an empty list
    by default, so tests that don't care about result data work without extra setup.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    return mock_conn


@pytest.fixture
def mock_session_state(mock_connection):
    """Patch st.session_state with a plain dict pre-populated with a mock connection.

    Yields the dict so tests can read and write session state keys directly
    without a running Streamlit app.
    """
    session = {"conn": mock_connection}

    with patch("utilities.snowflake_connector.st") as mock_st:
        mock_st.session_state = session
        yield session
