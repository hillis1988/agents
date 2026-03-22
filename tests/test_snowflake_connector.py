"""Unit tests for utilities/snowflake_connector.py.

All Snowflake connections are mocked with unittest.mock.patch.
No live Snowflake instance is required.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

import snowflake.connector.errors

from utilities.snowflake_connector import (
    SnowflakeConnectionError,
    SnowflakeQueryError,
    get_connection,
    execute,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FULL_ENV = {
    "SNOWFLAKE_ACCOUNT": "my_account",
    "SNOWFLAKE_USER": "my_user",
    "SNOWFLAKE_PASSWORD": "my_password",
    "SNOWFLAKE_WAREHOUSE": "my_warehouse",
    "SNOWFLAKE_DATABASE": "my_database",
    "SNOWFLAKE_SCHEMA": "my_schema",
}

FULL_SECRETS = {
    "account": "my_account",
    "user": "my_user",
    "password": "my_password",
    "warehouse": "my_warehouse",
    "database": "my_database",
    "schema": "my_schema",
}

# Patch target for snowflake.connector.connect — patched at the source module
# because the connector is lazy-imported inside _get_connector_connection().
_CONNECT = "snowflake.connector.connect"
# Patch target for _get_snowpark_session — controls SiS vs non-SiS branching.
_GET_SESSION = "utilities.snowflake_connector._get_snowpark_session"


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class TestCustomExceptions:
    def test_connection_error_is_exception(self):
        err = SnowflakeConnectionError("boom")
        assert isinstance(err, Exception)
        assert str(err) == "boom"

    def test_query_error_is_exception(self):
        err = SnowflakeQueryError("bad sql")
        assert isinstance(err, Exception)
        assert str(err) == "bad sql"


# ---------------------------------------------------------------------------
# get_connection() — success path (non-SiS)
# ---------------------------------------------------------------------------

class TestGetConnectionSuccess:
    def test_returns_connection_from_env_vars(self):
        mock_conn = MagicMock()
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", FULL_ENV), \
             patch(_CONNECT, return_value=mock_conn):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)
            conn = get_connection()

        assert conn is mock_conn
        assert session["conn"] is mock_conn

    def test_connection_stored_in_session_state(self):
        mock_conn = MagicMock()
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", FULL_ENV), \
             patch(_CONNECT, return_value=mock_conn):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)
            get_connection()

        assert session.get("conn") is mock_conn


# ---------------------------------------------------------------------------
# get_connection() — failure path (non-SiS)
# ---------------------------------------------------------------------------

class TestGetConnectionFailure:
    def test_raises_on_connect_exception(self):
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", FULL_ENV), \
             patch(_CONNECT, side_effect=Exception("network error")):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)

            with pytest.raises(SnowflakeConnectionError, match="network error"):
                get_connection()

    def test_raises_on_missing_credentials(self):
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", {}):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)

            with pytest.raises(SnowflakeConnectionError):
                get_connection()


# ---------------------------------------------------------------------------
# get_connection() — connection reuse (non-SiS)
# ---------------------------------------------------------------------------

class TestGetConnectionReuse:
    def test_reuses_existing_connection(self):
        existing_conn = MagicMock()
        session = {"conn": existing_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch(_CONNECT) as mock_connect:

            mock_st.session_state = session
            conn1 = get_connection()
            conn2 = get_connection()

        assert conn1 is existing_conn
        assert conn2 is existing_conn
        mock_connect.assert_not_called()


# ---------------------------------------------------------------------------
# Credential resolution — env vars
# ---------------------------------------------------------------------------

class TestCredentialResolutionEnvVars:
    def test_resolves_all_keys_from_env(self):
        mock_conn = MagicMock()
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", FULL_ENV), \
             patch(_CONNECT, return_value=mock_conn) as mock_connect:

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)
            get_connection()

        mock_connect.assert_called_once_with(
            account="my_account",
            user="my_user",
            password="my_password",
            warehouse="my_warehouse",
            database="my_database",
            schema="my_schema",
        )

    def test_partial_env_raises_connection_error(self):
        partial_env = {k: v for k, v in FULL_ENV.items() if k != "SNOWFLAKE_PASSWORD"}
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", partial_env):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)

            with pytest.raises(SnowflakeConnectionError, match="password"):
                get_connection()


# ---------------------------------------------------------------------------
# Credential resolution — st.secrets
# ---------------------------------------------------------------------------

class TestCredentialResolutionSecrets:
    def test_resolves_all_keys_from_secrets(self):
        mock_conn = MagicMock()
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", {}), \
             patch(_CONNECT, return_value=mock_conn) as mock_connect:

            mock_st.session_state = session
            mock_st.secrets = {"snowflake": FULL_SECRETS}
            get_connection()

        mock_connect.assert_called_once_with(**FULL_SECRETS)

    def test_secrets_takes_priority_over_env(self):
        mock_conn = MagicMock()
        session = {}
        secrets_creds = {k: f"secrets_{k}" for k in FULL_SECRETS}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", FULL_ENV), \
             patch(_CONNECT, return_value=mock_conn) as mock_connect:

            mock_st.session_state = session
            mock_st.secrets = {"snowflake": secrets_creds}
            get_connection()

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["account"] == "secrets_account"


# ---------------------------------------------------------------------------
# Missing credentials
# ---------------------------------------------------------------------------

class TestMissingCredentials:
    @pytest.mark.parametrize("missing_key", [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
    ])
    def test_raises_when_any_key_absent(self, missing_key):
        env = {k: v for k, v in FULL_ENV.items() if k != missing_key}
        session = {}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st, \
             patch("utilities.snowflake_connector.os.environ", env):

            mock_st.session_state = session
            type(mock_st).secrets = PropertyMock(side_effect=AttributeError)

            with pytest.raises(SnowflakeConnectionError):
                get_connection()


# ---------------------------------------------------------------------------
# execute() — success path (non-SiS)
# ---------------------------------------------------------------------------

class TestExecuteSuccess:
    def test_returns_dataframe_with_correct_columns(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_cursor.description = [("id",), ("name",)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # Ensure hasattr(conn, "sql") is False so the connector path is taken
        del mock_conn.sql

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            df = execute("SELECT id, name FROM users")

        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2
        assert df.iloc[0]["id"] == 1
        assert df.iloc[1]["name"] == "Bob"

    def test_returns_empty_dataframe_for_zero_rows(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [("id",), ("name",)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        del mock_conn.sql

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            df = execute("SELECT id, name FROM users WHERE 1=0")

        assert list(df.columns) == ["id", "name"]
        assert df.empty


# ---------------------------------------------------------------------------
# execute() — error path (non-SiS)
# ---------------------------------------------------------------------------

class TestExecuteProgrammingError:
    def test_raises_query_error_on_programming_error(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = snowflake.connector.errors.ProgrammingError(
            "syntax error near 'SELEKT'"
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        del mock_conn.sql

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            with pytest.raises(SnowflakeQueryError, match="syntax error"):
                execute("SELEKT * FROM users")

    def test_raises_query_error_on_unexpected_exception(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("unexpected failure")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        del mock_conn.sql

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            with pytest.raises(SnowflakeQueryError, match="unexpected failure"):
                execute("SELECT 1")

    def test_original_message_preserved(self):
        original_msg = "Object 'MISSING_TABLE' does not exist"
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = snowflake.connector.errors.ProgrammingError(original_msg)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        del mock_conn.sql

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            with pytest.raises(SnowflakeQueryError) as exc_info:
                execute("SELECT * FROM MISSING_TABLE")

        assert original_msg in str(exc_info.value)


# ---------------------------------------------------------------------------
# SiS path
# ---------------------------------------------------------------------------

class TestSiSConnection:
    def test_returns_snowpark_session_when_sis(self):
        mock_session = MagicMock()
        mock_session.sql = MagicMock()

        with patch(_GET_SESSION, return_value=mock_session):
            conn = get_connection()

        assert conn is mock_session

    def test_execute_uses_snowpark_sql_to_pandas(self):
        import pandas as pd
        expected_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        mock_result = MagicMock()
        mock_result.to_pandas.return_value = expected_df

        mock_session = MagicMock()
        mock_session.sql.return_value = mock_result

        with patch(_GET_SESSION, return_value=mock_session):
            df = execute("SELECT id, name FROM users")

        mock_session.sql.assert_called_once_with("SELECT id, name FROM users")
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2

    def test_execute_sis_error_raises_query_error(self):
        mock_session = MagicMock()
        mock_session.sql.side_effect = Exception("Snowpark execution failed")

        with patch(_GET_SESSION, return_value=mock_session):
            with pytest.raises(SnowflakeQueryError, match="Snowpark execution failed"):
                execute("SELECT 1")

    def test_falls_back_to_connector_when_not_sis(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        mock_conn.cursor.return_value = mock_cursor
        del mock_conn.sql  # ensure hasattr(conn, "sql") is False

        session = {"conn": mock_conn}

        with patch(_GET_SESSION, return_value=None), \
             patch("utilities.snowflake_connector.st") as mock_st:
            mock_st.session_state = session
            execute("SELECT 1")

        mock_conn.cursor.assert_called_once()
