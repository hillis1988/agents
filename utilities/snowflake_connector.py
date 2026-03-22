"""Snowflake connection management and query execution.

Connection resolution order:
  1. Streamlit in Snowflake (SiS) — uses get_active_session() automatically;
     no credentials required.
  2. st.secrets["snowflake"]  (Streamlit Cloud / secrets.toml)
  3. Environment variables:   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
                              SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE,
                              SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

from __future__ import annotations

import os
from typing import Union

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class SnowflakeConnectionError(Exception):
    """Raised when a Snowflake connection cannot be established."""


class SnowflakeQueryError(Exception):
    """Raised when a query fails on the Snowflake side."""


# ---------------------------------------------------------------------------
# SiS detection
# ---------------------------------------------------------------------------

def _get_snowpark_session():
    """Return the active Snowpark session if running inside Streamlit in Snowflake.

    Returns None when running outside SiS (local / Streamlit Cloud).
    """
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        return None


def _is_sis() -> bool:
    """True when the app is running inside Streamlit in Snowflake."""
    return _get_snowpark_session() is not None


# ---------------------------------------------------------------------------
# Credential-based connection (non-SiS path)
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = ("account", "user", "password", "warehouse", "database", "schema")

_ENV_VAR_MAP = {
    "account": "SNOWFLAKE_ACCOUNT",
    "user": "SNOWFLAKE_USER",
    "password": "SNOWFLAKE_PASSWORD",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "database": "SNOWFLAKE_DATABASE",
    "schema": "SNOWFLAKE_SCHEMA",
}


def _resolve_credentials() -> dict:
    """Return a dict of credentials, or raise SnowflakeConnectionError."""
    creds: dict = {}

    # 1. Try st.secrets["snowflake"] first
    try:
        secrets = st.secrets["snowflake"]
        creds = {k: secrets[k] for k in _REQUIRED_KEYS if k in secrets}
    except (AttributeError, KeyError):
        pass

    # 2. Fill any missing keys from environment variables
    for key, env_var in _ENV_VAR_MAP.items():
        if key not in creds:
            value = os.environ.get(env_var)
            if value is not None:
                creds[key] = value

    missing = [k for k in _REQUIRED_KEYS if k not in creds or not creds[k]]
    if missing:
        raise SnowflakeConnectionError(
            f"Missing required Snowflake credential(s): {', '.join(missing)}"
        )

    return creds


def _get_connector_connection():
    """Return a cached snowflake-connector-python connection."""
    import snowflake.connector

    if "conn" in st.session_state and st.session_state["conn"] is not None:
        return st.session_state["conn"]

    creds = _resolve_credentials()
    try:
        conn = snowflake.connector.connect(**creds)
    except Exception as exc:
        raise SnowflakeConnectionError(f"Failed to connect to Snowflake: {exc}") from exc

    st.session_state["conn"] = conn
    return conn


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_connection():
    """Return the active connection/session.

    Inside SiS: returns the Snowpark Session (no credentials needed).
    Outside SiS: returns a cached snowflake-connector-python connection.

    Raises SnowflakeConnectionError if credentials are missing or the
    Snowflake handshake fails (non-SiS path only).
    """
    session = _get_snowpark_session()
    if session is not None:
        return session

    return _get_connector_connection()


def execute(sql: str) -> pd.DataFrame:
    """Execute *sql* and return results as a DataFrame.

    Handles both the Snowpark Session (SiS) and connector (non-SiS) paths.
    Raises SnowflakeQueryError on any Snowflake-side error.
    """
    conn = get_connection()

    try:
        # SiS path: Snowpark Session exposes .sql().to_pandas()
        if hasattr(conn, "sql"):
            return conn.sql(sql).to_pandas()

        # Non-SiS path: standard connector cursor
        import snowflake.connector.errors
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=columns)

    except SnowflakeQueryError:
        raise
    except Exception as exc:
        # Covers both snowflake.connector.errors.ProgrammingError and Snowpark exceptions
        raise SnowflakeQueryError(str(exc)) from exc
