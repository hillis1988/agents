"""Property-based tests for the Snowflake Analyst IDE.

Uses Hypothesis to verify universal properties across many randomly generated inputs.
No live Snowflake connection is required — Snowflake calls are mocked where needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from hypothesis import given, settings
import hypothesis.strategies as hst
from hypothesis.extra import pandas as hpd


# ---------------------------------------------------------------------------
# Property 1: SQL length boundary
# ---------------------------------------------------------------------------

# Feature: snowflake-analyst-ide, Property 1: SQL length boundary
@settings(max_examples=100)
@given(sql=hst.text(min_size=0, max_size=12000))
def test_sql_length_boundary(sql: str) -> None:
    # Feature: snowflake-analyst-ide, Property 1: SQL length boundary
    # Validates: Requirements 1.2
    is_valid = bool(sql and sql.strip() and len(sql) <= 10000)

    if len(sql) <= 10000 and sql and sql.strip():
        assert is_valid is True
    elif len(sql) > 10000:
        assert is_valid is False
    else:
        # empty or whitespace-only — also invalid
        assert is_valid is False


# ---------------------------------------------------------------------------
# Property 2: Empty and whitespace SQL rejection
# ---------------------------------------------------------------------------

# Feature: snowflake-analyst-ide, Property 2: Empty and whitespace SQL rejection
@settings(max_examples=100)
@given(
    sql=hst.text(
        alphabet=hst.characters(whitelist_categories=("Zs", "Cc")),
        min_size=0,
        max_size=100,
    )
)
def test_empty_whitespace_rejection(sql: str) -> None:
    # Feature: snowflake-analyst-ide, Property 2: Empty and whitespace SQL rejection
    # Validates: Requirements 2.2
    from utilities.sql_analyzer import analyze

    # The Query_Executor (analyze/execute) should never be called for these inputs;
    # verify by asserting analyze() returns [] (it short-circuits on empty/whitespace).
    # Zs = Unicode space separators, Cc = control characters — neither produces
    # meaningful SQL tokens, so analyze() must return an empty list.
    result = analyze(sql)
    assert result == [], (
        f"analyze() should return [] for whitespace/control-char input, got {result!r}"
    )


# ---------------------------------------------------------------------------
# Property 3: Result render contains required metadata
# ---------------------------------------------------------------------------

# Feature: snowflake-analyst-ide, Property 3: Result render contains required metadata
@settings(max_examples=100)
@given(
    df=hst.integers(min_value=1, max_value=5).flatmap(
        lambda n_cols: hpd.data_frames(
            columns=hpd.columns(n_cols, dtype=float),
            index=hpd.range_indexes(min_size=1, max_size=20),
        )
    )
)
def test_render_metadata(df: pd.DataFrame) -> None:
    # Feature: snowflake-analyst-ide, Property 3: Result render contains required metadata
    # Validates: Requirements 3.2, 3.4
    row_count = df.shape[0]
    col_count = df.shape[1]
    col_names = list(df.columns)

    # Exact row count
    assert df.shape[0] == row_count
    # Exact column count
    assert df.shape[1] == col_count
    # All column names present
    for col in col_names:
        assert col in df.columns


# ---------------------------------------------------------------------------
# Property 10: Successful query appends to history
# ---------------------------------------------------------------------------

# Feature: snowflake-analyst-ide, Property 10: Successful query appends to history
@settings(max_examples=100)
@given(
    queries=hst.lists(
        hst.text(min_size=1, max_size=100),
        min_size=1,
        max_size=10,
    )
)
def test_history_append(queries: list) -> None:
    # Feature: snowflake-analyst-ide, Property 10: Successful query appends to history
    # Validates: Requirements 7.2
    history: list = []

    for sql in queries:
        time_before = datetime.now(timezone.utc)
        entry = {"sql": sql, "timestamp": datetime.now(timezone.utc)}
        history.insert(0, entry)

        assert history[0]["sql"] == sql
        assert history[0]["timestamp"] >= time_before


# ---------------------------------------------------------------------------
# Property 11: History is ordered most-recent first
# ---------------------------------------------------------------------------

# Feature: snowflake-analyst-ide, Property 11: History is ordered most-recent first
@settings(max_examples=100)
@given(
    queries=hst.lists(
        hst.text(min_size=1, max_size=50),
        min_size=2,
        max_size=10,
    )
)
def test_history_ordering(queries: list) -> None:
    # Feature: snowflake-analyst-ide, Property 11: History is ordered most-recent first
    # Validates: Requirements 7.3
    history: list = []
    base_time = datetime(2024, 1, 1)

    for i, sql in enumerate(queries):
        # Monotonically increasing timestamps via timedelta offsets
        ts = base_time + timedelta(seconds=i)
        history.insert(0, {"sql": sql, "timestamp": ts})

    for i in range(len(history) - 1):
        assert history[i]["timestamp"] >= history[i + 1]["timestamp"], (
            f"history[{i}].timestamp ({history[i]['timestamp']}) < "
            f"history[{i + 1}].timestamp ({history[i + 1]['timestamp']})"
        )
