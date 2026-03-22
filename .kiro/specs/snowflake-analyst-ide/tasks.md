# Implementation Plan: Snowflake Analyst IDE

## Overview

Build the application bottom-up: pure-Python utilities first (no external dependencies to mock), then the Streamlit UI layer, then the full test suite. Each task wires directly into the previous one so there is no orphaned code.

## Tasks

- [x] 1. Project scaffolding and dependencies
  - Create `utilities/__init__.py` (empty)
  - Create `requirements.txt` with: `streamlit`, `snowflake-connector-python`, `sqlparse`, `pandas`, `python-dotenv`, `pytest`, `hypothesis[pandas]`
  - Create `.env.example` with the six required keys: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
  - Create `tests/__init__.py` (empty) and `tests/conftest.py` with placeholder
  - _Requirements: 6.1, 6.2, 8.5_

- [x] 2. Implement `utilities/sql_analyzer.py`
  - [x] 2.1 Define the `Tip` dataclass and the four anti-pattern codes
    - `Tip(code, message, severity)` with `severity` constrained to `"warning" | "info"`
    - _Requirements: 4.2, 4.3, 4.4, 4.5_

  - [x] 2.2 Implement `analyze(sql: str) -> List[Tip]`
    - Use `sqlparse` to parse the SQL
    - Detect `MISSING_LIMIT`: no top-level LIMIT token in a SELECT statement
    - Detect `SELECT_STAR`: wildcard `*` in the SELECT column list
    - Detect `MISSING_WHERE`: single-table SELECT with no WHERE clause
    - Detect `CARTESIAN_JOIN`: JOIN keyword present but no ON or USING clause follows
    - Return empty list when no anti-patterns found
    - _Requirements: 4.2, 4.3, 4.4, 4.5_

  - [ ]* 2.3 Write property test for `analyze()` — Property 4: MISSING_LIMIT detection
    - `# Feature: snowflake-analyst-ide, Property 4: MISSING_LIMIT detection`
    - Strategy: generate SELECT SQL strings without a LIMIT clause
    - **Property 4: MISSING_LIMIT detection**
    - **Validates: Requirements 4.2**

  - [ ]* 2.4 Write property test for `analyze()` — Property 5: SELECT_STAR detection
    - `# Feature: snowflake-analyst-ide, Property 5: SELECT_STAR detection`
    - Strategy: generate `SELECT * FROM <table>` SQL strings
    - **Property 5: SELECT_STAR detection**
    - **Validates: Requirements 4.3**

  - [ ]* 2.5 Write property test for `analyze()` — Property 6: MISSING_WHERE detection
    - `# Feature: snowflake-analyst-ide, Property 6: MISSING_WHERE detection`
    - Strategy: generate single-table SELECT SQL strings without a WHERE clause
    - **Property 6: MISSING_WHERE detection**
    - **Validates: Requirements 4.4**

  - [ ]* 2.6 Write property test for `analyze()` — Property 7: CARTESIAN_JOIN detection
    - `# Feature: snowflake-analyst-ide, Property 7: CARTESIAN_JOIN detection`
    - Strategy: generate JOIN SQL strings without ON/USING
    - **Property 7: CARTESIAN_JOIN detection**
    - **Validates: Requirements 4.5**

  - [x] 2.7 Implement `rewrite(sql: str, tips: List[Tip]) -> str`
    - `MISSING_LIMIT` → append `LIMIT 1000`
    - `SELECT_STAR` → replace `*` with `/* specify columns */ *`
    - `MISSING_WHERE` → append `WHERE TRUE -- add your filter`
    - `CARTESIAN_JOIN` → append `/* WARNING: Cartesian join detected */`
    - Rewrites are additive; never remove original tokens
    - _Requirements: 5.2, 8.3, 8.6_

  - [ ]* 2.8 Write property test for `rewrite()` — Property 8: Rewrite is a structural superset
    - `# Feature: snowflake-analyst-ide, Property 8: Rewrite is a structural superset`
    - Strategy: any SQL string where `analyze()` returns tips; assert `analyze(rewrite(s, T))` has no codes from `T`, result is parseable, and all original tokens are present
    - **Property 8: Rewrite is a structural superset (round-trip equivalence)**
    - **Validates: Requirements 5.2, 8.6**

  - [ ]* 2.9 Write unit tests for `sql_analyzer.py` in `tests/test_sql_analyzer.py`
    - One passing and one failing SQL example per detection rule (4 rules × 2 = 8 cases)
    - Known input/output pair for each rewrite rule (4 cases)
    - _Requirements: 8.1, 8.2, 8.3_

- [x] 3. Checkpoint — sql_analyzer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement `utilities/snowflake_connector.py`
  - [x] 4.1 Define custom exceptions `SnowflakeConnectionError` and `SnowflakeQueryError`
    - _Requirements: 6.3, 6.5_

  - [x] 4.2 Implement `get_connection() -> SnowflakeConnection`
    - Resolve credentials: try `st.secrets["snowflake"]` first, then env vars
    - Required keys: `account`, `user`, `password`, `warehouse`, `database`, `schema`
    - Cache connection in `st.session_state.conn`; reuse on subsequent calls
    - Raise `SnowflakeConnectionError` if any key is missing or the handshake fails
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]* 4.3 Write property test for `get_connection()` — Property 9: Missing credentials raise a connection error
    - `# Feature: snowflake-analyst-ide, Property 9: Missing credentials raise a connection error`
    - Strategy: `st.sets(st.sampled_from([...6 keys...]), max_size=5)` to generate incomplete credential sets
    - **Property 9: Missing credentials raise a connection error**
    - **Validates: Requirements 6.2**

  - [x] 4.4 Implement `execute(sql: str) -> pd.DataFrame`
    - Call `get_connection()`, run the SQL, fetch results into a `pd.DataFrame`
    - Catch `snowflake.connector.errors.ProgrammingError` and re-raise as `SnowflakeQueryError` preserving the original message
    - _Requirements: 2.3, 6.5_

  - [ ]* 4.5 Write unit tests for `snowflake_connector.py` in `tests/test_snowflake_connector.py`
    - `get_connection()`: success path and failure path with mocked `snowflake.connector.connect`
    - `get_connection()`: connection reuse (calling twice returns same object via mock session_state)
    - Credential resolution from env vars vs. mock `st.secrets` dict
    - `execute()`: success path with mocked cursor returning rows
    - `execute()`: `ProgrammingError` path raises `SnowflakeQueryError`
    - All Snowflake connections mocked with `unittest.mock.patch`
    - _Requirements: 8.1, 8.4, 8.5_

- [x] 5. Checkpoint — connector tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement `tests/conftest.py` shared fixtures
  - `mock_connection` fixture: returns a `MagicMock` SnowflakeConnection
  - `mock_session_state` fixture: patches `st.session_state` with a plain dict
  - _Requirements: 8.5_

- [x] 7. Implement `app.py` — Streamlit UI
  - [x] 7.1 Page setup and session state initialisation
    - `st.set_page_config(...)`, initialise `sql`, `tips`, `rewritten_sql`, `result_df`, `history`, `conn` keys in `st.session_state` if absent
    - _Requirements: 1.4_

  - [x] 7.2 Snowflake connection at startup
    - Call `get_connection()` on load; catch `SnowflakeConnectionError` and render `st.error(...)`, disable Submit button while no connection
    - _Requirements: 6.3_

  - [x] 7.3 Query_Editor component
    - Render `st.text_area` bound to `st.session_state.sql` (max 10,000 chars enforced in validation)
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 7.4 Submit button and input validation
    - Render Submit button; disable while executing or no connection
    - On click: reject empty/whitespace SQL with `st.warning("Please enter a SQL query.")`
    - On click: reject SQL > 10,000 chars with `st.warning("Query exceeds the 10,000 character limit.")`
    - _Requirements: 2.1, 2.2_

  - [x] 7.5 SQL analysis and Tips panel
    - Call `sql_analyzer.analyze(sql)` before execution
    - If tips exist: render each Tip, show "Accept Changes" and "Dismiss" buttons
    - If no tips: proceed directly to execution
    - _Requirements: 4.1, 4.6, 4.7_

  - [x] 7.6 Accept Changes / Dismiss flow
    - "Accept Changes": call `sql_analyzer.rewrite(sql, tips)`, store in `st.session_state.rewritten_sql`, display rewritten SQL, then execute the rewrite
    - "Dismiss": execute original SQL
    - Catch unexpected rewrite exceptions, log traceback, fall back to original SQL with `st.warning(...)`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x] 7.7 Query execution and loading indicator
    - Execute final SQL via `snowflake_connector.execute(sql)` inside `st.spinner(...)`
    - Catch `SnowflakeQueryError` and render `st.error(...)` without updating history
    - On success: store result in `st.session_state.result_df`, append `{sql, timestamp}` to history (newest-first)
    - _Requirements: 2.4, 2.5, 6.5, 7.2_

  - [x] 7.8 Result_Renderer
    - Display row count and column count above the table
    - Render `st.dataframe(result_df)` with column headers from `df.columns`
    - If `df.empty`: display "The query returned no results."
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 7.9 Query history sidebar
    - Render `st.sidebar` with a collapsible section listing history entries (newest-first)
    - Each entry shows the SQL and timestamp; clicking populates `st.session_state.sql`
    - _Requirements: 7.1, 7.3, 7.4_

- [x] 8. Implement remaining property-based tests in `tests/test_properties.py`
  - [x] 8.1 Write property test — Property 1: SQL length boundary
    - `# Feature: snowflake-analyst-ide, Property 1: SQL length boundary`
    - Strategy: `st.text(min_size=0, max_size=12000)`; assert accepted iff `len(sql) <= 10000`
    - `@settings(max_examples=100)`
    - **Property 1: SQL length boundary**
    - **Validates: Requirements 1.2**

  - [x] 8.2 Write property test — Property 2: Empty and whitespace SQL rejection
    - `# Feature: snowflake-analyst-ide, Property 2: Empty and whitespace SQL rejection`
    - Strategy: `st.text(alphabet=st.characters(whitelist_categories=("Zs","Cc")))`; assert Query_Executor is never called
    - `@settings(max_examples=100)`
    - **Property 2: Empty and whitespace SQL rejection**
    - **Validates: Requirements 2.2**

  - [x] 8.3 Write property test — Property 3: Result render contains required metadata
    - `# Feature: snowflake-analyst-ide, Property 3: Result render contains required metadata`
    - Strategy: `st.data_frames(...)` from `hypothesis[pandas]`; assert rendered output contains exact row count, column count, and all column names
    - `@settings(max_examples=100)`
    - **Property 3: Result render contains required metadata**
    - **Validates: Requirements 3.2, 3.4**

  - [x] 8.4 Write property test — Property 10: Successful query appends to history
    - `# Feature: snowflake-analyst-ide, Property 10: Successful query appends to history`
    - Strategy: `st.lists(st.text(min_size=1), min_size=1)`; assert history contains entry with exact SQL and UTC timestamp ≥ pre-call time
    - `@settings(max_examples=100)`
    - **Property 10: Successful query appends to history**
    - **Validates: Requirements 7.2**

  - [x] 8.5 Write property test — Property 11: History is ordered most-recent first
    - `# Feature: snowflake-analyst-ide, Property 11: History is ordered most-recent first`
    - Strategy: `st.lists(st.text(min_size=1), min_size=2)`; assert `history[i].timestamp >= history[i+1].timestamp` for all i
    - `@settings(max_examples=100)`
    - **Property 11: History is ordered most-recent first**
    - **Validates: Requirements 7.3**

- [x] 9. Final checkpoint — all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- All Snowflake interactions must be mocked; no live instance required for tests
- Property tests use `@settings(max_examples=100)` and are tagged with the property number
- Unit tests and property tests are complementary — both are needed for full coverage
