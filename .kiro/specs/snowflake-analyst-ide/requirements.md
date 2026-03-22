# Requirements Document

## Introduction

A Streamlit-based IDE for junior analysts to write, analyse, and submit SQL queries against a Snowflake Cloud Data Warehouse. The application acts as a governed gateway that intercepts queries before execution, analyses them for cost and quality issues, surfaces actionable tips, and optionally rewrites the SQL before submission. The goal is to prevent expensive or poorly-written queries from reaching Snowflake unchecked.

The application is built in Python. All helper/utility logic lives in a `utilities/` folder, separate from the main Streamlit application code. A test suite covers all critical paths.

---

## Glossary

- **IDE**: The Streamlit web application described in this document.
- **Analyst**: A junior data analyst who is the primary user of the IDE.
- **Query_Editor**: The SQL text input component within the IDE.
- **Query_Analyser**: The utility module that inspects submitted SQL and produces tips.
- **Query_Executor**: The utility module responsible for connecting to Snowflake and running SQL.
- **Result_Renderer**: The component that displays query results as a table.
- **Tip**: A structured suggestion produced by the Query_Analyser (e.g. "Add a LIMIT clause", "Avoid SELECT *").
- **Rewrite**: A modified version of the original SQL produced after the Analyst accepts one or more Tips.
- **Snowflake**: The cloud data warehouse that the IDE connects to.
- **Session**: An active Snowflake connection held for the duration of an analyst's browser session.

---

## Requirements

### Requirement 1: SQL Query Editor

**User Story:** As an Analyst, I want a text editor in the IDE where I can write SQL, so that I can compose queries before submitting them.

#### Acceptance Criteria

1. THE IDE SHALL render a multi-line SQL text input (Query_Editor) on the main page.
2. THE Query_Editor SHALL accept SQL strings of up to 10,000 characters.
3. WHEN the Analyst clears the Query_Editor, THE IDE SHALL reset the editor to an empty state without affecting previously displayed results.
4. THE Query_Editor SHALL preserve the Analyst's current SQL across Streamlit re-runs caused by widget interactions on the same page.

---

### Requirement 2: Query Submission

**User Story:** As an Analyst, I want to submit my SQL query to Snowflake, so that I can retrieve data for analysis.

#### Acceptance Criteria

1. THE IDE SHALL provide a "Submit" button that triggers query execution.
2. WHEN the Analyst clicks "Submit" and the Query_Editor is empty, THE IDE SHALL display an inline validation message and SHALL NOT submit the query to Snowflake.
3. WHEN the Analyst clicks "Submit" and the Query_Editor contains SQL, THE IDE SHALL pass the SQL to the Query_Executor.
4. WHILE a query is executing, THE IDE SHALL display a loading indicator and SHALL disable the Submit button.
5. WHEN query execution completes successfully, THE IDE SHALL pass the result set to the Result_Renderer.

---

### Requirement 3: Result Display

**User Story:** As an Analyst, I want query results displayed in a table, so that I can read and interpret the data returned by Snowflake.

#### Acceptance Criteria

1. WHEN the Query_Executor returns a result set, THE Result_Renderer SHALL display the data in a paginated, scrollable table.
2. THE Result_Renderer SHALL display the row count and column count above the table.
3. WHEN the result set contains zero rows, THE Result_Renderer SHALL display a message stating that the query returned no results.
4. THE Result_Renderer SHALL render column headers using the names returned by Snowflake.

---

### Requirement 4: SQL Analysis and Tips

**User Story:** As an Analyst, I want the IDE to analyse my SQL and provide helpful tips, so that I can learn to write more efficient and cost-effective queries.

#### Acceptance Criteria

1. WHEN the Analyst clicks "Submit", THE Query_Analyser SHALL analyse the SQL before it is sent to Snowflake.
2. THE Query_Analyser SHALL detect the absence of a LIMIT clause and SHALL produce a Tip recommending one.
3. THE Query_Analyser SHALL detect the use of `SELECT *` and SHALL produce a Tip recommending explicit column selection.
4. THE Query_Analyser SHALL detect the absence of a WHERE clause on queries targeting a single table and SHALL produce a Tip recommending a filter.
5. THE Query_Analyser SHALL detect Cartesian joins (joins with no ON or USING clause) and SHALL produce a Tip warning the Analyst.
6. WHEN the Query_Analyser produces one or more Tips, THE IDE SHALL display each Tip to the Analyst before query execution proceeds.
7. WHEN the Query_Analyser produces zero Tips, THE IDE SHALL proceed directly to query execution without displaying a Tips panel.

---

### Requirement 5: Tip Acceptance and SQL Rewrite

**User Story:** As an Analyst, I want to accept suggested improvements to my SQL, so that the IDE can rewrite and submit an optimised query on my behalf.

#### Acceptance Criteria

1. WHEN Tips are displayed, THE IDE SHALL provide an "Accept Changes" button alongside the Tips panel.
2. WHEN the Analyst clicks "Accept Changes", THE Query_Analyser SHALL apply all accepted Tips to produce a Rewrite of the original SQL.
3. WHEN a Rewrite is produced, THE IDE SHALL display the rewritten SQL to the Analyst before submission.
4. WHEN a Rewrite is produced, THE IDE SHALL submit the Rewrite (not the original SQL) to the Query_Executor.
5. WHEN the Analyst dismisses the Tips panel without clicking "Accept Changes", THE IDE SHALL submit the original SQL to the Query_Executor.

---

### Requirement 6: Snowflake Connectivity

**User Story:** As an Analyst, I want the IDE to connect to Snowflake using my credentials, so that my queries run in the correct account and warehouse context.

#### Acceptance Criteria

1. THE Query_Executor SHALL establish a Snowflake connection using credentials supplied via environment variables or a Streamlit secrets file.
2. THE Query_Executor SHALL require the following connection parameters: account, user, password, warehouse, database, and schema.
3. IF a Snowflake connection cannot be established, THEN THE IDE SHALL display a descriptive error message and SHALL NOT allow query submission.
4. WHILE a Session is active, THE Query_Executor SHALL reuse the existing connection rather than opening a new one per query.
5. IF a query execution error is returned by Snowflake, THEN THE IDE SHALL display the Snowflake error message to the Analyst.

---

### Requirement 7: Query History

**User Story:** As an Analyst, I want to see a history of queries I have run in the current session, so that I can revisit and re-run previous queries.

#### Acceptance Criteria

1. THE IDE SHALL maintain an in-session query history list.
2. WHEN a query is successfully submitted (original or rewritten), THE IDE SHALL append the SQL and its execution timestamp to the history list.
3. THE IDE SHALL display the query history in a collapsible sidebar panel, ordered most-recent first.
4. WHEN the Analyst selects a query from the history, THE IDE SHALL populate the Query_Editor with that SQL.

---

### Requirement 8: Test Suite

**User Story:** As a developer, I want a test suite covering all critical paths, so that I can verify the application behaves correctly and catch regressions.

#### Acceptance Criteria

1. THE Test_Suite SHALL include unit tests for every public function in the `utilities/` folder.
2. THE Test_Suite SHALL include tests that verify the Query_Analyser correctly identifies each anti-pattern defined in Requirement 4.
3. THE Test_Suite SHALL include tests that verify the Query_Analyser produces a correct Rewrite for each anti-pattern.
4. THE Test_Suite SHALL include tests that verify the Query_Executor returns a structured error when Snowflake connectivity fails.
5. THE Test_Suite SHALL mock all Snowflake connections so that tests run without a live Snowflake instance.
6. FOR ALL valid SQL strings that the Query_Analyser rewrites, parsing the original SQL and the rewritten SQL SHALL produce structurally equivalent parse trees where the rewrite is a strict superset of the original intent (round-trip equivalence property).
