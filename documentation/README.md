# Snowflake Analyst IDE

A Streamlit-based SQL editor designed to help junior analysts write cost-effective queries on Snowflake. It analyses your SQL before execution, flags expensive patterns, suggests rewrites, and estimates how much you could save.

---

## Getting Started

### Running Locally

1. Clone the repository and install dependencies:

```bash
pip install -r requirements.txt
```

2. Copy the example environment file and fill in your Snowflake credentials:

```bash
cp .env.example .env
```

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

3. Start the app:

```bash
streamlit run app.py
```

### Running in Streamlit in Snowflake (SiS)

Upload the following files to your Snowflake stage, naming the entry point `streamlit_app.py`:

```
streamlit_app.py        ← app.py renamed
utilities/
  __init__.py
  sql_analyzer.py
  snowflake_connector.py
```

The app automatically detects the SiS environment and uses the native Snowpark session — no credentials needed.

---

## The Interface

### Editor (left panel)

Type your SQL directly into the editor. A few things to know:

- **Tab key** inserts 4 spaces for indentation
- **Multiple statements** are supported — separate them with a semicolon (`;`)
- There is a 10,000 character limit per submission

### Syntax Preview (right panel)

As you type, the right panel shows a formatted, syntax-highlighted version of your SQL in real time. Keywords are highlighted in blue, string literals in green, and numbers in orange. This is read-only — it's just a preview.

---

## Submitting a Query

Click **Submit** to send your SQL for analysis.

### If no issues are found

Your query runs immediately and results appear below.

### If issues are found

The app pauses before execution and shows you:

1. **A cost savings estimate** — how much cheaper your query could be after applying the suggestions (shown as % reduction, Snowflake credits, and USD)
2. **Query tips** — specific issues detected, grouped by statement if you submitted multiple
3. **Side-by-side SQL preview** — your original SQL on the left, the suggested rewrite on the right

You then choose what to do:

| Button | What it does |
|---|---|
| **Run suggested SQL** | Applies all rewrites and executes the improved query |
| **Run my SQL as-is** | Skips the suggestions and runs your original query |
| **Cancel** | Closes the tips panel without executing anything |

---

## What the Analyser Checks

| Check | Severity | Why it matters |
|---|---|---|
| Missing `LIMIT` | Warning | Without a limit, a query can return millions of rows and consume significant credits |
| `SELECT *` | Info | Scanning all columns increases data volume; specify only what you need |
| Missing `WHERE` clause | Warning | A query with no filter performs a full table scan |
| Cartesian `JOIN` (no `ON`/`USING`) | Warning | A join without a condition multiplies every row with every other row — extremely expensive |
| Leading wildcard `LIKE '%value'` | Warning | A leading `%` disables Snowflake's micro-partition pruning |
| `IN (SELECT ...)` subquery | Info | Correlated subqueries re-execute for every row; a `JOIN` is usually faster |
| Unaliased subquery in `FROM` | Info | Subqueries without an alias can cause errors and reduce readability |
| `USE` statements | Warning | `USE DATABASE`, `USE SCHEMA` etc. are not supported inside Streamlit in Snowflake |

---

## Query History

Every successfully executed query is saved to the **Query History** panel in the left sidebar, ordered most-recent first. Click any entry to expand it and see the SQL, then click **Load** to bring it back into the editor.

---

## Cost Savings Estimate

The savings estimate is a relative indicator based on known Snowflake cost drivers — it is not a precise billing figure. It uses the Snowflake Enterprise list price of **$3.00 per credit** and assumes a Medium warehouse baseline. Use it as a guide, not a guarantee.

---

## Project Structure

```
app.py                          Main Streamlit application
utilities/
  sql_analyzer.py               SQL analysis, rewriting, formatting, cost estimation
  snowflake_connector.py        Snowflake connection (SiS-aware)
tests/
  test_sql_analyzer.py          Unit tests for the analyser
  test_snowflake_connector.py   Unit tests for the connector
  test_properties.py            Property-based tests (Hypothesis)
documentation/
  README.md                     This file
.env.example                    Template for local credentials
requirements.txt                Python dependencies
```

---

## Running the Tests

```bash
python -m pytest tests/ -q
```

All tests run without a live Snowflake connection — the connector is fully mocked.
