"""Snowflake Analyst IDE — Streamlit entry point.

Developed by the Customer Data Engineering team.
"""

from __future__ import annotations

import re
import traceback
from datetime import datetime, timezone

import streamlit as st
import streamlit.components.v1 as components

from utilities.snowflake_connector import (
    SnowflakeConnectionError,
    SnowflakeQueryError,
    execute,
    fetch_schema,
    get_connection,
)
from utilities.sql_analyzer import (
    analyze,
    analyze_all,
    estimate_savings,
    format_sql,
    rewrite,
    split_statements,
    UNSUPPORTED_STATEMENT,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="❄️ SQL Analyst IDE",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS — dark IDE theme with polished branding
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] {
    background: #11111b;
}
[data-testid="stSidebar"] {
    background: #181825;
    border-right: 1px solid #313244;
}

/* ── Hero header ── */
.ide-hero {
    background: linear-gradient(135deg, #1e1e2e 0%, #181825 60%, #11111b 100%);
    border: 1px solid #313244;
    border-radius: 12px;
    padding: 28px 32px 20px 32px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.ide-hero::before {
    content: "";
    position: absolute;
    top: -40px; right: -40px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, #89b4fa22 0%, transparent 70%);
    pointer-events: none;
}
.ide-title {
    font-size: 2.2rem;
    font-weight: 800;
    color: #cdd6f4;
    letter-spacing: -0.5px;
    margin: 0 0 4px 0;
    line-height: 1.2;
}
.ide-title span { color: #89b4fa; }
.ide-subtitle e defaults
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "sql": "",
    "tips": [],
    "statement_tips": [],
    "pending_rewrite": None,
    "rewritten_sql": None,
    "result_df": None,
    "result_dfs": [],       # list of DataFrames for multi-statement results
    "history": [],
    "conn": None,
    "pending_execution": False,
    "show_tips": False,
}
for key, default in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ---------------------------------------------------------------------------
# Snowflake connection at startup
# ---------------------------------------------------------------------------

conn_error = False
try:
    get_connection()
except SnowflakeConnectionError as exc:
    st.error(f"🔴 Snowflake connection error: {exc}")
    conn_error = True

# ---------------------------------------------------------------------------
# Sidebar — query history
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### 📋 Query History")
    if st.session_state.history:
        for entry in st.session_state.history:
            label = entry["sql"][:80] + ("…" if len(entry["sql"]) > 80 else "")
            ts = entry["timestamp"].strftime("%H:%M:%S")
            with st.expander(f"🕐 {ts}", expanded=False):
                st.code(label, language="sql")
                if st.button("Load", key=f"hist_{entry['timestamp'].isoformat()}"):
                    st.session_state.sql = entry["sql"]
                    st.rerun()
    else:
        st.caption("No queries yet.")

# ---------------------------------------------------------------------------
# Tab-key support injection
# Intercepts Tab in the textarea and inserts 4 spaces instead of
# moving focus — works in both local Streamlit and SiS.
# ---------------------------------------------------------------------------

components.html("""
<script>
(function() {
  function attachTabHandler() {
    const textareas = window.parent.document.querySelectorAll('textarea');
    textareas.forEach(function(ta) {
      if (ta._tabHandlerAttached) return;
      ta._tabHandlerAttached = true;
      ta.addEventListener('keydown', function(e) {
        if (e.key === 'Tab') {
          e.preventDefault();
          const start = ta.selectionStart;
          const end   = ta.selectionEnd;
          ta.value = ta.value.substring(0, start) + '    ' + ta.value.substring(end);
          ta.selectionStart = ta.selectionEnd = start + 4;
          // Trigger React's synthetic onChange so Streamlit picks up the change
          const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
            window.HTMLTextAreaElement.prototype, 'value').set;
          nativeInputValueSetter.call(ta, ta.value);
          ta.dispatchEvent(new Event('input', { bubbles: true }));
        }
      });
    });
  }
  // Run on load and after any DOM mutations (Streamlit re-renders)
  attachTabHandler();
  const observer = new MutationObserver(attachTabHandler);
  observer.observe(window.parent.document.body, { childList: true, subtree: true });
})();
</script>
""", height=0)

# ---------------------------------------------------------------------------
# SQL keyword syntax highlighter (read-only preview)
# ---------------------------------------------------------------------------

_KW = (
    r'\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|USING|'
    r'GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|UNION|ALL|DISTINCT|AS|'
    r'INSERT|INTO|UPDATE|SET|DELETE|CREATE|DROP|ALTER|TABLE|VIEW|WITH|'
    r'CASE|WHEN|THEN|ELSE|END|AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL|'
    r'COUNT|SUM|AVG|MIN|MAX|COALESCE|NVL|IFF|QUALIFY|OVER|PARTITION\s+BY|'
    r'ROWS|RANGE|UNBOUNDED|PRECEDING|FOLLOWING|CURRENT\s+ROW)\b'
)

def _highlight_sql(sql: str) -> str:
    """Return HTML with SQL keywords colour-highlighted."""
    import html as _html
    # Escape HTML entities first so < > & in SQL don't break markup
    escaped = _html.escape(sql)

    # Keywords — use a lambda to preserve original casing
    def kw_replace(m):
        return f'<span style="color:#89b4fa;font-weight:bold">{m.group(0)}</span>'

    highlighted = re.sub(_KW, kw_replace, escaped, flags=re.IGNORECASE)

    # String literals  'value'
    highlighted = re.sub(
        r"(&#x27;[^&]*&#x27;|'[^']*')",
        lambda m: f'<span style="color:#a6e3a1">{m.group(0)}</span>',
        highlighted,
    )

    # Numbers
    highlighted = re.sub(
        r'\b(\d+)\b',
        lambda m: f'<span style="color:#fab387">{m.group(0)}</span>',
        highlighted,
    )

    # Single-line comments  -- ...
    highlighted = re.sub(
        r'(--[^\n]*)',
        lambda m: f'<span style="color:#6c7086;font-style:italic">{m.group(0)}</span>',
        highlighted,
    )

    return highlighted


def _render_highlighted(sql: str) -> None:
    """Render a syntax-highlighted, read-only SQL preview using st.html."""
    if not sql.strip():
        return
    formatted = format_sql(sql)
    highlighted = _highlight_sql(formatted)
    # st.html injects directly into the page DOM — no iframe, no sizing issues.
    # white-space:pre preserves indentation; overflow-x:auto handles long lines.
    st.html(
        f'<div style="'
        f'background:#1e1e2e;'
        f'border:1px solid #45475a;'
        f'border-radius:6px;'
        f'padding:12px 16px;'
        f'font-family:Consolas,Menlo,monospace;'
        f'font-size:13px;'
        f'line-height:1.6;'
        f'white-space:pre;'
        f'overflow-x:auto;'
        f'color:#cdd6f4;'
        f'margin:0 0 8px 0;'
        f'">{highlighted}</div>'
    )

# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

st.markdown("## ❄️ Snowflake Analyst IDE")
st.caption("Write SQL, get cost tips, and submit — all in one place.")

editor_col, preview_col = st.columns([1, 1], gap="medium")

with editor_col:
    st.markdown("**✏️ Editor**")
    st.session_state.sql = st.text_area(
        label="SQL Editor",
        value=st.session_state.sql,
        height=300,
        label_visibility="collapsed",
        placeholder="-- Write your SQL here\nSELECT * FROM my_table",
        key="_sql_editor",
    )
    st.caption("Tip: Tab inserts 4 spaces. Separate multiple statements with `;`")

with preview_col:
    st.markdown("**🎨 Preview**")
    if st.session_state.sql.strip():
        _render_highlighted(st.session_state.sql)
    else:
        st.html(
            '<div style="background:#1e1e2e;border:1px solid #45475a;border-radius:6px;'
            'padding:12px 16px;color:#6c7086;font-style:italic;min-height:80px">'
            'Syntax-highlighted preview appears here…</div>'
        )

# ---------------------------------------------------------------------------
# Submit button and input validation
# ---------------------------------------------------------------------------

col_submit, col_clear = st.columns([1, 6])
with col_submit:
    submitted = st.button("▶ Submit", disabled=conn_error, type="primary")
with col_clear:
    if st.button("🗑 Clear"):
        st.session_state.sql = ""
        st.session_state.tips = []
        st.session_state.statement_tips = []
        st.session_state.pending_rewrite = None
        st.session_state.rewritten_sql = None
        st.session_state.result_df = None
        st.session_state.result_dfs = []
        st.session_state.show_tips = False
        st.rerun()

if submitted:
    sql_input = st.session_state.sql

    if not sql_input or not sql_input.strip():
        st.warning("Please enter a SQL query.")
    elif len(sql_input) > 10_000:
        st.warning("Query exceeds the 10,000 character limit.")
    else:
        statement_tips = analyze_all(sql_input)
        flat_tips = analyze(sql_input)
        st.session_state.tips = flat_tips
        st.session_state.statement_tips = statement_tips
        st.session_state.rewritten_sql = None
        st.session_state.result_df = None
        st.session_state.result_dfs = []

        if flat_tips:
            # Pre-compute the rewrite so the preview is ready immediately
            result = rewrite(sql_input, flat_tips)
            st.session_state.pending_rewrite = result
            st.session_state.show_tips = True
            st.session_state.pending_execution = False
        else:
            st.session_state.pending_rewrite = None
            st.session_state.show_tips = False
            st.session_state.pending_execution = True

# ---------------------------------------------------------------------------
# Helper: run one or more statements and update state
# ---------------------------------------------------------------------------

def _run_query(sql_to_run: str) -> None:
    statements = split_statements(sql_to_run)
    if not statements:
        return

    # Block unsupported statements (e.g. USE DATABASE/SCHEMA) before hitting Snowflake
    pre_tips = analyze(sql_to_run)
    if any(t.code == UNSUPPORTED_STATEMENT for t in pre_tips):
        for t in pre_tips:
            if t.code == UNSUPPORTED_STATEMENT:
                st.error(t.message)
        return

    all_dfs = []
    had_error = False

    with st.spinner(f"Running {len(statements)} statement(s)…"):
        for i, stmt in enumerate(statements, 1):
            try:
                df = execute(stmt)
                all_dfs.append({"sql": stmt, "df": df, "index": i})
            except SnowflakeQueryError as exc:
                st.error(f"Statement {i} error: {exc}")
                had_error = True
                break

    if all_dfs:
        st.session_state.result_dfs = all_dfs
        # Use last result as the primary result_df for backward compat
        st.session_state.result_df = all_dfs[-1]["df"]
        if not had_error:
            st.session_state.history.insert(
                0, {"sql": sql_to_run, "timestamp": datetime.now(timezone.utc)}
            )

# ---------------------------------------------------------------------------
# Pending execution (no tips path)
# ---------------------------------------------------------------------------

if st.session_state.pending_execution:
    st.session_state.pending_execution = False
    _run_query(st.session_state.sql)

# ---------------------------------------------------------------------------
# Tips panel + Accept / Dismiss
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Tips panel + SQL preview + Accept / Run original / Cancel
# ---------------------------------------------------------------------------

if st.session_state.show_tips and st.session_state.tips:
    st.divider()
    st.markdown("### 💡 Query Tips")

    # Tips — grouped by statement when there are multiple
    statement_tips = st.session_state.get("statement_tips", [])
    if len(statement_tips) > 1:
        for st_tip in statement_tips:
            st.markdown(f"**Statement {st_tip.index}**")
            for tip in st_tip.tips:
                css_class = "tip-warning" if tip.severity == "warning" else "tip-info"
                icon = "⚠️" if tip.severity == "warning" else "ℹ️"
                st.markdown(
                    f'<div class="{css_class}">{icon} {tip.message}</div>',
                    unsafe_allow_html=True,
                )
    else:
        for tip in st.session_state.tips:
            css_class = "tip-warning" if tip.severity == "warning" else "tip-info"
            icon = "⚠️" if tip.severity == "warning" else "ℹ️"
            st.markdown(
                f'<div class="{css_class}">{icon} {tip.message}</div>',
                unsafe_allow_html=True,
            )

    # Always show both versions side-by-side so the user knows what they're running
    st.markdown("")
    prev_col1, prev_col2 = st.columns(2, gap="medium")

    with prev_col1:
        st.markdown("**📝 Your SQL**")
        _render_highlighted(st.session_state.sql)

    pending = st.session_state.get("pending_rewrite")
    with prev_col2:
        st.markdown("**✨ Suggested SQL**")
        if pending and pending.valid:
            _render_highlighted(pending.sql)
        elif pending and not pending.valid:
            _render_highlighted(st.session_state.sql)
            for issue in pending.syntax_issues:
                st.markdown(
                    f'<div class="syntax-error">🔴 {issue}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No rewrite available.")

    st.markdown("")
    col_accept, col_original, col_cancel = st.columns([1, 1, 1])

    with col_accept:
        run_suggested = st.button(
            "✅ Run suggested SQL",
            type="primary",
            disabled=(pending is None or not pending.valid),
        )
    with col_original:
        run_original = st.button("▶ Run my SQL as-is")
    with col_cancel:
        cancelled = st.button("🚫 Cancel")

    if run_suggested and pending and pending.valid:
        st.session_state.rewritten_sql = pending.sql
        st.session_state.show_tips = False
        st.session_state.pending_rewrite = None
        _run_query(pending.sql)

    elif run_original:
        st.session_state.show_tips = False
        st.session_state.pending_rewrite = None
        _run_query(st.session_state.sql)

    elif cancelled:
        st.session_state.show_tips = False
        st.session_state.pending_rewrite = None
        st.info("Execution cancelled.")

# ---------------------------------------------------------------------------
# Result Renderer — supports multiple statement results
# ---------------------------------------------------------------------------

if st.session_state.result_dfs:
    st.divider()
    st.markdown("### 📊 Results")

    for entry in st.session_state.result_dfs:
        df = entry["df"]
        idx = entry["index"]
        stmt_preview = entry["sql"][:60] + ("…" if len(entry["sql"]) > 60 else "")

        with st.expander(f"Statement {idx}: `{stmt_preview}`", expanded=True):
            rows, cols = df.shape
            st.markdown(
                f'<div class="result-meta">Rows: <b>{rows}</b> &nbsp;|&nbsp; Columns: <b>{cols}</b></div>',
                unsafe_allow_html=True,
            )
            if df.empty:
                st.info("This statement returned no results.")
            else:
                st.dataframe(df, use_container_width=True)

elif st.session_state.result_df is not None:
    # Fallback for single result (e.g. loaded from history)
    st.divider()
    df = st.session_state.result_df
    rows, cols = df.shape
    st.markdown("### 📊 Results")
    st.markdown(
        f'<div class="result-meta">Rows: <b>{rows}</b> &nbsp;|&nbsp; Columns: <b>{cols}</b></div>',
        unsafe_allow_html=True,
    )
    if df.empty:
        st.info("The query returned no results.")
    else:
        st.dataframe(df, use_container_width=True)
