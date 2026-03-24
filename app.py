# -*- coding: utf-8 -*-
"""Snowflake Analyst IDE - Streamlit entry point."""

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
    page_title="Snowflake Analyst IDE",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* ---- Base ---- */
[data-testid="stAppViewContainer"] { background: #11111b; }
[data-testid="stSidebar"] {
    background: #181825;
    border-right: 1px solid #313244;
}
[data-testid="stSidebar"] * { color: #cdd6f4 !important; }

/* ---- Hero banner ---- */
.ide-hero {
    background: linear-gradient(135deg, #1e1e2e 0%, #181825 100%);
    border: 1px solid #313244;
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 20px;
}
.ide-title {
    font-size: 2rem;
    font-weight: 800;
    color: #cdd6f4;
    margin: 0 0 4px 0;
    line-height: 1.2;
}
.ide-title .accent { color: #89b4fa; }
.ide-subtitle {
    font-size: 0.95rem;
    color: #6c7086;
    margin: 0;
}
.ide-badge {
    display: inline-block;
    background: #313244;
    color: #89b4fa;
    font-size: 0.75rem;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 20px;
    margin-top: 10px;
    letter-spacing: 0.5px;
}

/* ---- Cost savings card ---- */
.savings-card {
    background: linear-gradient(135deg, #1a2a1a 0%, #0e1f0e 100%);
    border: 1px solid #2d5a2d;
    border-radius: 10px;
    padding: 16px 20px;
    margin: 12px 0;
    display: flex;
    align-items: center;
    gap: 16px;
}
.savings-icon {
    font-size: 2rem;
    line-height: 1;
}
.savings-pct {
    font-size: 2.2rem;
    font-weight: 800;
    color: #a6e3a1;
    line-height: 1;
}
.savings-label {
    font-size: 0.85rem;
    color: #94e2a1;
    margin-top: 2px;
}
.savings-detail {
    font-size: 0.78rem;
    color: #6c7086;
    margin-top: 4px;
}

/* ---- Tip cards ---- */
.tip-warning {
    background: #2a1f0e;
    border-left: 4px solid #f9a825;
    padding: 8px 12px;
    border-radius: 4px;
    margin: 4px 0;
    color: #ffd54f;
    font-size: 0.9rem;
}
.tip-info {
    background: #0e1f2a;
    border-left: 4px solid #29b6f6;
    padding: 8px 12px;
    border-radius: 4px;
    margin: 4px 0;
    color: #81d4fa;
    font-size: 0.9rem;
}
.syntax-error {
    background: #2a0e0e;
    border-left: 4px solid #ef5350;
    padding: 8px 12px;
    border-radius: 4px;
    margin: 4px 0;
    color: #ef9a9a;
    font-size: 0.9rem;
}

/* ---- Editor textarea ---- */
textarea {
    font-family: Consolas, Menlo, monospace !important;
    font-size: 14px !important;
    background-color: #1e1e2e !important;
    color: #cdd6f4 !important;
    border: 1px solid #45475a !important;
    border-radius: 6px !important;
    line-height: 1.6 !important;
}

/* ---- Section labels ---- */
.section-label {
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #6c7086;
    margin-bottom: 6px;
}

/* ---- Result metadata ---- */
.result-meta {
    font-size: 13px;
    color: #a6adc8;
    margin-bottom: 6px;
}

/* ---- Divider colour ---- */
hr { border-color: #313244 !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "sql": "",
    "tips": [],
    "statement_tips": [],
    "pending_rewrite": None,
    "rewritten_sql": None,
    "result_df": None,
    "result_dfs": [],
    "history": [],
    "conn": None,
    "pending_execution": False,
    "show_tips": False,
}
for key, default in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

conn_error = False
try:
    get_connection()
except SnowflakeConnectionError as exc:
    st.error("Snowflake connection error: " + str(exc))
    conn_error = True

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        '<div style="padding:12px 0 8px 0;">'
        '<span style="font-size:1.1rem;font-weight:700;color:#89b4fa;">Query History</span>'
        '</div>',
        unsafe_allow_html=True,
    )
    if st.session_state.history:
        for entry in st.session_state.history:
            label = entry["sql"][:80] + ("..." if len(entry["sql"]) > 80 else "")
            ts = entry["timestamp"].strftime("%H:%M:%S")
            with st.expander(ts, expanded=False):
                st.code(label, language="sql")
                if st.button("Load", key="hist_" + entry["timestamp"].isoformat()):
                    st.session_state.sql = entry["sql"]
                    st.rerun()
    else:
        st.caption("No queries yet.")

# ---------------------------------------------------------------------------
# Tab-key support
# ---------------------------------------------------------------------------

components.html("""
<script>
(function() {
  function attachTabHandler() {
    var textareas = window.parent.document.querySelectorAll('textarea');
    textareas.forEach(function(ta) {
      if (ta._tabHandlerAttached) return;
      ta._tabHandlerAttached = true;
      ta.addEventListener('keydown', function(e) {
        if (e.key === 'Tab') {
          e.preventDefault();
          var start = ta.selectionStart;
          var end = ta.selectionEnd;
          ta.value = ta.value.substring(0, start) + '    ' + ta.value.substring(end);
          ta.selectionStart = ta.selectionEnd = start + 4;
          var setter = Object.getOwnPropertyDescriptor(
            window.HTMLTextAreaElement.prototype, 'value').set;
          setter.call(ta, ta.value);
          ta.dispatchEvent(new Event('input', { bubbles: true }));
        }
      });
    });
  }
  attachTabHandler();
  var observer = new MutationObserver(attachTabHandler);
  observer.observe(window.parent.document.body, { childList: true, subtree: true });
})();
</script>
""", height=0)

# ---------------------------------------------------------------------------
# SQL highlighter
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
    import html as _html
    escaped = _html.escape(sql)
    highlighted = re.sub(
        _KW,
        lambda m: '<span style="color:#89b4fa;font-weight:bold">' + m.group(0) + '</span>',
        escaped,
        flags=re.IGNORECASE,
    )
    highlighted = re.sub(
        r"(&#x27;[^&]*&#x27;|'[^']*')",
        lambda m: '<span style="color:#a6e3a1">' + m.group(0) + '</span>',
        highlighted,
    )
    highlighted = re.sub(
        r'\b(\d+)\b',
        lambda m: '<span style="color:#fab387">' + m.group(0) + '</span>',
        highlighted,
    )
    highlighted = re.sub(
        r'(--[^\n]*)',
        lambda m: '<span style="color:#6c7086;font-style:italic">' + m.group(0) + '</span>',
        highlighted,
    )
    return highlighted


def _render_highlighted(sql: str) -> None:
    if not sql.strip():
        return
    highlighted = _highlight_sql(format_sql(sql))
    st.html(
        '<div style="background:#1e1e2e;border:1px solid #45475a;border-radius:6px;'
        'padding:12px 16px;font-family:Consolas,Menlo,monospace;font-size:13px;'
        'line-height:1.6;white-space:pre;overflow-x:auto;color:#cdd6f4;margin:0 0 8px 0;">'
        + highlighted + '</div>'
    )

# ---------------------------------------------------------------------------
# Hero banner
# ---------------------------------------------------------------------------

st.markdown(
    '<div class="ide-hero">'
    '<div class="ide-title">Snowflake <span class="accent">Analyst IDE</span></div>'
    '<p class="ide-subtitle">Write cost-effective SQL with real-time analysis and guided rewrites.</p>'
    '<span class="ide-badge">POWERED BY SNOWFLAKE</span>'
    '</div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Editor + Preview
# ---------------------------------------------------------------------------

editor_col, preview_col = st.columns([1, 1], gap="medium")

with editor_col:
    st.markdown('<div class="section-label">SQL Editor</div>', unsafe_allow_html=True)
    st.session_state.sql = st.text_area(
        label="SQL Editor",
        value=st.session_state.sql,
        height=300,
        label_visibility="collapsed",
        placeholder="-- Write your SQL here\nSELECT * FROM my_table",
        key="_sql_editor",
    )
    st.caption("Tab inserts 4 spaces  |  Separate statements with ;")

with preview_col:
    st.markdown('<div class="section-label">Syntax Preview</div>', unsafe_allow_html=True)
    if st.session_state.sql.strip():
        _render_highlighted(st.session_state.sql)
    else:
        st.html(
            '<div style="background:#1e1e2e;border:1px solid #313244;border-radius:6px;'
            'padding:12px 16px;color:#45475a;font-style:italic;min-height:80px;'
            'font-family:Consolas,Menlo,monospace;font-size:13px;">'
            'Syntax-highlighted preview appears here...</div>'
        )

# ---------------------------------------------------------------------------
# Submit / Clear
# ---------------------------------------------------------------------------

col_submit, col_clear, col_spacer = st.columns([1, 1, 8])
with col_submit:
    submitted = st.button("Submit", disabled=conn_error, type="primary", use_container_width=True)
with col_clear:
    if st.button("Clear", use_container_width=True):
        for k in ("sql", "tips", "statement_tips", "pending_rewrite",
                  "rewritten_sql", "result_df", "result_dfs", "show_tips"):
            st.session_state[k] = [] if k in ("tips", "statement_tips", "result_dfs") else None if k not in ("sql",) else ""
        st.session_state.show_tips = False
        st.rerun()

if submitted:
    sql_input = st.session_state.sql
    if not sql_input or not sql_input.strip():
        st.warning("Please enter a SQL query.")
    elif len(sql_input) > 10_000:
        st.warning("Query exceeds the 10,000 character limit.")
    else:
        flat_tips = analyze(sql_input)
        st.session_state.tips = flat_tips
        st.session_state.statement_tips = analyze_all(sql_input)
        st.session_state.rewritten_sql = None
        st.session_state.result_df = None
        st.session_state.result_dfs = []

        if flat_tips:
            st.session_state.pending_rewrite = rewrite(sql_input, flat_tips)
            st.session_state.show_tips = True
            st.session_state.pending_execution = False
        else:
            st.session_state.pending_rewrite = None
            st.session_state.show_tips = False
            st.session_state.pending_execution = True

# ---------------------------------------------------------------------------
# Execute helper
# ---------------------------------------------------------------------------

def _run_query(sql_to_run: str) -> None:
    statements = split_statements(sql_to_run)
    if not statements:
        return
    pre_tips = analyze(sql_to_run)
    if any(t.code == UNSUPPORTED_STATEMENT for t in pre_tips):
        for t in pre_tips:
            if t.code == UNSUPPORTED_STATEMENT:
                st.error(t.message)
        return
    all_dfs = []
    had_error = False
    with st.spinner("Running " + str(len(statements)) + " statement(s)..."):
        for i, stmt in enumerate(statements, 1):
            try:
                df = execute(stmt)
                all_dfs.append({"sql": stmt, "df": df, "index": i})
            except SnowflakeQueryError as exc:
                st.error("Statement " + str(i) + " error: " + str(exc))
                had_error = True
                break
    if all_dfs:
        st.session_state.result_dfs = all_dfs
        st.session_state.result_df = all_dfs[-1]["df"]
        if not had_error:
            st.session_state.history.insert(
                0, {"sql": sql_to_run, "timestamp": datetime.now(timezone.utc)}
            )

if st.session_state.pending_execution:
    st.session_state.pending_execution = False
    _run_query(st.session_state.sql)

# ---------------------------------------------------------------------------
# Tips panel + cost savings + SQL preview + action buttons
# ---------------------------------------------------------------------------

if st.session_state.show_tips and st.session_state.tips:
    st.divider()

    # Cost savings banner
    savings = estimate_savings(st.session_state.tips)
    pct = savings["pct_reduction"]
    credits = savings["credits_saved"]
    usd = savings["usd_saved"]
    if pct > 0:
        st.markdown(
            '<div class="savings-card">'
            '<div class="savings-icon">&#128200;</div>'
            '<div>'
            '<div class="savings-pct">~' + str(pct) + '% cost reduction</div>'
            '<div class="savings-label">Estimated saving by applying suggestions</div>'
            '<div class="savings-detail">'
            + str(credits) + ' credits / $' + str(round(usd, 4))
            + ' per execution (Snowflake Enterprise list price)'
            '</div>'
            '</div>'
            '</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="section-label" style="margin-top:12px;">Query Tips</div>', unsafe_allow_html=True)

    statement_tips = st.session_state.get("statement_tips", [])
    if len(statement_tips) > 1:
        for st_tip in statement_tips:
            st.markdown(
                '<div style="color:#89b4fa;font-weight:600;margin:8px 0 4px 0;">'
                'Statement ' + str(st_tip.index) + '</div>',
                unsafe_allow_html=True,
            )
            for tip in st_tip.tips:
                css_class = "tip-warning" if tip.severity == "warning" else "tip-info"
                icon = "&#9888;" if tip.severity == "warning" else "&#8505;"
                st.markdown(
                    '<div class="' + css_class + '">' + icon + '  ' + tip.message + '</div>',
                    unsafe_allow_html=True,
                )
    else:
        for tip in st.session_state.tips:
            css_class = "tip-warning" if tip.severity == "warning" else "tip-info"
            icon = "&#9888;" if tip.severity == "warning" else "&#8505;"
            st.markdown(
                '<div class="' + css_class + '">' + icon + '  ' + tip.message + '</div>',
                unsafe_allow_html=True,
            )

    # Side-by-side SQL preview
    st.markdown("")
    prev_col1, prev_col2 = st.columns(2, gap="medium")

    with prev_col1:
        st.markdown('<div class="section-label">Your SQL</div>', unsafe_allow_html=True)
        _render_highlighted(st.session_state.sql)

    pending = st.session_state.get("pending_rewrite")
    with prev_col2:
        st.markdown('<div class="section-label">Suggested SQL</div>', unsafe_allow_html=True)
        if pending and pending.valid:
            _render_highlighted(pending.sql)
        elif pending and not pending.valid:
            _render_highlighted(st.session_state.sql)
            for issue in pending.syntax_issues:
                st.markdown(
                    '<div class="syntax-error">&#128308; ' + issue + '</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No rewrite available.")

    # Action buttons
    st.markdown("")
    col_accept, col_original, col_cancel = st.columns([2, 2, 1])

    with col_accept:
        run_suggested = st.button(
            "Run suggested SQL",
            type="primary",
            use_container_width=True,
            disabled=(pending is None or not pending.valid),
        )
    with col_original:
        run_original = st.button("Run my SQL as-is", use_container_width=True)
    with col_cancel:
        cancelled = st.button("Cancel", use_container_width=True)

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
# Results
# ---------------------------------------------------------------------------

if st.session_state.result_dfs:
    st.divider()
    st.markdown('<div class="section-label">Results</div>', unsafe_allow_html=True)
    for entry in st.session_state.result_dfs:
        df = entry["df"]
        idx = entry["index"]
        preview = entry["sql"][:60] + ("..." if len(entry["sql"]) > 60 else "")
        with st.expander("Statement " + str(idx) + " — " + preview, expanded=True):
            rows, cols = df.shape
            st.markdown(
                '<div class="result-meta">'
                '<span style="color:#a6e3a1;font-weight:600;">' + str(rows) + ' rows</span>'
                '&nbsp;&nbsp;|&nbsp;&nbsp;'
                '<span style="color:#89b4fa;font-weight:600;">' + str(cols) + ' columns</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            if df.empty:
                st.info("This statement returned no results.")
            else:
                st.dataframe(df, use_container_width=True)

elif st.session_state.result_df is not None:
    st.divider()
    df = st.session_state.result_df
    rows, cols = df.shape
    st.markdown('<div class="section-label">Results</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="result-meta">'
        '<span style="color:#a6e3a1;font-weight:600;">' + str(rows) + ' rows</span>'
        '&nbsp;&nbsp;|&nbsp;&nbsp;'
        '<span style="color:#89b4fa;font-weight:600;">' + str(cols) + ' columns</span>'
        '</div>',
        unsafe_allow_html=True,
    )
    if df.empty:
        st.info("The query returned no results.")
    else:
        st.dataframe(df, use_container_width=True)
