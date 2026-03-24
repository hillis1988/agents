"""SQL analysis and rewrite utilities.

Zero third-party dependencies — uses only the Python standard library (re).
Compatible with Streamlit in Snowflake (SiS).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Literal, Optional


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Tip:
    code: str
    message: str
    severity: Literal["warning", "info"]


@dataclass
class RewriteResult:
    sql: str
    valid: bool
    syntax_issues: List[str] = field(default_factory=list)


@dataclass
class StatementTip:
    """Tips for a single statement within a multi-statement input."""
    index: int          # 1-based position in the original input
    statement: str      # the original statement text
    tips: List[Tip] = field(default_factory=list)


# Anti-pattern code constants
MISSING_LIMIT   = "MISSING_LIMIT"
SELECT_STAR     = "SELECT_STAR"
MISSING_WHERE   = "MISSING_WHERE"
CARTESIAN_JOIN  = "CARTESIAN_JOIN"
LEADING_WILDCARD = "LEADING_WILDCARD"
IMPLICIT_CONVERSION = "IMPLICIT_CONVERSION"
MISSING_ALIAS   = "MISSING_ALIAS"
SUBQUERY_IN_WHERE = "SUBQUERY_IN_WHERE"
UNSUPPORTED_STATEMENT = "UNSUPPORTED_STATEMENT"


# ---------------------------------------------------------------------------
# Comment stripping
# ---------------------------------------------------------------------------

_SINGLE_LINE_COMMENT = re.compile(r'--[^\n]*')
_MULTI_LINE_COMMENT  = re.compile(r'/\*.*?\*/', re.DOTALL)


def _strip_comments(sql: str) -> str:
    sql = _MULTI_LINE_COMMENT.sub(' ', sql)
    sql = _SINGLE_LINE_COMMENT.sub(' ', sql)
    return sql


# ---------------------------------------------------------------------------
# Multi-statement splitting
# ---------------------------------------------------------------------------

def split_statements(sql: str) -> List[str]:
    """Split *sql* on semicolons, returning non-empty statements."""
    # Avoid splitting on semicolons inside string literals
    parts = re.split(r';(?=(?:[^\'\"]*[\'"][^\'\"]*[\'"])*[^\'\"]*$)', sql)
    return [p.strip() for p in parts if p.strip()]


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

def _is_select(sql: str) -> bool:
    return bool(re.match(r'\s*SELECT\b', sql, re.IGNORECASE))


def _has_limit(sql: str) -> bool:
    return bool(re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE))


def _has_select_star(sql: str) -> bool:
    # SELECT * or SELECT t.* — but not COUNT(*) or other aggregate(*)
    # Strategy: find * tokens that are NOT preceded by an opening paren
    # i.e. not inside a function call like COUNT(*)
    return bool(re.search(r'\bSELECT\b(?:(?!\bFROM\b).)*(?<!\()\s*\*\s*(?!\s*\))',
                           sql, re.IGNORECASE | re.DOTALL))


def _has_where(sql: str) -> bool:
    return bool(re.search(r'\bWHERE\b', sql, re.IGNORECASE))


def _has_join(sql: str) -> bool:
    return bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE))


def _has_on_or_using(sql: str) -> bool:
    return bool(re.search(r'\bJOIN\b.+?\b(ON|USING)\b', sql,
                           re.IGNORECASE | re.DOTALL))


def _has_leading_wildcard_like(sql: str) -> bool:
    """Detect LIKE '%value' patterns that prevent index use."""
    return bool(re.search(r"\bLIKE\s+['\"]%", sql, re.IGNORECASE))


def _has_implicit_conversion(sql: str) -> bool:
    """Detect WHERE numeric_col = '123' style implicit type conversions."""
    return bool(re.search(r"\bWHERE\b.+\bIS\s+NOT\s+NULL\b|\bWHERE\b.+=\s*'[0-9]",
                           sql, re.IGNORECASE | re.DOTALL))


def _has_subquery_in_where(sql: str) -> bool:
    """Detect correlated subqueries in WHERE (IN (SELECT ...))."""
    return bool(re.search(r'\bIN\s*\(\s*SELECT\b', sql, re.IGNORECASE))


def _has_use_statement(sql: str) -> bool:
    """Detect USE DATABASE/SCHEMA/WAREHOUSE/ROLE — unsupported in SiS."""
    return bool(re.match(r'\s*USE\b', sql, re.IGNORECASE))


_SQL_KEYWORDS = re.compile(
    r'^(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|USING|'
    r'GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|ALL|DISTINCT|AS|WITH|'
    r'INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|MERGE|CASE|WHEN|THEN|ELSE|END|'
    r'AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL)\b',
    re.IGNORECASE,
)

def _has_unaliased_subquery(sql: str) -> bool:
    """Detect subqueries in FROM without an alias."""
    for m in re.finditer(r'\bFROM\s*\(', sql, re.IGNORECASE):
        start = m.end() - 1
        depth = 0
        pos = start
        while pos < len(sql):
            if sql[pos] == '(':
                depth += 1
            elif sql[pos] == ')':
                depth -= 1
                if depth == 0:
                    after = sql[pos + 1:].lstrip()
                    # Aliased if next token is a word that is NOT a SQL keyword
                    word_match = re.match(r'[\w"]+', after)
                    if word_match and not _SQL_KEYWORDS.match(word_match.group()):
                        return False  # has alias
                    return True  # no alias
            pos += 1
    return False


# ---------------------------------------------------------------------------
# Rewrite syntax validation
# ---------------------------------------------------------------------------

# Balanced parentheses check
def _balanced_parens(sql: str) -> bool:
    depth = 0
    in_string = False
    quote_char = None
    for ch in sql:
        if in_string:
            if ch == quote_char:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote_char = ch
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _validate_rewrite(original: str, rewritten: str) -> RewriteResult:
    """Run basic syntax checks on a rewritten SQL string.

    Checks performed (per statement):
    - Rewritten SQL still starts with a valid DML keyword
    - Parentheses are balanced
    - No duplicate WHERE / LIMIT within a single statement
    - LIMIT value is a positive integer
    """
    issues: List[str] = []
    clean = _strip_comments(rewritten).strip()

    # Must still be a recognisable SQL statement
    if not re.match(r'\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|MERGE|WITH)\b',
                    clean, re.IGNORECASE):
        issues.append("Rewrite does not begin with a valid SQL keyword.")

    # Balanced parentheses across the whole string
    if not _balanced_parens(clean):
        issues.append("Rewrite has unbalanced parentheses.")

    # Per-statement checks — one LIMIT / WHERE per statement is fine
    for stmt in split_statements(clean):
        s = _strip_comments(stmt)

        where_count = len(re.findall(r'\bWHERE\b', s, re.IGNORECASE))
        if where_count > 1:
            issues.append(f"A statement contains {where_count} WHERE clauses.")

        limit_count = len(re.findall(r'\bLIMIT\b', s, re.IGNORECASE))
        if limit_count > 1:
            issues.append(f"A statement contains {limit_count} LIMIT clauses.")

        limit_match = re.search(r'\bLIMIT\s+(\S+)', s, re.IGNORECASE)
        if limit_match:
            val = limit_match.group(1).rstrip(';')
            if not val.isdigit() or int(val) <= 0:
                issues.append(f"LIMIT value '{val}' is not a positive integer.")

    return RewriteResult(sql=rewritten, valid=len(issues) == 0, syntax_issues=issues)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _analyze_one(stmt: str) -> List[Tip]:
    """Return tips for a single SQL statement."""
    tips: List[Tip] = []
    clean = _strip_comments(stmt)

    if not _has_limit(clean):
        tips.append(Tip(
            code=MISSING_LIMIT,
            message="Add a LIMIT clause to avoid fetching unexpectedly large result sets.",
            severity="warning",
        ))

    if _has_select_star(clean):
        tips.append(Tip(
            code=SELECT_STAR,
            message="Avoid SELECT *; specify only the columns you need to reduce data scanned.",
            severity="info",
        ))

    if not _has_join(clean) and not _has_where(clean):
        tips.append(Tip(
            code=MISSING_WHERE,
            message="Add a WHERE clause to filter rows and reduce scan cost.",
            severity="warning",
        ))

    if _has_join(clean) and not _has_on_or_using(clean):
        tips.append(Tip(
            code=CARTESIAN_JOIN,
            message="A JOIN without ON or USING produces a Cartesian product — this can be extremely expensive.",
            severity="warning",
        ))

    if _has_leading_wildcard_like(clean):
        tips.append(Tip(
            code=LEADING_WILDCARD,
            message="LIKE '%value' with a leading wildcard cannot use partition pruning. Consider a full-text search or reverse the pattern.",
            severity="warning",
        ))

    if _has_subquery_in_where(clean):
        tips.append(Tip(
            code=SUBQUERY_IN_WHERE,
            message="IN (SELECT ...) subqueries can be expensive. Consider rewriting as a JOIN.",
            severity="info",
        ))

    if _has_unaliased_subquery(clean):
        tips.append(Tip(
            code=MISSING_ALIAS,
            message="Subquery in FROM clause has no alias. Add an alias for clarity and to avoid errors.",
            severity="info",
        ))

    return tips


def analyze(sql: str) -> List[Tip]:
    """Analyse *sql* and return a flat list of Tips across all SELECT statements.

    For multi-statement inputs each SELECT is analysed independently.
    Tips from all statements are combined into one list (duplicates removed by code).
    Use analyze_all() if you need per-statement breakdown.
    """
    if not sql or not sql.strip():
        return []

    statements = split_statements(sql)

    # Block unsupported USE commands first
    if any(_has_use_statement(s) for s in statements):
        return [Tip(
            code=UNSUPPORTED_STATEMENT,
            message="USE statements (USE DATABASE/SCHEMA/WAREHOUSE/ROLE) are not supported "
                    "in Streamlit in Snowflake. Set the context in your connection settings instead.",
            severity="warning",
        )]

    seen_codes: set = set()
    tips: List[Tip] = []
    for stmt in statements:
        if not re.match(r'\s*SELECT\b', stmt, re.IGNORECASE):
            continue
        for tip in _analyze_one(stmt):
            if tip.code not in seen_codes:
                seen_codes.add(tip.code)
                tips.append(tip)

    return tips


def analyze_all(sql: str) -> List[StatementTip]:
    """Return per-statement tips for every SELECT in *sql*.

    Useful for the UI to show which statement each tip belongs to.
    """
    if not sql or not sql.strip():
        return []

    statements = split_statements(sql)

    if any(_has_use_statement(s) for s in statements):
        return [StatementTip(
            index=1,
            statement=statements[0],
            tips=[Tip(
                code=UNSUPPORTED_STATEMENT,
                message="USE statements are not supported in Streamlit in Snowflake.",
                severity="warning",
            )],
        )]

    result: List[StatementTip] = []
    for i, stmt in enumerate(statements, 1):
        if not re.match(r'\s*SELECT\b', stmt, re.IGNORECASE):
            continue
        tips = _analyze_one(stmt)
        if tips:
            result.append(StatementTip(index=i, statement=stmt, tips=tips))

    return result


def _rewrite_one(stmt: str, tips: List[Tip]) -> str:
    """Apply tips to a single SELECT statement and return the rewritten string."""
    codes = {tip.code for tip in tips}
    result = stmt.rstrip()
    clean = _strip_comments(result)
    has_existing_where = _has_where(clean)
    has_existing_join = _has_join(clean)

    if SELECT_STAR in codes:
        result = re.sub(
            r'(?<=SELECT\s)(\s*)\*(?!\s*\))',
            r'\1/* specify columns */ *',
            result,
            count=1,
            flags=re.IGNORECASE,
        )

    if MISSING_WHERE in codes and not has_existing_where and not has_existing_join:
        result += "\nWHERE TRUE -- add your filter here"

    if MISSING_LIMIT in codes and not _has_limit(clean):
        result += "\nLIMIT 1000"

    if CARTESIAN_JOIN in codes:
        result += "\n/* WARNING: Cartesian join detected — add an ON or USING clause */"

    if SUBQUERY_IN_WHERE in codes:
        result += "\n-- Consider rewriting IN (SELECT ...) as a JOIN for better performance"

    return result


def rewrite(sql: str, tips: List[Tip]) -> RewriteResult:
    """Apply *tips* to every SELECT statement in *sql*.

    Each SELECT is rewritten independently. Non-SELECT statements are preserved
    unchanged. Returns a validated RewriteResult.
    """
    if not tips:
        return RewriteResult(sql=sql, valid=True)

    statements = split_statements(sql)
    if not statements:
        return RewriteResult(sql=sql, valid=True)

    rewritten: List[str] = []
    for stmt in statements:
        if re.match(r'\s*SELECT\b', stmt, re.IGNORECASE):
            rewritten.append(_rewrite_one(stmt, tips))
        else:
            rewritten.append(stmt)

    final_sql = ";\n".join(rewritten)
    return _validate_rewrite(sql, final_sql)


# ---------------------------------------------------------------------------
# SQL formatter (stdlib only — safe for Streamlit in Snowflake)
# ---------------------------------------------------------------------------

# Clause keywords that should start on a new line at the top level
_CLAUSE_KEYWORDS = re.compile(
    r'\b(SELECT|FROM|WHERE|'
    r'LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|'
    r'FULL\s+(?:OUTER\s+)?JOIN|INNER\s+JOIN|CROSS\s+JOIN|JOIN|'
    r'ON|USING|'
    r'GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|'
    r'UNION(?:\s+ALL)?|EXCEPT|INTERSECT|'
    r'WITH)\b',
    re.IGNORECASE,
)


def format_sql(sql: str) -> str:
    """Return a lightly formatted version of *sql* for display purposes.

    Rules applied (stdlib only, no third-party deps):
    - Each major clause starts on its own line
    - SELECT columns are comma-separated, one per line with 4-space indent
    - Preserves existing newlines the user typed
    - Falls back to the original string on any error
    """
    if not sql or not sql.strip():
        return sql

    try:
        # Normalise whitespace runs (but keep newlines the user typed)
        # Work statement by statement
        statements = split_statements(sql)
        formatted_parts = []

        for stmt in statements:
            formatted_parts.append(_format_single(stmt))

        return ";\n\n".join(formatted_parts)
    except Exception:
        return sql  # never crash — just show the original


def _format_single(sql: str) -> str:
    """Format one SQL statement."""
    # Collapse all whitespace (including newlines) to single spaces
    flat = re.sub(r'\s+', ' ', sql.strip())

    # Split on clause boundaries, keeping the keyword
    tokens = _CLAUSE_KEYWORDS.split(flat)
    # _CLAUSE_KEYWORDS has groups so split returns [before, kw, after, kw, after ...]
    # Rebuild into (keyword, body) pairs
    parts: List[tuple] = []
    i = 0
    # First token is anything before the first keyword (usually empty)
    preamble = tokens[0].strip()
    i = 1
    while i < len(tokens):
        kw = tokens[i].strip()
        body = tokens[i + 1].strip() if i + 1 < len(tokens) else ""
        parts.append((kw, body))
        i += 2

    if not parts:
        return sql

    lines: List[str] = []
    if preamble:
        lines.append(preamble)

    for idx, (kw, body) in enumerate(parts):
        kw_upper = re.sub(r'\s+', ' ', kw).upper()

        if kw_upper == 'SELECT':
            lines.append('SELECT')
            if body:
                # Split columns on commas that are not inside parentheses
                cols = _split_on_top_level_comma(body)
                lines.append(
                    ',\n    '.join(c.strip() for c in cols)
                    if len(cols) > 1
                    else '    ' + body
                )
        elif kw_upper in ('ON', 'USING'):
            # Indent ON/USING under the JOIN line
            lines.append(f'    {kw_upper} {body}')
        else:
            lines.append(f'{kw_upper} {body}' if body else kw_upper)

    return '\n'.join(lines)


def _split_on_top_level_comma(s: str) -> List[str]:
    """Split *s* on commas that are not inside parentheses."""
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    for ch in s:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append(''.join(current).strip())
    return parts


# ---------------------------------------------------------------------------
# Cost saving estimator
# ---------------------------------------------------------------------------

# Rough credit weights per anti-pattern (relative, not exact billing figures).
# Based on common Snowflake cost-driver guidance:
#   SELECT *       → scans all columns → high data volume impact
#   MISSING_LIMIT  → unbounded result → risk of full table scan materialisation
#   MISSING_WHERE  → full table scan
#   CARTESIAN_JOIN → exponential row explosion
#   LEADING_WILDCARD → disables micro-partition pruning
#   SUBQUERY_IN_WHERE → correlated re-execution per row
#   MISSING_ALIAS  → minor — clarity only, no direct cost
_COST_WEIGHTS = {
    SELECT_STAR:       0.30,
    MISSING_LIMIT:     0.20,
    MISSING_WHERE:     0.25,
    CARTESIAN_JOIN:    0.50,
    LEADING_WILDCARD:  0.15,
    SUBQUERY_IN_WHERE: 0.20,
    MISSING_ALIAS:     0.02,
}

# Snowflake list price per credit (USD) as of 2024 — Enterprise tier
_CREDIT_PRICE_USD = 3.00
# Assumed baseline credits consumed by a "bad" query on a Medium warehouse
_BASELINE_CREDITS = 0.05


def estimate_savings(tips: List[Tip]) -> dict:
    """Return a cost saving estimate dict for the given tips.

    Keys:
        credits_saved  float  — estimated Snowflake credits saved per run
        usd_saved      float  — USD equivalent at Enterprise list price
        pct_reduction  int    — % reduction vs unoptimised baseline
        label          str    — human-readable summary string
    """
    if not tips:
        return {"credits_saved": 0.0, "usd_saved": 0.0, "pct_reduction": 0, "label": "No savings estimated."}

    total_weight = sum(_COST_WEIGHTS.get(t.code, 0.0) for t in tips)
    # Cap at 90% — we can never guarantee 100% savings
    pct = min(int(total_weight * 100), 90)
    credits = round(_BASELINE_CREDITS * (pct / 100), 4)
    usd = round(credits * _CREDIT_PRICE_USD, 4)

    label = (
        f"Applying these suggestions could reduce query cost by ~{pct}% "
        f"(≈ {credits} credits / ${usd:.4f} per execution at Enterprise list price)."
    )
    return {
        "credits_saved": credits,
        "usd_saved": usd,
        "pct_reduction": pct,
        "label": label,
    }
