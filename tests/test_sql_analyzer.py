"""Unit tests for utilities/sql_analyzer.py."""

import pytest
from utilities.sql_analyzer import (
    Tip,
    RewriteResult,
    analyze,
    rewrite,
    split_statements,
    MISSING_LIMIT,
    SELECT_STAR,
    MISSING_WHERE,
    CARTESIAN_JOIN,
    LEADING_WILDCARD,
    SUBQUERY_IN_WHERE,
    MISSING_ALIAS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def codes(tips):
    return {t.code for t in tips}


def rewrite_sql(sql, tips):
    """Convenience: return the rewritten SQL string from a RewriteResult."""
    return rewrite(sql, tips).sql


# ---------------------------------------------------------------------------
# Tip dataclass
# ---------------------------------------------------------------------------

class TestTipDataclass:
    def test_tip_fields(self):
        tip = Tip(code=MISSING_LIMIT, message="Add a LIMIT", severity="warning")
        assert tip.code == MISSING_LIMIT
        assert tip.message == "Add a LIMIT"
        assert tip.severity == "warning"

    def test_tip_info_severity(self):
        tip = Tip(code=SELECT_STAR, message="Avoid *", severity="info")
        assert tip.severity == "info"


# ---------------------------------------------------------------------------
# RewriteResult dataclass
# ---------------------------------------------------------------------------

class TestRewriteResult:
    def test_valid_result(self):
        r = RewriteResult(sql="SELECT 1", valid=True)
        assert r.valid is True
        assert r.syntax_issues == []

    def test_invalid_result_has_issues(self):
        r = RewriteResult(sql="BAD", valid=False, syntax_issues=["Not a valid SQL keyword."])
        assert r.valid is False
        assert len(r.syntax_issues) == 1


# ---------------------------------------------------------------------------
# split_statements
# ---------------------------------------------------------------------------

class TestSplitStatements:
    def test_single_statement(self):
        assert split_statements("SELECT 1") == ["SELECT 1"]

    def test_two_statements(self):
        parts = split_statements("SELECT 1; SELECT 2")
        assert len(parts) == 2
        assert parts[0] == "SELECT 1"
        assert parts[1] == "SELECT 2"

    def test_trailing_semicolon(self):
        parts = split_statements("SELECT 1;")
        assert parts == ["SELECT 1"]

    def test_semicolon_inside_string_not_split(self):
        sql = "SELECT 'a;b' FROM t"
        parts = split_statements(sql)
        assert len(parts) == 1

    def test_empty_string(self):
        assert split_statements("") == []

    def test_whitespace_only(self):
        assert split_statements("   ;   ") == []


# ---------------------------------------------------------------------------
# Detection: MISSING_LIMIT
# ---------------------------------------------------------------------------

class TestMissingLimit:
    def test_detects_missing_limit(self):
        assert MISSING_LIMIT in codes(analyze("SELECT id FROM orders"))

    def test_no_tip_when_limit_present(self):
        assert MISSING_LIMIT not in codes(analyze("SELECT id FROM orders LIMIT 100"))


# ---------------------------------------------------------------------------
# Detection: SELECT_STAR
# ---------------------------------------------------------------------------

class TestSelectStar:
    def test_detects_select_star(self):
        assert SELECT_STAR in codes(analyze("SELECT * FROM customers LIMIT 10"))

    def test_no_tip_when_columns_explicit(self):
        assert SELECT_STAR not in codes(analyze("SELECT id, name FROM customers LIMIT 10"))

    def test_count_star_does_not_trigger(self):
        # COUNT(*) should not trigger SELECT_STAR
        assert SELECT_STAR not in codes(analyze("SELECT COUNT(*) FROM orders LIMIT 10"))


# ---------------------------------------------------------------------------
# Detection: MISSING_WHERE
# ---------------------------------------------------------------------------

class TestMissingWhere:
    def test_detects_missing_where(self):
        assert MISSING_WHERE in codes(analyze("SELECT id FROM orders LIMIT 10"))

    def test_no_tip_when_where_present(self):
        assert MISSING_WHERE not in codes(
            analyze("SELECT id FROM orders WHERE status = 'open' LIMIT 10")
        )

    def test_no_missing_where_tip_for_join_query(self):
        sql = "SELECT a.id FROM orders a JOIN customers b ON a.customer_id = b.id LIMIT 10"
        assert MISSING_WHERE not in codes(analyze(sql))


# ---------------------------------------------------------------------------
# Detection: CARTESIAN_JOIN
# ---------------------------------------------------------------------------

class TestCartesianJoin:
    def test_detects_cartesian_join(self):
        sql = "SELECT a.id, b.name FROM orders a JOIN customers b LIMIT 10"
        assert CARTESIAN_JOIN in codes(analyze(sql))

    def test_no_tip_when_on_clause_present(self):
        sql = "SELECT a.id FROM orders a JOIN customers b ON a.customer_id = b.id LIMIT 10"
        assert CARTESIAN_JOIN not in codes(analyze(sql))

    def test_no_tip_when_using_clause_present(self):
        sql = "SELECT id FROM orders JOIN customers USING (customer_id) LIMIT 10"
        assert CARTESIAN_JOIN not in codes(analyze(sql))


# ---------------------------------------------------------------------------
# Detection: LEADING_WILDCARD
# ---------------------------------------------------------------------------

class TestLeadingWildcard:
    def test_detects_leading_wildcard(self):
        sql = "SELECT id FROM orders WHERE name LIKE '%smith' LIMIT 10"
        assert LEADING_WILDCARD in codes(analyze(sql))

    def test_no_tip_for_trailing_wildcard(self):
        sql = "SELECT id FROM orders WHERE name LIKE 'smith%' LIMIT 10"
        assert LEADING_WILDCARD not in codes(analyze(sql))


# ---------------------------------------------------------------------------
# Detection: SUBQUERY_IN_WHERE
# ---------------------------------------------------------------------------

class TestSubqueryInWhere:
    def test_detects_in_subquery(self):
        sql = "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers) LIMIT 10"
        assert SUBQUERY_IN_WHERE in codes(analyze(sql))

    def test_no_tip_for_in_list(self):
        sql = "SELECT id FROM orders WHERE status IN ('open', 'closed') LIMIT 10"
        assert SUBQUERY_IN_WHERE not in codes(analyze(sql))


# ---------------------------------------------------------------------------
# Detection: MISSING_ALIAS (unaliased subquery)
# ---------------------------------------------------------------------------

class TestMissingAlias:
    def test_detects_unaliased_subquery(self):
        sql = "SELECT id FROM (SELECT id FROM orders) LIMIT 10"
        assert MISSING_ALIAS in codes(analyze(sql))

    def test_no_tip_when_alias_present(self):
        sql = "SELECT id FROM (SELECT id FROM orders) sub LIMIT 10"
        assert MISSING_ALIAS not in codes(analyze(sql))


# ---------------------------------------------------------------------------
# Multi-statement: analyze uses first SELECT
# ---------------------------------------------------------------------------

class TestMultiStatement:
    def test_analyzes_first_select(self):
        sql = "CREATE TABLE t AS SELECT 1; SELECT * FROM t"
        tip_codes = codes(analyze(sql))
        # SELECT * FROM t should trigger SELECT_STAR
        assert SELECT_STAR in tip_codes

    def test_non_select_only_returns_no_tips(self):
        assert analyze("INSERT INTO t VALUES (1); UPDATE t SET x=1") == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_sql_returns_no_tips(self):
        assert analyze("") == []

    def test_whitespace_sql_returns_no_tips(self):
        assert analyze("   \n\t  ") == []

    def test_non_select_returns_no_tips(self):
        assert analyze("INSERT INTO t VALUES (1)") == []

    def test_multiple_tips_returned(self):
        tip_codes = codes(analyze("SELECT * FROM orders"))
        assert MISSING_LIMIT in tip_codes
        assert SELECT_STAR in tip_codes
        assert MISSING_WHERE in tip_codes


# ---------------------------------------------------------------------------
# Rewrite: return type is RewriteResult
# ---------------------------------------------------------------------------

class TestRewriteReturnType:
    def test_returns_rewrite_result(self):
        sql = "SELECT id FROM orders WHERE status = 'open'"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        result = rewrite(sql, tips)
        assert isinstance(result, RewriteResult)

    def test_no_tips_returns_original_sql(self):
        sql = "SELECT id FROM orders WHERE status = 'open' LIMIT 10"
        result = rewrite(sql, [])
        assert result.sql == sql
        assert result.valid is True


# ---------------------------------------------------------------------------
# Rewrite: MISSING_LIMIT
# ---------------------------------------------------------------------------

class TestRewriteMissingLimit:
    def test_appends_limit(self):
        sql = "SELECT id FROM orders WHERE status = 'open'"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        assert "LIMIT 1000" in result
        assert "SELECT id FROM orders" in result

    def test_limit_appended_at_end(self):
        sql = "SELECT id FROM orders WHERE status = 'open'"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        assert result.strip().endswith("LIMIT 1000")

    def test_valid_flag_set(self):
        sql = "SELECT id FROM orders WHERE status = 'open'"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        assert rewrite(sql, tips).valid is True


# ---------------------------------------------------------------------------
# Rewrite: SELECT_STAR
# ---------------------------------------------------------------------------

class TestRewriteSelectStar:
    def test_replaces_star_with_comment_hint(self):
        sql = "SELECT * FROM customers LIMIT 10"
        tips = [Tip(code=SELECT_STAR, message="", severity="info")]
        result = rewrite_sql(sql, tips)
        assert "/* specify columns */" in result
        assert "*" in result
        assert "FROM customers" in result


# ---------------------------------------------------------------------------
# Rewrite: MISSING_WHERE
# ---------------------------------------------------------------------------

class TestRewriteMissingWhere:
    def test_appends_where_hint(self):
        sql = "SELECT id FROM orders LIMIT 10"
        tips = [Tip(code=MISSING_WHERE, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        assert "WHERE TRUE" in result
        assert "add your filter" in result

    def test_original_content_preserved(self):
        sql = "SELECT id FROM orders LIMIT 10"
        tips = [Tip(code=MISSING_WHERE, message="", severity="warning")]
        assert "SELECT id FROM orders" in rewrite_sql(sql, tips)


# ---------------------------------------------------------------------------
# Rewrite: CARTESIAN_JOIN
# ---------------------------------------------------------------------------

class TestRewriteCartesianJoin:
    def test_appends_warning_comment(self):
        sql = "SELECT a.id FROM orders a JOIN customers b LIMIT 10"
        tips = [Tip(code=CARTESIAN_JOIN, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        assert "WARNING: Cartesian join detected" in result

    def test_original_content_preserved(self):
        sql = "SELECT a.id FROM orders a JOIN customers b LIMIT 10"
        tips = [Tip(code=CARTESIAN_JOIN, message="", severity="warning")]
        assert "SELECT a.id FROM orders" in rewrite_sql(sql, tips)


# ---------------------------------------------------------------------------
# Rewrite: multiple tips simultaneously
# ---------------------------------------------------------------------------

class TestRewriteMultipleTips:
    def test_all_rewrites_applied(self):
        sql = "SELECT * FROM orders"
        tips = analyze(sql)
        result = rewrite_sql(sql, tips)
        assert "/* specify columns */" in result
        assert "WHERE TRUE" in result
        assert "LIMIT 1000" in result

    def test_no_tips_returns_original(self):
        sql = "SELECT id FROM orders WHERE status = 'open' LIMIT 10"
        assert rewrite(sql, []).sql == sql


# ---------------------------------------------------------------------------
# Rewrite: syntax validation
# ---------------------------------------------------------------------------

class TestRewriteValidation:
    def test_valid_rewrite_has_no_issues(self):
        sql = "SELECT id FROM orders WHERE status = 'open'"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        result = rewrite(sql, tips)
        assert result.valid is True
        assert result.syntax_issues == []

    def test_does_not_introduce_duplicate_where(self):
        # Query already has WHERE — MISSING_WHERE rewrite should not add another
        sql = "SELECT id FROM orders WHERE status = 'open' LIMIT 10"
        tips = [Tip(code=MISSING_WHERE, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        where_count = result.upper().count("WHERE")
        assert where_count == 1

    def test_does_not_introduce_duplicate_limit(self):
        # Query already has LIMIT — MISSING_LIMIT rewrite should not add another
        sql = "SELECT id FROM orders WHERE status = 'open' LIMIT 50"
        tips = [Tip(code=MISSING_LIMIT, message="", severity="warning")]
        result = rewrite_sql(sql, tips)
        limit_count = result.upper().count("LIMIT")
        assert limit_count == 1
