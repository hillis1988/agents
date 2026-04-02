# Test Evidence Report — Snowflake Analyst IDE

| | |
|---|---|
| **Date** | 27 March 2026 |
| **Environment** | Windows / Python 3.13.6 |
| **Test runner** | pytest 9.0.2 |
| **Hypothesis** | 6.151.9 (property-based testing) |
| **Result** | **79 passed, 0 failed, 1 warning** |

> The single warning is a third-party `pyarrow` version incompatibility in the Snowflake connector package. It does not affect test outcomes and is not present in the Streamlit in Snowflake runtime.

---

## Summary by Test Module

| Module | Tests | Passed | Failed |
|---|---|---|---|
| `tests/test_properties.py` | 5 | 5 | 0 |
| `tests/test_snowflake_connector.py` | 26 | 26 | 0 |
| `tests/test_sql_analyzer.py` | 48 | 48 | 0 |
| **Total** | **79** | **79** | **0** |

---

## test_properties.py — Property-Based Tests

These tests use [Hypothesis](https://hypothesis.readthedocs.io/) to verify universal properties across hundreds of randomly generated inputs. No live Snowflake connection is required.

| Test | Property Validated | Result |
|---|---|---|
| `test_sql_length_boundary` | SQL inputs over 10,000 characters are rejected; valid inputs within the limit are accepted | PASSED |
| `test_empty_whitespace_rejection` | Whitespace-only and control-character inputs return no analysis tips | PASSED |
| `test_render_metadata` | Result DataFrames always report correct row count, column count, and column names | PASSED |
| `test_history_append` | Each executed query is appended to history with a timestamp no earlier than submission time | PASSED |
| `test_history_ordering` | Query history is always ordered most-recent first | PASSED |

---

## test_snowflake_connector.py — Connector Unit Tests

All Snowflake calls are mocked. No live instance is required.

| Class | Test | Result |
|---|---|---|
| `TestCustomExceptions` | `test_connection_error_is_exception` | PASSED |
| `TestCustomExceptions` | `test_query_error_is_exception` | PASSED |
| `TestGetConnectionSuccess` | `test_returns_connection_from_env_vars` | PASSED |
| `TestGetConnectionSuccess` | `test_connection_stored_in_session_state` | PASSED |
| `TestGetConnectionFailure` | `test_raises_on_connect_exception` | PASSED |
| `TestGetConnectionFailure` | `test_raises_on_missing_credentials` | PASSED |
| `TestGetConnectionReuse` | `test_reuses_existing_connection` | PASSED |
| `TestCredentialResolutionEnvVars` | `test_resolves_all_keys_from_env` | PASSED |
| `TestCredentialResolutionEnvVars` | `test_partial_env_raises_connection_error` | PASSED |
| `TestCredentialResolutionSecrets` | `test_resolves_all_keys_from_secrets` | PASSED |
| `TestCredentialResolutionSecrets` | `test_secrets_takes_priority_over_env` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_ACCOUNT]` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_USER]` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_PASSWORD]` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_WAREHOUSE]` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_DATABASE]` | PASSED |
| `TestMissingCredentials` | `test_raises_when_any_key_absent[SNOWFLAKE_SCHEMA]` | PASSED |
| `TestExecuteSuccess` | `test_returns_dataframe_with_correct_columns` | PASSED |
| `TestExecuteSuccess` | `test_returns_empty_dataframe_for_zero_rows` | PASSED |
| `TestExecuteProgrammingError` | `test_raises_query_error_on_programming_error` | PASSED |
| `TestExecuteProgrammingError` | `test_raises_query_error_on_unexpected_exception` | PASSED |
| `TestExecuteProgrammingError` | `test_original_message_preserved` | PASSED |
| `TestSiSConnection` | `test_returns_snowpark_session_when_sis` | PASSED |
| `TestSiSConnection` | `test_execute_uses_snowpark_sql_to_pandas` | PASSED |
| `TestSiSConnection` | `test_execute_sis_error_raises_query_error` | PASSED |
| `TestSiSConnection` | `test_falls_back_to_connector_when_not_sis` | PASSED |

---

## test_sql_analyzer.py — SQL Analyser Unit Tests

| Class | Test | Result |
|---|---|---|
| `TestTipDataclass` | `test_tip_fields` | PASSED |
| `TestTipDataclass` | `test_tip_info_severity` | PASSED |
| `TestRewriteResult` | `test_valid_result` | PASSED |
| `TestRewriteResult` | `test_invalid_result_has_issues` | PASSED |
| `TestSplitStatements` | `test_single_statement` | PASSED |
| `TestSplitStatements` | `test_two_statements` | PASSED |
| `TestSplitStatements` | `test_trailing_semicolon` | PASSED |
| `TestSplitStatements` | `test_semicolon_inside_string_not_split` | PASSED |
| `TestSplitStatements` | `test_empty_string` | PASSED |
| `TestSplitStatements` | `test_whitespace_only` | PASSED |
| `TestMissingLimit` | `test_detects_missing_limit` | PASSED |
| `TestMissingLimit` | `test_no_tip_when_limit_present` | PASSED |
| `TestSelectStar` | `test_detects_select_star` | PASSED |
| `TestSelectStar` | `test_no_tip_when_columns_explicit` | PASSED |
| `TestSelectStar` | `test_count_star_does_not_trigger` | PASSED |
| `TestMissingWhere` | `test_detects_missing_where` | PASSED |
| `TestMissingWhere` | `test_no_tip_when_where_present` | PASSED |
| `TestMissingWhere` | `test_no_missing_where_tip_for_join_query` | PASSED |
| `TestCartesianJoin` | `test_detects_cartesian_join` | PASSED |
| `TestCartesianJoin` | `test_no_tip_when_on_clause_present` | PASSED |
| `TestCartesianJoin` | `test_no_tip_when_using_clause_present` | PASSED |
| `TestLeadingWildcard` | `test_detects_leading_wildcard` | PASSED |
| `TestLeadingWildcard` | `test_no_tip_for_trailing_wildcard` | PASSED |
| `TestSubqueryInWhere` | `test_detects_in_subquery` | PASSED |
| `TestSubqueryInWhere` | `test_no_tip_for_in_list` | PASSED |
| `TestMissingAlias` | `test_detects_unaliased_subquery` | PASSED |
| `TestMissingAlias` | `test_no_tip_when_alias_present` | PASSED |
| `TestMultiStatement` | `test_analyzes_first_select` | PASSED |
| `TestMultiStatement` | `test_non_select_only_returns_no_tips` | PASSED |
| `TestEdgeCases` | `test_empty_sql_returns_no_tips` | PASSED |
| `TestEdgeCases` | `test_whitespace_sql_returns_no_tips` | PASSED |
| `TestEdgeCases` | `test_non_select_returns_no_tips` | PASSED |
| `TestEdgeCases` | `test_multiple_tips_returned` | PASSED |
| `TestRewriteReturnType` | `test_returns_rewrite_result` | PASSED |
| `TestRewriteReturnType` | `test_no_tips_returns_original_sql` | PASSED |
| `TestRewriteMissingLimit` | `test_appends_limit` | PASSED |
| `TestRewriteMissingLimit` | `test_limit_appended_at_end` | PASSED |
| `TestRewriteMissingLimit` | `test_valid_flag_set` | PASSED |
| `TestRewriteSelectStar` | `test_replaces_star_with_comment_hint` | PASSED |
| `TestRewriteMissingWhere` | `test_appends_where_hint` | PASSED |
| `TestRewriteMissingWhere` | `test_original_content_preserved` | PASSED |
| `TestRewriteCartesianJoin` | `test_appends_warning_comment` | PASSED |
| `TestRewriteCartesianJoin` | `test_original_content_preserved` | PASSED |
| `TestRewriteMultipleTips` | `test_all_rewrites_applied` | PASSED |
| `TestRewriteMultipleTips` | `test_no_tips_returns_original` | PASSED |
| `TestRewriteValidation` | `test_valid_rewrite_has_no_issues` | PASSED |
| `TestRewriteValidation` | `test_does_not_introduce_duplicate_where` | PASSED |
| `TestRewriteValidation` | `test_does_not_introduce_duplicate_limit` | PASSED |

---

## Test Coverage Notes

| Area | Approach | Coverage |
|---|---|---|
| SQL anti-pattern detection | Unit tests per rule, positive and negative cases | All 7 rules covered |
| SQL rewrite logic | Unit tests verifying additive-only rewrites | All rewrite paths covered |
| Rewrite validation | Tests for duplicate clause detection, balanced parentheses | Per-statement validation confirmed |
| Multi-statement support | Tests for split, analyse, and rewrite across multiple statements | Covered |
| Snowflake connector | Mocked unit tests for env vars, Streamlit secrets, SiS session, error handling | All credential and execution paths covered |
| Input validation | Property-based tests over random inputs (100 examples each) | Length boundary, empty input, whitespace rejection |
| History management | Property-based tests over random query sequences | Append correctness and ordering confirmed |

---

## Dependencies

No live Snowflake connection is required to run the test suite. All external calls are mocked using `unittest.mock`.

```
pytest==9.0.2
hypothesis==6.151.9
hypothesis[pandas]
pandas
snowflake-connector-python  (mocked in tests)
```
