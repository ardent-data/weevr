"""Tests for naming normalization functions."""

import pytest

from weevr.errors.exceptions import ConfigError
from weevr.model.naming import NamingConfig, NamingPattern
from weevr.operations.naming import (
    _tokenize,
    is_excluded,
    normalize_columns,
    normalize_name,
    normalize_table_name,
)


class TestTokenize:
    """Test word boundary tokenization."""

    def test_camel_case(self):
        assert _tokenize("httpStatus") == ["http", "status"]

    def test_pascal_case(self):
        assert _tokenize("HttpStatus") == ["http", "status"]

    def test_snake_case(self):
        assert _tokenize("http_status") == ["http", "status"]

    def test_upper_snake_case(self):
        assert _tokenize("HTTP_STATUS") == ["http", "status"]

    def test_acronym_boundary(self):
        assert _tokenize("HTTPStatus") == ["http", "status"]

    def test_single_word(self):
        assert _tokenize("simple") == ["simple"]

    def test_acronym_parser(self):
        assert _tokenize("XMLParser") == ["xml", "parser"]

    def test_multiple_acronyms(self):
        assert _tokenize("getHTTPSUrl") == ["get", "https", "url"]

    def test_numeric_suffix(self):
        assert _tokenize("column_name_v2") == ["column", "name", "v2"]

    def test_leading_underscores(self):
        assert _tokenize("__metadata") == ["metadata"]

    def test_hyphens(self):
        assert _tokenize("http-status-code") == ["http", "status", "code"]

    def test_mixed_separators(self):
        assert _tokenize("http-status_code") == ["http", "status", "code"]

    def test_empty_string(self):
        assert _tokenize("") == []

    def test_all_uppercase(self):
        assert _tokenize("AMOUNT") == ["amount"]

    def test_single_char(self):
        assert _tokenize("x") == ["x"]


class TestNormalizeNamePatterns:
    """Test each naming pattern with consistent input."""

    def test_snake_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.SNAKE_CASE) == "http_status"

    def test_camel_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.CAMEL_CASE) == "httpStatus"

    def test_pascal_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.PASCAL_CASE) == "HttpStatus"

    def test_upper_snake_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.UPPER_SNAKE_CASE) == "HTTP_STATUS"

    def test_title_snake_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.TITLE_SNAKE_CASE) == "Http_Status"

    def test_title_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.TITLE_CASE) == "Http Status"

    def test_lowercase(self):
        assert normalize_name("HTTPStatus", NamingPattern.LOWERCASE) == "httpstatus"

    def test_uppercase(self):
        assert normalize_name("HTTPStatus", NamingPattern.UPPERCASE) == "HTTPSTATUS"

    def test_none_passthrough(self):
        assert normalize_name("HTTPStatus", NamingPattern.NONE) == "HTTPStatus"

    def test_kebab_case(self):
        assert normalize_name("HTTPStatus", NamingPattern.KEBAB_CASE) == "http-status"


class TestKebabCase:
    """Test kebab-case normalization."""

    def test_snake_input(self):
        assert normalize_name("http_status", NamingPattern.KEBAB_CASE) == "http-status"

    def test_pascal_acronym_input(self):
        assert normalize_name("HTTPStatus", NamingPattern.KEBAB_CASE) == "http-status"

    def test_multi_word_snake(self):
        assert normalize_name("my_column_name", NamingPattern.KEBAB_CASE) == "my-column-name"

    def test_already_kebab(self):
        assert normalize_name("already-kebab", NamingPattern.KEBAB_CASE) == "already-kebab"


class TestNormalizeNameEdgeCases:
    """Test edge cases across patterns."""

    def test_all_lowercase_snake(self):
        assert normalize_name("amount", NamingPattern.SNAKE_CASE) == "amount"

    def test_all_uppercase_snake(self):
        assert normalize_name("AMOUNT", NamingPattern.SNAKE_CASE) == "amount"

    def test_all_lowercase_camel(self):
        assert normalize_name("amount", NamingPattern.CAMEL_CASE) == "amount"

    def test_numeric_suffix_pascal(self):
        assert normalize_name("column1", NamingPattern.PASCAL_CASE) == "Column1"

    def test_mixed_separators_snake(self):
        assert normalize_name("http-status_code", NamingPattern.SNAKE_CASE) == "http_status_code"

    def test_single_char_upper(self):
        assert normalize_name("x", NamingPattern.UPPERCASE) == "X"

    def test_empty_string_returns_unchanged(self):
        assert normalize_name("", NamingPattern.SNAKE_CASE) == ""


class TestIsExcluded:
    """Test exclusion pattern matching."""

    def test_explicit_name(self):
        assert is_excluded("_metadata", ["_metadata"]) is True

    def test_glob_star_prefix(self):
        assert is_excluded("__internal_col", ["__*"]) is True

    def test_glob_suffix(self):
        assert is_excluded("raw_data_raw", ["*_raw"]) is True

    def test_no_match(self):
        assert is_excluded("amount", ["__*", "*_raw"]) is False

    def test_question_mark(self):
        assert is_excluded("col_a", ["col_?"]) is True
        assert is_excluded("col_ab", ["col_?"]) is False

    def test_empty_patterns(self):
        assert is_excluded("anything", []) is False


class TestNormalizTableName:
    """Test table name normalization."""

    def test_snake_case(self):
        config = NamingConfig(tables=NamingPattern.SNAKE_CASE)
        assert normalize_table_name("DimCustomer", config) == "dim_customer"

    def test_none_passthrough(self):
        config = NamingConfig(tables=NamingPattern.NONE)
        assert normalize_table_name("DimCustomer", config) == "DimCustomer"

    def test_no_tables_config(self):
        config = NamingConfig()
        assert normalize_table_name("DimCustomer", config) == "DimCustomer"


class TestNormalizeTableNameReservedWords:
    """Test reserved word protection for table names."""

    def test_prefix_strategy(self):
        """Reserved table name is prefixed."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            tables=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        assert normalize_table_name("Select", config) == "_select"

    def test_quote_strategy_passthrough(self):
        """Quote strategy leaves reserved table name unchanged."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            tables=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="quote"),
        )
        assert normalize_table_name("Select", config) == "select"

    def test_error_strategy_raises(self):
        """Error strategy raises ConfigError for reserved table name."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            tables=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="error"),
        )
        with pytest.raises(ConfigError, match="select"):
            normalize_table_name("Select", config)

    def test_non_reserved_unaffected(self):
        """Non-reserved table name passes through unchanged."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            tables=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        assert normalize_table_name("DimCustomer", config) == "dim_customer"

    def test_preset_dax(self):
        """DAX preset catches DAX-reserved table name."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            tables=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", preset="dax"),  # type: ignore[arg-type]
        )
        assert normalize_table_name("Measure", config) == "_measure"

    def test_no_tables_pattern_still_checks_reserved(self):
        """Reserved word check applies even without a table pattern."""
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        assert normalize_table_name("select", config) == "_select"

    def test_no_reserved_words_config(self):
        """No reserved_words config means no protection."""
        config = NamingConfig(tables=NamingPattern.SNAKE_CASE)
        assert normalize_table_name("Select", config) == "select"


class TestNamingConfig:
    """Test NamingConfig model."""

    def test_valid_config(self):
        cfg = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            tables=NamingPattern.LOWERCASE,
            exclude=["__*"],
        )
        assert cfg.columns == NamingPattern.SNAKE_CASE
        assert cfg.tables == NamingPattern.LOWERCASE
        assert cfg.exclude == ["__*"]

    def test_defaults(self):
        cfg = NamingConfig()
        assert cfg.columns is None
        assert cfg.tables is None
        assert cfg.exclude == []

    def test_none_values(self):
        cfg = NamingConfig(columns=None, tables=None)
        assert cfg.columns is None
        assert cfg.tables is None


@pytest.mark.spark
class TestNormalizeColumnsSpark:
    """Test column normalization with Spark DataFrames."""

    def test_snake_case_columns(self, spark):
        df = spark.createDataFrame([(1, "a")], ["HttpStatus", "userName"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE)
        result = normalize_columns(df, config)
        assert result.columns == ["http_status", "user_name"]

    def test_exclusion_skips_matching(self, spark):
        df = spark.createDataFrame([(1, "a")], ["HttpStatus", "__metadata"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE, exclude=["__*"])
        result = normalize_columns(df, config)
        assert result.columns == ["http_status", "__metadata"]

    def test_none_pattern_no_change(self, spark):
        df = spark.createDataFrame([(1,)], ["HttpStatus"])
        config = NamingConfig(columns=NamingPattern.NONE)
        result = normalize_columns(df, config)
        assert result.columns == ["HttpStatus"]

    def test_no_columns_config_no_change(self, spark):
        df = spark.createDataFrame([(1,)], ["HttpStatus"])
        config = NamingConfig()
        result = normalize_columns(df, config)
        assert result.columns == ["HttpStatus"]

    def test_duplicate_columns_raises(self, spark):
        # httpStatus and http_status both normalize to http_status
        df = spark.createDataFrame([(1, 2)], ["httpStatus", "http_status"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE)
        with pytest.raises(ConfigError, match="duplicate"):
            normalize_columns(df, config)

    def test_data_preserved(self, spark):
        df = spark.createDataFrame([(1, "alice")], ["UserId", "UserName"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE)
        result = normalize_columns(df, config)
        row = result.collect()[0]
        assert row["user_id"] == 1
        assert row["user_name"] == "alice"

    def test_kebab_case_logs_warning(self, spark, caplog):
        import logging

        df = spark.createDataFrame([(1,)], ["myColumn"])
        config = NamingConfig(columns=NamingPattern.KEBAB_CASE)
        with caplog.at_level(logging.WARNING, logger="weevr.operations.naming"):
            normalize_columns(df, config)
        messages = " ".join(caplog.messages)
        assert "backtick" in messages or "kebab-case" in messages

    def test_on_collision_error_raises(self, spark):
        # httpStatus and http_status both normalise to http_status
        df = spark.createDataFrame([(1, 2)], ["httpStatus", "http_status"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE, on_collision="error")
        with pytest.raises(ConfigError, match="duplicate"):
            normalize_columns(df, config)

    def test_on_collision_suffix_two_way(self, spark):
        # httpStatus and http_status both normalise to http_status; second gets _2
        df = spark.createDataFrame([(1, 2)], ["httpStatus", "http_status"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE, on_collision="suffix")
        result = normalize_columns(df, config)
        assert result.columns == ["http_status", "http_status_2"]

    def test_on_collision_suffix_three_way(self, spark):
        # Three columns normalising to the same base name
        df = spark.createDataFrame([(1, 2, 3)], ["httpStatus", "http_status", "Http_Status"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE, on_collision="suffix")
        result = normalize_columns(df, config)
        assert result.columns == ["http_status", "http_status_2", "http_status_3"]

    def test_on_collision_suffix_data_preserved(self, spark):
        df = spark.createDataFrame([(10, 20)], ["httpStatus", "http_status"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE, on_collision="suffix")
        result = normalize_columns(df, config)
        row = result.collect()[0]
        assert row["http_status"] == 10
        assert row["http_status_2"] == 20

    def test_on_collision_excluded_columns_not_in_dedup(self, spark):
        # Excluded column keeps its original name even if it matches a normalised name
        df = spark.createDataFrame([(1, 2)], ["http_status", "httpStatus"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            on_collision="suffix",
            exclude=["http_status"],
        )
        result = normalize_columns(df, config)
        # http_status is excluded (identity rename), httpStatus normalises to http_status
        # They produce a collision — the non-excluded second occurrence gets _2
        assert result.columns == ["http_status", "http_status_2"]


@pytest.mark.spark
class TestNormalizeColumnsReservedWords:
    """Test reserved word protection in normalize_columns."""

    def test_prefix_strategy_default_prefix(self, spark):
        # "select" is a reserved word; default prefix is "_"
        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_select"]

    def test_prefix_strategy_custom_prefix(self, spark):
        # Custom prefix "col_" applied to reserved word
        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", prefix="col_"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["col_select"]

    def test_quote_strategy_passthrough(self, spark):
        # "quote" strategy leaves the name unchanged (Spark handles quoting)
        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="quote"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["select"]

    def test_error_strategy_raises(self, spark):
        # "error" strategy raises ConfigError listing conflicting names
        df = spark.createDataFrame([(1,)], ["from"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="error"),
        )
        with pytest.raises(ConfigError, match="from"):
            normalize_columns(df, config)

    def test_extend_adds_custom_word(self, spark):
        # "custom_word" is not a standard reserved word but added via extend
        df = spark.createDataFrame([(1,)], ["custom_word"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", extend=["custom_word"]),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_custom_word"]

    def test_exclude_removes_reserved_word(self, spark):
        # "to" is excluded from reserved word check — passes through without prefix
        df = spark.createDataFrame([(1,)], ["to"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.NONE,
            reserved_words=ReservedWordConfig(strategy="prefix", exclude=["to"]),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["to"]

    def test_non_reserved_word_unaffected(self, spark):
        # "amount" is not a reserved word; no transformation
        df = spark.createDataFrame([(1,)], ["amount"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["amount"]

    def test_no_reserved_words_config_no_change(self, spark):
        # reserved_words=None means no protection applied
        df = spark.createDataFrame([(1,)], ["select"])
        config = NamingConfig(columns=NamingPattern.SNAKE_CASE)
        result = normalize_columns(df, config)
        assert result.columns == ["select"]

    def test_error_strategy_collects_multiple(self, spark):
        # Multiple reserved words trigger a single error listing all conflicts
        df = spark.createDataFrame([(1, 2)], ["select", "from"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="error"),
        )
        with pytest.raises(ConfigError) as exc_info:
            normalize_columns(df, config)
        message = str(exc_info.value)
        assert "select" in message
        assert "from" in message

    def test_suffix_strategy_default(self, spark):
        """Suffix strategy appends default '_col' to colliding names."""
        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="suffix"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["select_col"]

    def test_suffix_strategy_custom(self, spark):
        """Suffix strategy appends custom suffix to colliding names."""
        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="suffix", suffix="_column"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["select_column"]

    def test_suffix_non_colliding_unchanged(self, spark):
        """Suffix strategy leaves non-colliding names unchanged."""
        df = spark.createDataFrame([(1, 2)], ["amount", "select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="suffix"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["amount", "select_col"]

    def test_revert_strategy(self, spark):
        """Revert strategy keeps original name for colliding columns."""
        df = spark.createDataFrame([(1, 2)], ["order_date", "amount"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.NONE,
            reserved_words=ReservedWordConfig(
                strategy="revert",
                extend=["order_date"],
            ),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["order_date", "amount"]

    def test_revert_non_colliding_unchanged(self, spark):
        """Revert strategy leaves non-colliding renames intact."""
        df = spark.createDataFrame([(1, 2)], ["OrderDate", "select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="revert"),
        )
        result = normalize_columns(df, config)
        # "select" is reserved -> reverts to "select" (the original name, same here)
        # "OrderDate" normalizes to "order_date" (not reserved) -> kept
        assert result.columns == ["order_date", "select"]

    def test_drop_strategy(self, spark):
        """Drop strategy removes colliding columns from output."""
        df = spark.createDataFrame([(1, 2, 3)], ["amount", "select", "total"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="drop"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["amount", "total"]

    def test_drop_non_colliding_unchanged(self, spark):
        """Drop strategy keeps non-colliding columns."""
        df = spark.createDataFrame([(1,)], ["amount"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="drop"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["amount"]

    def test_revert_warning_logged(self, spark, caplog):
        """Revert strategy logs warning per reverted column."""
        import logging

        df = spark.createDataFrame([(1,)], ["select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="revert"),
        )
        with caplog.at_level(logging.WARNING):
            normalize_columns(df, config)
        assert any("revert" in msg.lower() for msg in caplog.messages)

    def test_drop_warning_logged(self, spark, caplog):
        """Drop strategy logs warning per dropped column."""
        import logging

        df = spark.createDataFrame([(1, 2)], ["amount", "select"])
        from weevr.model.column_set import ReservedWordConfig

        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="drop"),
        )
        with caplog.at_level(logging.WARNING):
            normalize_columns(df, config)
        assert any("drop" in msg.lower() for msg in caplog.messages)


@pytest.mark.spark
class TestNormalizeColumnsPresetIntegration:
    """Test reserved word presets with normalize_columns."""

    def test_dax_preset_catches_dax_word(self, spark):
        """DAX word is caught when using dax preset."""
        from weevr.model.column_set import ReservedWordConfig

        df = spark.createDataFrame([(1,)], ["calculate"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", preset="dax"),  # type: ignore[arg-type]
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_calculate"]

    def test_dax_preset_ignores_ansi_only_word(self, spark):
        """ANSI-only word is NOT caught when using only dax preset."""
        from weevr.model.column_set import ReservedWordConfig

        df = spark.createDataFrame([(1,)], ["select"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", preset="dax"),  # type: ignore[arg-type]
        )
        result = normalize_columns(df, config)
        # "select" is ANSI but not DAX — should pass through unprefixed
        assert result.columns == ["select"]

    def test_combined_ansi_dax_catches_both(self, spark):
        """Combined presets catch words from both sets."""
        from weevr.model.column_set import ReservedWordConfig

        df = spark.createDataFrame([(1, 2)], ["select", "calculate"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", preset=["ansi", "dax"]),  # type: ignore[arg-type]
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_select", "_calculate"]

    def test_no_preset_backwards_compat(self, spark):
        """No preset specified uses ANSI default (backwards compat)."""
        from weevr.model.column_set import ReservedWordConfig

        df = spark.createDataFrame([(1,)], ["select"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix"),
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_select"]

    def test_powerbi_preset_catches_dax_and_m(self, spark):
        """powerbi preset catches both DAX and M words."""
        from weevr.model.column_set import ReservedWordConfig

        # "calculate" is DAX, "each" is M
        df = spark.createDataFrame([(1, 2)], ["calculate", "each"])
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            reserved_words=ReservedWordConfig(strategy="prefix", preset="powerbi"),  # type: ignore[arg-type]
        )
        result = normalize_columns(df, config)
        assert result.columns == ["_calculate", "_each"]


@pytest.mark.spark
class TestNormalizeColumnsFullPipeline:
    """End-to-end test covering normalize → dedup → reserved stages in sequence."""

    def test_full_pipeline_order(self, spark):
        # Columns as if rename has already been applied upstream.
        # "http_status" and "HttpStatus" both normalise to "http_status" → dedup gives _2.
        # "select" is a reserved word → prefix strategy prepends "_".
        from weevr.model.column_set import ReservedWordConfig

        columns = ["company_code", "sales_org", "http_status", "HttpStatus", "select"]
        df = spark.createDataFrame([(1, 2, 3, 4, 5)], columns)
        config = NamingConfig(
            columns=NamingPattern.SNAKE_CASE,
            on_collision="suffix",
            reserved_words=ReservedWordConfig(strategy="prefix", prefix="_"),
        )
        result = normalize_columns(df, config)
        assert result.columns == [
            "company_code",
            "sales_org",
            "http_status",
            "http_status_2",
            "_select",
        ]
