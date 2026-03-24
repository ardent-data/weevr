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
