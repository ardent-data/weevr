"""Sibling-reference scan: false negatives must be impossible."""

from weevr.operations.pipeline._batch_safety import references_any_column


class TestReferencesAnyColumn:
    def test_plain_template_is_clean(self):
        assert references_any_column("trim({col})", ["name", "email"]) is False

    def test_bare_sibling_reference_detected(self):
        assert references_any_column("concat({col}, backup_col)", ["backup_col"]) is True

    def test_case_only_difference_detected(self):
        # Spark resolves unquoted identifiers case-insensitively
        assert references_any_column("concat({col}, Backup_Col)", ["backup_col"]) is True
        assert references_any_column("concat({col}, backup_col)", ["BACKUP_COL"]) is True

    def test_backtick_quoted_sibling_detected(self):
        assert references_any_column("concat({col}, `backup_col`)", ["backup_col"]) is True

    def test_backtick_quoted_name_with_space_detected(self):
        assert references_any_column("coalesce({col}, `order date`)", ["order date"]) is True

    def test_function_name_collision_is_conservative(self):
        # 'trim' the column collides with trim() the function — a false
        # positive by design: costs the batch, never correctness
        assert references_any_column("trim({col})", ["trim"]) is True

    def test_substring_of_identifier_not_flagged(self):
        # 'col' inside 'colour' is not a reference to column 'col'... but
        # the {col} placeholder itself tokenizes as 'col', so only NON-
        # placeholder names get substring safety
        assert references_any_column("upper(colour_code)", ["colour"]) is False
        assert references_any_column("upper(colour_code)", ["colour_code"]) is True

    def test_escaped_backtick_in_quoted_name(self):
        assert references_any_column("f(`we``ird`)", ["we`ird"]) is True
