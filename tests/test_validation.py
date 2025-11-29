"""Tests for validation utilities."""

import pytest

from automic_etl.core.validation import (
    ValidationError,
    validate_batch_size,
    validate_column_name,
    validate_dict_keys,
    validate_in_choices,
    validate_list_items,
    validate_non_empty_string,
    validate_positive_int,
    validate_table_name,
)


class TestTableNameValidation:
    """Test table name validation."""

    def test_valid_table_name(self):
        """Valid table names should pass."""
        validate_table_name("users")
        validate_table_name("user_data")
        validate_table_name("_temp")
        validate_table_name("Table123")

    def test_empty_table_name(self):
        """Empty table name should fail."""
        with pytest.raises(ValidationError):
            validate_table_name("")

    def test_none_table_name(self):
        """None table name should fail."""
        with pytest.raises(ValidationError):
            validate_table_name(None)  # type: ignore

    def test_invalid_start_character(self):
        """Table names starting with number should fail."""
        with pytest.raises(ValidationError):
            validate_table_name("123table")

    def test_invalid_characters(self):
        """Table names with invalid characters should fail."""
        with pytest.raises(ValidationError):
            validate_table_name("user-data")

        with pytest.raises(ValidationError):
            validate_table_name("user.data")

        with pytest.raises(ValidationError):
            validate_table_name("user data")

    def test_table_name_too_long(self):
        """Table names exceeding 255 characters should fail."""
        long_name = "a" * 256
        with pytest.raises(ValidationError):
            validate_table_name(long_name)


class TestColumnNameValidation:
    """Test column name validation."""

    def test_valid_column_name(self):
        """Valid column names should pass."""
        validate_column_name("user_id")
        validate_column_name("_id")
        validate_column_name("Column1")

    def test_invalid_column_name(self):
        """Invalid column names should fail."""
        with pytest.raises(ValidationError):
            validate_column_name("123column")

        with pytest.raises(ValidationError):
            validate_column_name("column-name")


class TestNonEmptyStringValidation:
    """Test non-empty string validation."""

    def test_valid_string(self):
        """Valid non-empty strings should pass."""
        result = validate_non_empty_string("test", "field")
        assert result == "test"

    def test_whitespace_string(self):
        """Whitespace-only strings should fail."""
        with pytest.raises(ValidationError):
            validate_non_empty_string("   ", "field")

    def test_empty_string(self):
        """Empty strings should fail."""
        with pytest.raises(ValidationError):
            validate_non_empty_string("", "field")

    def test_non_string_value(self):
        """Non-string values should fail."""
        with pytest.raises(ValidationError):
            validate_non_empty_string(123, "field")  # type: ignore


class TestPositiveIntValidation:
    """Test positive integer validation."""

    def test_valid_positive_int(self):
        """Valid positive integers should pass."""
        result = validate_positive_int(10, "count")
        assert result == 10

    def test_zero_not_allowed(self):
        """Zero should fail when not allowed."""
        with pytest.raises(ValidationError):
            validate_positive_int(0, "count", allow_zero=False)

    def test_zero_allowed(self):
        """Zero should pass when allowed."""
        result = validate_positive_int(0, "count", allow_zero=True)
        assert result == 0

    def test_negative_int(self):
        """Negative integers should fail."""
        with pytest.raises(ValidationError):
            validate_positive_int(-5, "count")

    def test_non_int_value(self):
        """Non-integer values should fail."""
        with pytest.raises(ValidationError):
            validate_positive_int("10", "count")  # type: ignore


class TestDictKeysValidation:
    """Test dictionary keys validation."""

    def test_valid_dict_with_required_keys(self):
        """Valid dicts with required keys should pass."""
        data = {"name": "test", "age": 30}
        result = validate_dict_keys(data, required_keys={"name", "age"})
        assert result == data

    def test_missing_required_keys(self):
        """Dicts missing required keys should fail."""
        data = {"name": "test"}
        with pytest.raises(ValidationError):
            validate_dict_keys(data, required_keys={"name", "age"})

    def test_allowed_keys_constraint(self):
        """Dicts with unexpected keys should fail when allowed_keys specified."""
        data = {"name": "test", "extra": "value"}
        with pytest.raises(ValidationError):
            validate_dict_keys(data, allowed_keys={"name"})

    def test_non_dict_value(self):
        """Non-dict values should fail."""
        with pytest.raises(ValidationError):
            validate_dict_keys("not a dict")  # type: ignore


class TestInChoicesValidation:
    """Test 'in choices' validation."""

    def test_valid_choice(self):
        """Valid choices should pass."""
        result = validate_in_choices("aws", {"aws", "gcp", "azure"}, "provider")
        assert result == "aws"

    def test_invalid_choice(self):
        """Invalid choices should fail."""
        with pytest.raises(ValidationError):
            validate_in_choices("invalid", {"aws", "gcp", "azure"}, "provider")

    def test_case_sensitive_validation(self):
        """Validation should be case-sensitive."""
        with pytest.raises(ValidationError):
            validate_in_choices("AWS", {"aws", "gcp", "azure"}, "provider")


class TestBatchSizeValidation:
    """Test batch size validation."""

    def test_valid_batch_size(self):
        """Valid batch sizes should pass."""
        assert validate_batch_size(100) == 100
        assert validate_batch_size(1000000) == 1000000

    def test_invalid_batch_size_zero(self):
        """Batch size of zero should fail."""
        with pytest.raises(ValidationError):
            validate_batch_size(0)

    def test_invalid_batch_size_negative(self):
        """Negative batch sizes should fail."""
        with pytest.raises(ValidationError):
            validate_batch_size(-100)

    def test_batch_size_too_large(self):
        """Batch sizes exceeding 1,000,000 should fail."""
        with pytest.raises(ValidationError):
            validate_batch_size(1_000_001)


class TestListItemsValidation:
    """Test list items validation."""

    def test_valid_list(self):
        """Valid lists should pass."""
        def validator(item):
            if not isinstance(item, str):
                raise ValidationError("must be string")

        result = validate_list_items(["a", "b", "c"], validator, "items")
        assert result == ["a", "b", "c"]

    def test_empty_list_allowed(self):
        """Empty lists should pass when allowed."""
        result = validate_list_items([], lambda x: None, "items", allow_empty=True)
        assert result == []

    def test_empty_list_not_allowed(self):
        """Empty lists should fail when not allowed."""
        with pytest.raises(ValidationError):
            validate_list_items([], lambda x: None, "items", allow_empty=False)

    def test_invalid_list_item(self):
        """Lists with invalid items should fail."""
        def validator(item):
            if not isinstance(item, int):
                raise ValidationError("must be int")

        with pytest.raises(ValidationError):
            validate_list_items([1, 2, "invalid"], validator, "items")
