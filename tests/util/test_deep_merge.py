from pytest import raises

from paasta_tools.util import deep_merge


def test_deep_merge_dictionaries():
    overrides = {
        "common_key": "value",
        "common_dict": {"subkey1": 1, "subkey2": 2, "subkey3": 3},
        "just_in_overrides": "value",
        "just_in_overrides_dict": {"key": "value"},
        "overwriting_key": "value",
        "overwriting_dict": {"test": "value"},
    }
    defaults = {
        "common_key": "overwritten_value",
        "common_dict": {"subkey1": "overwritten_value", "subkey4": 4, "subkey5": 5},
        "just_in_defaults": "value",
        "just_in_defaults_dict": {"key": "value"},
        "overwriting_key": {"overwritten-key", "overwritten-value"},
        "overwriting_dict": "overwritten-value",
    }
    expected = {
        "common_key": "value",
        "common_dict": {
            "subkey1": 1,
            "subkey2": 2,
            "subkey3": 3,
            "subkey4": 4,
            "subkey5": 5,
        },
        "just_in_overrides": "value",
        "just_in_overrides_dict": {"key": "value"},
        "just_in_defaults": "value",
        "just_in_defaults_dict": {"key": "value"},
        "overwriting_key": "value",
        "overwriting_dict": {"test": "value"},
    }
    assert (
        deep_merge.deep_merge_dictionaries(
            overrides, defaults, allow_duplicate_keys=True
        )
        == expected
    )


def test_deep_merge_dictionaries_no_duplicate_keys_allowed():
    # Nested dicts should be allowed
    overrides = {"nested": {"a": "override"}}
    defaults = {"nested": {"b": "default"}}
    expected = {"nested": {"a": "override", "b": "default"}}
    assert (
        deep_merge.deep_merge_dictionaries(
            overrides, defaults, allow_duplicate_keys=True
        )
        == expected
    )
    del expected

    overrides2 = {"a": "override"}
    defaults2 = {"a": "default"}

    with raises(deep_merge.DuplicateKeyError):
        deep_merge.deep_merge_dictionaries(
            overrides2, defaults2, allow_duplicate_keys=False
        )

    overrides = {"nested": {"a": "override"}}
    defaults = {"nested": {"a": "default"}}

    with raises(deep_merge.DuplicateKeyError):
        deep_merge.deep_merge_dictionaries(
            overrides, defaults, allow_duplicate_keys=False
        )
