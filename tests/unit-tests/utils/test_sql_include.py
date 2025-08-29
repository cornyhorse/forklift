import pytest

from forklift.utils.sql_include import derive_sql_include_patterns


def test_none_schema_returns_fallback():
    assert derive_sql_include_patterns(None) == ["*.*"]


def test_empty_schema_returns_fallback():
    assert derive_sql_include_patterns({}) == ["*.*"]


def test_root_include_only_with_duplicates_and_order():
    schema = {"include": ["public.users", "sales.orders", "public.users"]}
    assert derive_sql_include_patterns(schema) == ["public.users", "sales.orders"]


def test_xsql_include_and_root_merge_and_dedup():
    schema = {
        "include": ["a.b", "c.d"],
        "x-sql": {"include": ["c.d", "e.f"]},
    }
    assert derive_sql_include_patterns(schema) == ["a.b", "c.d", "e.f"]


def test_tables_with_pattern_schema_name_and_name_only():
    schema = {
        "x-sql": {
            "tables": [
                {"select": {"pattern": "sch1.*"}},
                {"select": {"schema": "sch2", "name": "t2"}},
                {"select": {"name": "t3"}},
            ]
        }
    }
    assert derive_sql_include_patterns(schema) == ["sch1.*", "sch2.t2", "t3"]


def test_full_merge_all_sources_with_duplicates_and_order():
    schema = {
        "include": ["p1.t1", "p2.t2"],
        "x-sql": {
            "include": ["p2.t2", "p3.t3"],
            "tables": [
                {"select": {"pattern": "schX.*"}},
                {"select": {"schema": "p1", "name": "t1"}},  # duplicate of first root entry
                {"select": {"name": "loose_table"}},
                {"select": {"schema": "schY", "name": "tblY"}},
            ],
        },
    }
    assert derive_sql_include_patterns(schema) == [
        "p1.t1",  # first occurrence retained
        "p2.t2",
        "p3.t3",
        "schX.*",
        "loose_table",
        "schY.tblY",
    ]


def test_non_list_includes_are_ignored_but_tables_used():
    schema = {
        "include": "not-a-list",  # ignored
        "x-sql": {
            "include": "also-not-a-list",  # ignored
            "tables": [
                {"select": {"schema": "s1", "name": "t1"}},
                {"select": {"name": "t2"}},
            ],
        },
    }
    assert derive_sql_include_patterns(schema) == ["s1.t1", "t2"]


def test_no_patterns_anywhere_after_ignores_fallback():
    schema = {"include": "not-a-list", "x-sql": {"include": "still-no"}}
    assert derive_sql_include_patterns(schema) == ["*.*"]


def test_tables_key_none_or_empty():
    schema_none = {"x-sql": {"tables": None}}
    schema_empty = {"x-sql": {"tables": []}}
    assert derive_sql_include_patterns(schema_none) == ["*.*"]
    assert derive_sql_include_patterns(schema_empty) == ["*.*"]

