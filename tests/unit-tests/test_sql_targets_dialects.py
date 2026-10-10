"""What ``forklift.outputs.sql`` does differently on PostgreSQL, MySQL, SQL Server and Oracle.

These tests check the SQL text, type mapping and catalog parsing of each dialect; the service
tests in tests/integration-tests/services/test_sql_targets.py run the same SQL on real servers.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.outputs.sql.columns import SourceColumn
from forklift.outputs.sql.dialects import (
    MySql,
    Oracle,
    PostgreSQL,
    SqlServer,
    _truthy,
    dialect_for,
)
from forklift.outputs.sql.errors import TableWriteError

PG, MY, MS, ORA = PostgreSQL(), MySql(), SqlServer(), Oracle()
NUMBERS = {"integer", "decimal", "float"}
TEMPORAL = {"date", "timestamp", "timestamp_tz"}
NUMERIC_BOOLEANS = {"integer", "boolean"}
DIALECTS = [PG, MY, MS, ORA]


def _column(arrow_type, kind=None, name="c", key=False, nullable=True, **kwargs):
    from forklift.outputs.sql.columns import column_kind

    column = SourceColumn(name, kind or column_kind(arrow_type), arrow_type, nullable, key)
    column.sql_name = name
    for attribute, value in kwargs.items():
        setattr(column, attribute, value)
    return column


@pytest.mark.parametrize(
    "name, dbms, label",
    [
        ("postgresql", "PostgreSQL", "PostgreSQL"),
        ("mysql", "MySQL", "MySQL"),
        ("mysql", " MariaDB ", "MariaDB"),
        ("sqlserver", "Microsoft SQL Server", "SQL Server"),
        ("oracle", "Oracle", "Oracle"),
    ],
)
def test_dialect_for_a_dbms_name(name, dbms, label):
    dialect = dialect_for(dbms)
    assert (dialect.name, dialect.label) == (name, label)


@pytest.mark.parametrize("dbms", ["SQLite", "", None])
def test_other_databases_have_no_dialect(dbms):
    assert dialect_for(dbms) is None


@pytest.mark.parametrize(
    "value, truth", [(True, True), (0, False), ("1", True), (" t ", True), ("NO", False)]
)
def test_catalog_flags(value, truth):
    assert _truthy(value) is truth


class TestIdentifiers:
    @pytest.mark.parametrize(
        "dialect, quoted",
        [
            (PG, '"a""b"."t"'),
            (MY, '`a"b`.`t`'),
            (MS, '[a"b].[t]'),
            (ORA, '"a""b"."t"'),
        ],
    )
    def test_names_are_quoted_with_the_quote_character_doubled(self, dialect, quoted):
        assert dialect.qualified('a"b', "t") == quoted

    def test_closing_quotes_are_doubled(self):
        assert (MY.quote("a`b"), MS.quote("a]b")) == ("`a``b`", "[a]]b]")

    def test_oracle_upper_cases_plain_lower_case_names_only(self):
        assert [ORA.new_name(n) for n in ["orders", "order_2$#", "Orders", "my table", "2x"]] == [
            "ORDERS",
            "ORDER_2$#",
            "Orders",
            "my table",
            "2x",
        ]
        assert PG.new_name("orders") == "orders"

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize(
        "name, reason",
        [
            ("", "non-empty string"),
            (None, "non-empty string"),
            ("a\x00b", "control characters"),
            ("a\nb", "control characters"),
            (" a", "starts or ends with spaces"),
            ("a ", "starts or ends with spaces"),
        ],
    )
    def test_names_that_cannot_be_identifiers_are_refused(self, dialect, name, reason):
        with pytest.raises(TableWriteError, match=reason) as raised:
            dialect.check_identifier(name, "table")
        assert raised.value.error_code == "SPEC_INVALID"

    @pytest.mark.parametrize(
        "dialect, longest, unit",
        [(PG, "é" * 31 + "x", "bytes"), (MY, "é" * 64, "characters"), (MS, "x" * 128, "chars")],
    )
    def test_names_up_to_the_limit_are_accepted(self, dialect, longest, unit):
        assert dialect.check_identifier(longest, "column") == longest
        with pytest.raises(TableWriteError, match=f"{dialect.label} allows at most"):
            dialect.check_identifier(longest + "x", "column")

    def test_oracle_counts_bytes_and_refuses_double_quotes(self):
        assert ORA.check_identifier("é" * 64, "table")
        with pytest.raises(TableWriteError, match="129 given"):
            ORA.check_identifier("é" * 64 + "x", "table")
        with pytest.raises(TableWriteError, match="cannot contain double quotes"):
            ORA.check_identifier('a"b', "table")

    def test_names_with_spaces_and_symbols_are_fine(self):
        assert PG.check_identifier('Total $ "net"', "column") == 'Total $ "net"'


# (Arrow type, key column) -> PostgreSQL, MySQL, SQL Server, Oracle
TYPES = [
    (pa.bool_(), False, "BOOLEAN", "BOOLEAN", "BIT", "NUMBER(1)"),
    (pa.int8(), False, "SMALLINT", "TINYINT", "SMALLINT", "NUMBER(3)"),
    (pa.int16(), False, "SMALLINT", "SMALLINT", "SMALLINT", "NUMBER(5)"),
    (pa.int32(), False, "INTEGER", "INT", "INT", "NUMBER(10)"),
    (pa.int64(), False, "BIGINT", "BIGINT", "BIGINT", "NUMBER(19)"),
    (pa.uint8(), False, "SMALLINT", "TINYINT UNSIGNED", "TINYINT", "NUMBER(3)"),
    (pa.uint16(), False, "INTEGER", "SMALLINT UNSIGNED", "INT", "NUMBER(5)"),
    (pa.uint32(), False, "BIGINT", "INT UNSIGNED", "BIGINT", "NUMBER(10)"),
    (pa.uint64(), False, "NUMERIC(20)", "BIGINT UNSIGNED", "DECIMAL(20, 0)", "NUMBER(20)"),
    (pa.float16(), False, "REAL", "FLOAT", "REAL", "BINARY_FLOAT"),
    (pa.float32(), False, "REAL", "FLOAT", "REAL", "BINARY_FLOAT"),
    (pa.float64(), False, "DOUBLE PRECISION", "DOUBLE", "FLOAT", "BINARY_DOUBLE"),
    (
        pa.decimal128(12, 3),
        False,
        "NUMERIC(12, 3)",
        "DECIMAL(12, 3)",
        "DECIMAL(12, 3)",
        "NUMBER(12, 3)",
    ),
    (pa.string(), False, "TEXT", "LONGTEXT", "NVARCHAR(MAX)", "VARCHAR2(4000 CHAR)"),
    (pa.string(), True, "TEXT", "VARCHAR(255)", "NVARCHAR(255)", "VARCHAR2(255 CHAR)"),
    (pa.binary(), False, "BYTEA", "LONGBLOB", "VARBINARY(MAX)", "BLOB"),
    (pa.binary(), True, "BYTEA", "VARBINARY(255)", "VARBINARY(255)", "RAW(255)"),
    (pa.binary(16), False, "BYTEA", "BINARY(16)", "BINARY(16)", "RAW(16)"),
    (pa.binary(9000), False, "BYTEA", "LONGBLOB", "VARBINARY(MAX)", "BLOB"),
    (pa.date32(), False, "DATE", "DATE", "DATE", "DATE"),
    (pa.timestamp("ns"), False, "TIMESTAMP(6)", "DATETIME(6)", "DATETIME2(6)", "TIMESTAMP(6)"),
    (
        pa.timestamp("s", tz="UTC"),
        False,
        "TIMESTAMP(6) WITH TIME ZONE",
        "DATETIME(6)",
        "DATETIMEOFFSET(6)",
        "TIMESTAMP(6) WITH TIME ZONE",
    ),
    (pa.time64("us"), False, "TIME(6)", "TIME(6)", "TIME(6)", "INTERVAL DAY(0) TO SECOND(6)"),
]


@pytest.mark.parametrize("arrow_type, key, postgres, mysql, sql_server, oracle", TYPES)
def test_column_types(arrow_type, key, postgres, mysql, sql_server, oracle):
    column = _column(arrow_type, key=key)
    assert [dialect.column_type(column) for dialect in DIALECTS] == [
        postgres,
        mysql,
        sql_server,
        oracle,
    ]


class TestDecimalLimits:
    def test_postgres_takes_up_to_1000_digits(self):
        assert PG.column_type(_column(pa.decimal256(76, 10))) == "NUMERIC(76, 10)"

    @pytest.mark.parametrize(
        "dialect, arrow_type, reason",
        [
            (MY, pa.decimal256(66, 2), "MySQL's DECIMAL \\(precision 1 to 65"),
            (MY, pa.decimal128(38, 31), "MySQL's DECIMAL \\(scale at most 30"),
            (MS, pa.decimal256(39, 2), "SQL Server's DECIMAL \\(precision 1 to 38"),
            (ORA, pa.decimal256(39, 2), "Oracle's NUMBER"),
            (PG, pa.decimal128(5, -2), "scale 0 to the precision"),
        ],
    )
    def test_decimals_that_do_not_fit_are_refused(self, dialect, arrow_type, reason):
        with pytest.raises(TableWriteError, match=reason):
            dialect.column_type(_column(arrow_type))


class TestBind:
    @pytest.mark.parametrize("dialect", [PG, MY, MS])
    def test_times_are_cast_from_text(self, dialect):
        assert dialect.bind(_column(pa.time64("us"))) == ("CAST(? AS TIME(6))", "{}")
        assert dialect.bind(_column(pa.int8())) == ("?", "{}")

    def test_mysql_casts_timestamps_from_text(self):
        assert MY.bind(_column(pa.timestamp("us"))) == ("CAST(? AS DATETIME(6))", "{}")
        assert PG.bind(_column(pa.timestamp("us"))) == ("?", "{}")

    def test_oracle_casts_every_value_and_converts_temporal_text_on_the_server(self):
        def bind(arrow_type, **kwargs):
            column = _column(arrow_type, **kwargs)
            column.ddl = column.ddl or ORA.column_type(column)
            return ORA.bind(column)

        assert bind(pa.int64()) == ("CAST(? AS NUMBER(19))", "{}")
        assert bind(pa.string()) == ("CAST(? AS VARCHAR2(4000 CHAR))", "{}")
        assert bind(pa.string(), load_type="CLOB") == ("TO_CLOB(?)", "{}")
        assert bind(pa.binary()) == ("TO_BLOB(?)", "{}")
        assert bind(pa.timestamp("us"))[0] == "CAST(? AS VARCHAR2(26))"
        assert bind(pa.timestamp("us"))[1].format("x") == (
            "TO_TIMESTAMP(x, 'YYYY-MM-DD HH24:MI:SS.FF6')"
        )
        assert bind(pa.timestamp("us", tz="UTC"))[1].format("x") == (
            "FROM_TZ(TO_TIMESTAMP(x, 'YYYY-MM-DD HH24:MI:SS.FF6'), 'UTC')"
        )
        assert bind(pa.time64("us")) == (
            "CAST(? AS VARCHAR2(15))",
            "CASE WHEN {0} IS NOT NULL THEN TO_DSINTERVAL('0 ' || {0}) END",
        )


class TestExistingColumns:
    @pytest.mark.parametrize(
        "row, kind, accepts, required, writable, load_type",
        [
            (
                ("id", "integer", "int4", "1", "0", "", ""),
                "integer",
                {"integer"},
                True,
                True,
                None,
            ),
            (
                ("id", "bigint", "int8", True, False, "d", ""),
                "integer",
                {"integer"},
                False,
                True,
                None,
            ),
            (
                ("id", "bigint", "int8", True, False, "a", ""),
                "integer",
                {"integer"},
                False,
                False,
                None,
            ),
            (
                ("g", "integer", "int4", False, True, None, "s"),
                "integer",
                {"integer"},
                False,
                False,
                None,
            ),
            (
                ("n", "numeric(10,2)", "numeric", False, False, "", ""),
                "decimal",
                NUMBERS,
                False,
                True,
                None,
            ),
            (
                ("u", "uuid", "uuid", False, False, "", ""),
                "other",
                {"string"},
                False,
                True,
                "uuid",
            ),
            (
                ("d", "email", "text", False, False, "", ""),
                "string",
                {"string"},
                False,
                True,
                None,
            ),
        ],
    )
    def test_postgres(self, row, kind, accepts, required, writable, load_type):
        column = PG.existing_column(row)
        assert (column.kind, set(column.accepts), column.required) == (kind, accepts, required)
        assert (column.writable, column.load_type) == (writable, load_type)

    @pytest.mark.parametrize(
        "row, kind, accepts, required, writable",
        [
            (
                ("f", "tinyint", "tinyint(1)", "YES", None, ""),
                "boolean",
                NUMERIC_BOOLEANS,
                False,
                True,
            ),
            (
                ("i", "int", "int unsigned", "NO", None, "auto_increment"),
                "integer",
                NUMERIC_BOOLEANS,
                False,
                True,
            ),
            (("i", "INT", "int", "NO", None, ""), "integer", NUMERIC_BOOLEANS, True, True),
            (("i", "int", "int", "NO", "0", ""), "integer", NUMERIC_BOOLEANS, False, True),
            (("b", "bit", "bit(1)", "YES", None, ""), "boolean", NUMERIC_BOOLEANS, False, True),
            (
                ("t", "timestamp", "timestamp", "YES", None, "DEFAULT_GENERATED"),
                "timestamp_tz",
                TEMPORAL,
                False,
                True,
            ),
            (
                ("v", "int", "int", "YES", None, "VIRTUAL GENERATED"),
                "integer",
                NUMERIC_BOOLEANS,
                False,
                False,
            ),
            (
                ("s", "int", "int", "YES", None, "STORED GENERATED"),
                "integer",
                NUMERIC_BOOLEANS,
                False,
                False,
            ),
            (("j", "json", "json", "YES", None, None), "other", {"string"}, False, True),
        ],
    )
    def test_mysql(self, row, kind, accepts, required, writable):
        column = MY.existing_column(row)
        assert (column.kind, set(column.accepts), column.required) == (kind, accepts, required)
        assert column.writable is writable and column.load_type is None

    @pytest.mark.parametrize(
        "row, kind, required, writable",
        [
            (("b", "bit", 1, 0, 0, 0), "boolean", False, True),
            (("i", "INT", 0, 0, 0, 0), "integer", True, True),
            (("i", "int", 0, 0, 0, 1), "integer", False, True),
            (("i", "int", 0, 1, 0, 0), "integer", False, False),
            (("c", "int", 1, 0, 1, 0), "integer", False, False),
            (("r", "timestamp", 0, 0, 0, 0), "other", False, False),
            (("o", "datetimeoffset", 1, 0, 0, 0), "timestamp_tz", False, True),
            (("g", "uniqueidentifier", 1, 0, 0, 0), "other", False, True),
        ],
    )
    def test_sql_server(self, row, kind, required, writable):
        column = MS.existing_column(row)
        assert (column.kind, column.required, column.writable) == (kind, required, writable)

    @pytest.mark.parametrize(
        "row, kind, accepts, required, writable, load_type",
        [
            (
                ("N", "NUMBER", None, None, "Y", None, "NO", "NO"),
                "decimal",
                NUMBERS | {"boolean"},
                False,
                True,
                None,
            ),
            (
                ("N", "NUMBER", 1, 0, "N", None, "NO", "NO"),
                "integer",
                NUMERIC_BOOLEANS,
                True,
                True,
                None,
            ),
            (("N", "NUMBER", 10, 2, "N", 3, "NO", "NO"), "decimal", NUMBERS, False, True, None),
            (
                ("I", "NUMBER", 10, 0, "N", None, "NO", "YES"),
                "integer",
                NUMERIC_BOOLEANS,
                False,
                True,
                None,
            ),
            (
                ("V", "NUMBER", 10, 0, "Y", None, "YES", "NO"),
                "integer",
                NUMERIC_BOOLEANS,
                False,
                False,
                None,
            ),
            (
                ("F", "binary_double", None, None, "Y", None, "NO", "NO"),
                "float",
                NUMBERS,
                False,
                True,
                None,
            ),
            (
                ("S", "NVARCHAR2", None, None, "Y", None, "NO", "NO"),
                "string",
                {"string"},
                False,
                True,
                None,
            ),
            (
                ("C", "NCLOB", None, None, "Y", None, "NO", "NO"),
                "string",
                {"string"},
                False,
                True,
                "CLOB",
            ),
            (
                ("R", "RAW", None, None, "Y", None, "NO", "NO"),
                "binary",
                {"binary"},
                False,
                True,
                None,
            ),
            (
                ("B", "BLOB", None, None, "Y", None, "NO", "NO"),
                "binary",
                {"binary"},
                False,
                True,
                "BLOB",
            ),
            (
                ("D", "DATE", None, None, "Y", None, "NO", "NO"),
                "timestamp",
                TEMPORAL,
                False,
                True,
                None,
            ),
            (
                ("T", "TIMESTAMP(3)", None, None, "Y", None, "NO", "NO"),
                "timestamp",
                TEMPORAL,
                False,
                True,
                None,
            ),
            (
                ("Z", "TIMESTAMP(6) WITH LOCAL TIME ZONE", None, None, "Y", None, "NO", "NO"),
                "timestamp_tz",
                TEMPORAL,
                False,
                True,
                None,
            ),
            (
                ("M", "INTERVAL DAY(2) TO SECOND(6)", None, None, "Y", None, "NO", "NO"),
                "time",
                {"time"},
                False,
                True,
                None,
            ),
            (
                ("O", "BOOLEAN", None, None, "Y", None, "NO", "NO"),
                "boolean",
                {"boolean"},
                False,
                True,
                None,
            ),
            (
                ("X", "XMLTYPE", None, None, "Y", None, "NO", "NO"),
                "other",
                {"string"},
                False,
                True,
                None,
            ),
        ],
    )
    def test_oracle(self, row, kind, accepts, required, writable, load_type):
        column = ORA.existing_column(row)
        assert (column.kind, set(column.accepts), column.required) == (kind, accepts, required)
        assert (column.writable, column.load_type) == (writable, load_type)


def _columns():
    columns = [
        _column(pa.int64(), name="id", key=True, ddl="BIGINT"),
        _column(pa.string(), name="name", nullable=False, ddl="TEXT"),
        _column(pa.time64("us"), name="at", ddl="TIME(6)"),
    ]
    for column in columns:
        column.placeholder, column.expression = PG.bind(column)
    return columns


class TestStatements:
    def test_create_table_with_a_primary_key(self):
        assert PG.create_table_sql('"s"."t"', _columns(), ["id"]) == (
            'CREATE TABLE "s"."t" ("id" BIGINT NOT NULL, "name" TEXT NOT NULL, '
            '"at" TIME(6), PRIMARY KEY ("id"))'
        )

    def test_a_staging_table_for_an_existing_table_has_no_constraints(self):
        assert PG.create_table_sql("s", _columns(), [], not_null=False) == (
            'CREATE TABLE s ("id" BIGINT, "name" TEXT, "at" TIME(6))'
        )

    def test_add_primary_key_drop_and_rename(self):
        assert MY.add_primary_key_sql("`s`.`t`", ["a", "b"]) == (
            "ALTER TABLE `s`.`t` ADD PRIMARY KEY (`a`, `b`)"
        )
        assert MY.drop_table_sql("`s`.`t`") == "DROP TABLE `s`.`t`"
        assert ORA.drop_table_sql('"S"."T"') == 'DROP TABLE "S"."T" PURGE'
        assert MY.rename_table_sql("s", "a", "b") == "RENAME TABLE `s`.`a` TO `s`.`b`"
        assert ORA.rename_table_sql("S", "A", "B") == 'ALTER TABLE "S"."A" RENAME TO "B"'

    def test_multi_row_insert(self):
        assert PG.insert_rows_sql("t", ["id", "name", "at"], _columns(), 2) == (
            'INSERT INTO t ("id", "name", "at") VALUES (?, ?, CAST(? AS TIME(6))), '
            "(?, ?, CAST(? AS TIME(6)))"
        )

    def test_insert_select(self):
        assert MS.insert_select_sql("[s].[t]", ["a", "b"], "[s].[x]", ["a", "b"]) == (
            "INSERT INTO [s].[t] ([a], [b]) SELECT [a], [b] FROM [s].[x]"
        )

    def test_key_checks(self):
        assert PG.duplicate_keys_sql("s", ["a", "b"]) == (
            'SELECT COUNT(*) FROM (SELECT "a", "b" FROM s GROUP BY "a", "b" '
            "HAVING COUNT(*) > 1) d"
        )
        assert PG.null_keys_sql("s", ["a", "b"]) == (
            'SELECT COUNT(*) FROM s WHERE "a" IS NULL OR "b" IS NULL'
        )

    def test_oracle_inserts_rows_selected_from_dual(self):
        columns = [
            _column(pa.int64(), name="ID", ddl="NUMBER(19)"),
            _column(pa.time64("us"), name="AT", ddl="INTERVAL DAY(0) TO SECOND(6)"),
        ]
        for column in columns:
            column.placeholder, column.expression = ORA.bind(column)

        assert ORA.insert_rows_sql("T", ["ID", "AT"], columns, 2) == (
            'INSERT INTO T ("ID", "AT") SELECT x1, CASE WHEN x2 IS NOT NULL THEN '
            "TO_DSINTERVAL('0 ' || x2) END FROM (SELECT CAST(? AS NUMBER(19)) x1, "
            "CAST(? AS VARCHAR2(15)) x2 FROM dual UNION ALL SELECT CAST(? AS NUMBER(19)), "
            "CAST(? AS VARCHAR2(15)) FROM dual)"
        )


PAIRS = [("id", "id"), ("name", "name")]
KEYS = [("id", "id")]


class TestUpsertStatements:
    def test_postgres_updates_then_inserts_the_missing_keys(self):
        assert PG.upsert_from_staging_sql("T", "S", PAIRS, KEYS) == [
            'UPDATE T AS t SET "name" = s."name" FROM S AS s WHERE t."id" = s."id"',
            'INSERT INTO T ("id", "name") SELECT s."id", s."name" FROM S AS s '
            'WHERE NOT EXISTS (SELECT 1 FROM T AS t WHERE t."id" = s."id")',
        ]

    def test_without_other_columns_only_the_missing_keys_are_inserted(self):
        assert len(PG.upsert_from_staging_sql("T", "S", KEYS, KEYS)) == 1
        assert len(MY.upsert_from_staging_sql("T", "S", KEYS, KEYS)) == 1

    def test_mysql_updates_through_a_join(self):
        assert MY.upsert_from_staging_sql("T", "S", PAIRS, KEYS)[0] == (
            "UPDATE T AS t JOIN S AS s ON t.`id` = s.`id` SET t.`name` = s.`name`"
        )

    def test_sql_server_merges(self):
        assert MS.upsert_from_staging_sql("T", "S", PAIRS, KEYS) == [
            "MERGE INTO T WITH (HOLDLOCK) AS t USING S AS s ON t.[id] = s.[id] "
            "WHEN MATCHED THEN UPDATE SET t.[name] = s.[name] WHEN NOT MATCHED BY TARGET THEN "
            "INSERT ([id], [name]) VALUES (s.[id], s.[name]);"
        ]
        assert "WHEN MATCHED" not in MS.upsert_from_staging_sql("T", "S", KEYS, KEYS)[0]

    def test_oracle_merges(self):
        assert ORA.upsert_from_staging_sql("T", "S", PAIRS, KEYS) == [
            'MERGE INTO T t USING S s ON (t."id" = s."id") WHEN MATCHED THEN UPDATE SET '
            't."name" = s."name" WHEN NOT MATCHED THEN INSERT ("id", "name") '
            'VALUES (s."id", s."name")'
        ]
        assert "WHEN MATCHED" not in ORA.upsert_from_staging_sql("T", "S", KEYS, KEYS)[0]

    def _plain(self, dialect, *names):
        columns = [_column(pa.int64(), name=n, ddl="NUMBER(19)") for n in names]
        for column in columns:
            column.placeholder, column.expression = dialect.bind(column)
        return columns

    def test_postgres_upserts_rows_on_conflict(self):
        columns = self._plain(PG, "id", "name")
        assert PG.upsert_rows_sql("T", ["id", "name"], columns, ["id"], 1) == (
            'INSERT INTO T ("id", "name") VALUES (?, ?) ON CONFLICT ("id") '
            'DO UPDATE SET "name" = EXCLUDED."name"'
        )
        assert PG.upsert_rows_sql("T", ["id"], columns[:1], ["id"], 1).endswith(
            'ON CONFLICT ("id") DO NOTHING'
        )

    def test_mysql_upserts_rows_on_duplicate_key(self):
        columns = self._plain(MY, "id", "name")
        assert MY.upsert_rows_sql("T", ["id", "name"], columns, ["id"], 2) == (
            "INSERT INTO T (`id`, `name`) VALUES (?, ?), (?, ?) "
            "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        )
        assert MY.upsert_rows_sql("T", ["id"], columns[:1], ["id"], 1).endswith(
            "ON DUPLICATE KEY UPDATE `id` = VALUES(`id`)"
        )

    def test_sql_server_merges_rows_from_values(self):
        columns = self._plain(MS, "id", "name")
        assert MS.upsert_rows_sql("T", ["id", "name"], columns, ["id"], 1) == (
            "MERGE INTO T WITH (HOLDLOCK) AS t USING (VALUES (?, ?)) AS s ([id], [name]) "
            "ON t.[id] = s.[id] WHEN MATCHED THEN UPDATE SET t.[name] = s.[name] "
            "WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [name]) VALUES (s.[id], s.[name]);"
        )

    def test_oracle_merges_rows_selected_from_dual(self):
        columns = self._plain(ORA, "ID", "NAME")
        assert ORA.upsert_rows_sql("T", ["ID", "NAME"], columns, ["ID"], 1) == (
            'MERGE INTO T t USING (SELECT x1 "ID", x2 "NAME" FROM (SELECT CAST(? AS NUMBER(19)) '
            'x1, CAST(? AS NUMBER(19)) x2 FROM dual)) s ON (t."ID" = s."ID") WHEN MATCHED THEN '
            'UPDATE SET t."NAME" = s."NAME" WHEN NOT MATCHED THEN INSERT ("ID", "NAME") '
            'VALUES (s."ID", s."NAME")'
        )


class TestOracleLongValues:
    def _columns(self, *specs):
        columns = []
        for name, arrow_type, ddl in specs:
            column = _column(arrow_type, name=name, ddl=ddl)
            column.placeholder, column.expression = ORA.bind(column)
            columns.append(column)
        return columns

    def test_rows_with_long_lob_values_are_split_off(self):
        columns = self._columns(
            ("ID", pa.int64(), "NUMBER(19)"),
            ("BODY", pa.string(), "CLOB"),
            ("DATA", pa.binary(), "BLOB"),
        )
        rows = [
            (1, "x" * 8191, b"\x00" * 32767),
            (2, "x" * 8192, None),
            (3, None, b"\x00" * 32768),
        ]

        short, long = ORA.split_long_values(columns, rows)

        assert [row[0] for row in short] == [1]
        assert [row[0] for row in long] == [2, 3]

    def test_without_lob_columns_every_row_is_short(self):
        columns = self._columns(("ID", pa.int64(), "NUMBER(19)"), ("S", pa.string(), "VARCHAR2"))
        rows = [(1, "x" * 10000)]
        assert ORA.split_long_values(columns, rows) == (rows, [])
        assert PG.split_long_values(columns, rows) == (rows, [])

    def test_long_rows_are_inserted_one_by_one_with_lobs_bound_directly(self):
        columns = self._columns(
            ("ID", pa.int64(), "NUMBER(19)"),
            ("BODY", pa.string(), "CLOB"),
            ("AT", pa.timestamp("us"), "TIMESTAMP(6)"),
        )
        assert ORA.single_row_insert_sql("T", ["ID", "BODY", "AT"], columns) == (
            'INSERT INTO T ("ID", "BODY", "AT") VALUES (CAST(? AS NUMBER(19)), ?, '
            "TO_TIMESTAMP(CAST(? AS VARCHAR2(26)), 'YYYY-MM-DD HH24:MI:SS.FF6'))"
        )

    def test_long_rows_cannot_hold_times(self):
        columns = self._columns(
            ("BODY", pa.string(), "CLOB"), ("AT", pa.time64("us"), "INTERVAL DAY(0) TO SECOND(6)")
        )
        with pytest.raises(TableWriteError, match="INTERVAL \\(time\\) column\\(s\\) AT"):
            ORA.single_row_insert_sql("T", ["BODY", "AT"], columns)


@pytest.mark.parametrize(
    "dialect, create, drop",
    [
        (PG, "CREATE on schema s", "ownership of the table"),
        (MY, "CREATE (and DROP, to remove the staging table) on database s", "DROP on database s"),
        (MS, "CREATE TABLE in the database and ALTER on schema s", "ALTER on schema s"),
        (ORA, "CREATE TABLE (CREATE ANY TABLE", "ownership of the table (or DROP ANY TABLE)"),
    ],
)
def test_privileges_are_named_per_database(dialect, create, drop):
    assert dialect.create_privilege("s").startswith(create)
    assert dialect.drop_privilege("s").startswith(drop)


def test_rename_privileges():
    assert MY.rename_privilege("s") == "ALTER, DROP, CREATE and INSERT on database s"
    assert ORA.rename_privilege("s").startswith("ownership of the staging table")
