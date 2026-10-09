# Forklift Documentation Index

Every documentation page of the repository, by topic. The project overview and install instructions are in the [root README](../README.md); release notes are in the [CHANGELOG](../CHANGELOG.md).

## Start here

| I want to... | Read |
|---|---|
| import a CSV, set rejected rows aside, read the result | [Usage Guide](guides/USAGE.md) |
| see what `import_csv` does with the `x-...` extensions of a schema (order of the stages, supported keys, a worked example) | [Usage Guide: Applying Schema Extensions](guides/USAGE.md#applying-schema-extensions) |
| look up a function, option or result field | [API Reference](api/API_REFERENCE.md) |
| write or fix a schema | [Schema Standards](schemas/SCHEMA_STANDARDS.md) and the per-extension pages below |
| check which parts of a schema `import_csv` applies | [Schema extensions overview](schemas/README.md) |

## Guides (`guides/`)

- [USAGE.md](guides/USAGE.md): imports, schema extensions, schema generation, DataFrame readers, validation and error handling, Excel/SQL/S3, command line
- [USER_GUIDES_OVERVIEW.md](guides/USER_GUIDES_OVERVIEW.md): contents of the folder
- [VERSION_RELEASE_PROCESS.md](guides/VERSION_RELEASE_PROCESS.md): how a release is prepared and published

## API (`api/`)

- [API_REFERENCE.md](api/API_REFERENCE.md): import and reader functions, schema generation, `ImportConfig`, `ProcessingResults`, exceptions
- [API_DOCUMENTATION_OVERVIEW.md](api/API_DOCUMENTATION_OVERVIEW.md): contents of the folder

## Schemas (`schemas/`)

- [README.md](schemas/README.md): overview of the extensions and of what `import_csv` applies
- [SCHEMA_DOCUMENTATION_OVERVIEW.md](schemas/SCHEMA_DOCUMENTATION_OVERVIEW.md): contents of the folder
- [SCHEMA_STANDARDS.md](schemas/SCHEMA_STANDARDS.md): the schema format and its extensions

One page per extension:

| Extension | Page |
|---|---|
| `x-csv` | [X_CSV_DOCUMENTATION.md](schemas/X_CSV_DOCUMENTATION.md) |
| `x-fwf` | [X_FWF_DOCUMENTATION.md](schemas/X_FWF_DOCUMENTATION.md) |
| `x-transformations` | [X_TRANSFORMATIONS_DOCUMENTATION.md](schemas/X_TRANSFORMATIONS_DOCUMENTATION.md) |
| `x-special-type` | [X_SPECIAL_TYPE_DOCUMENTATION.md](schemas/X_SPECIAL_TYPE_DOCUMENTATION.md) |
| `x-columnMapping` | [X_COLUMN_MAPPING_DOCUMENTATION.md](schemas/X_COLUMN_MAPPING_DOCUMENTATION.md) |
| `x-calculatedColumns` | [X_CALCULATED_COLUMNS_DOCUMENTATION.md](schemas/X_CALCULATED_COLUMNS_DOCUMENTATION.md) |
| `x-validation` | [X_VALIDATION_DOCUMENTATION.md](schemas/X_VALIDATION_DOCUMENTATION.md) |
| `x-dataQuality` | [X_DATA_QUALITY_DOCUMENTATION.md](schemas/X_DATA_QUALITY_DOCUMENTATION.md) |
| `x-primaryKey` | [X_PRIMARY_KEY_DOCUMENTATION.md](schemas/X_PRIMARY_KEY_DOCUMENTATION.md) |
| `x-uniqueConstraints` | [X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md](schemas/X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md) |
| `x-constraintHandling` | [X_CONSTRAINT_HANDLING_DOCUMENTATION.md](schemas/X_CONSTRAINT_HANDLING_DOCUMENTATION.md) |
| `x-rowHash` | [X_ROW_HASH_DOCUMENTATION.md](schemas/X_ROW_HASH_DOCUMENTATION.md) |
| `x-metadata-generation` | [X_METADATA_GENERATION_DOCUMENTATION.md](schemas/X_METADATA_GENERATION_DOCUMENTATION.md) |
| `x-pii` | [X_PII_DOCUMENTATION.md](schemas/X_PII_DOCUMENTATION.md) (documentation only: no masking is applied) |

## Integration (`integration/`)

- [CONSTRAINT_VALIDATION_IMPLEMENTATION.md](integration/CONSTRAINT_VALIDATION_IMPLEMENTATION.md): the constraint validator, bad rows handler and enhanced processor classes, and how `import_csv` uses the constraint validator
- [INTEGRATION_GUIDES_OVERVIEW.md](integration/INTEGRATION_GUIDES_OVERVIEW.md): contents of the folder

## AWS and S3 (`aws/`)

- [AWS_INTEGRATION_OVERVIEW.md](aws/AWS_INTEGRATION_OVERVIEW.md): contents of the folder
- [S3_TESTING.md](aws/S3_TESTING.md): how the S3 code paths are tested
- [S3_GITHUB_ACTIONS_SETUP.md](aws/S3_GITHUB_ACTIONS_SETUP.md): S3 integration tests in GitHub Actions

## Package readmes (`src/forklift/`)

| Package | Readme |
|---|---|
| `forklift` (API, CLI, readers) | [forklift.readme.md](../src/forklift/forklift.readme.md) |
| `engine` (orchestration) | [forklift.engine.readme.md](../src/forklift/engine/forklift.engine.readme.md) |
| `engine.config` | [forklift.engine.config.readme.md](../src/forklift/engine/config/forklift.engine.config.readme.md) |
| `engine.processors` (CSV processor, schema extension pipeline) | [forklift.engine.processors.readme.md](../src/forklift/engine/processors/forklift.engine.processors.readme.md) |
| `engine.importers` (Excel, SQL) | [forklift.engine.importers.readme.md](../src/forklift/engine/importers/forklift.engine.importers.readme.md) |
| `processors` (extension loaders) | [forklift.processors.readme.md](../src/forklift/processors/forklift.processors.readme.md) |
| `processors.transformations` | [forklift.processors.transformations.readme.md](../src/forklift/processors/transformations/forklift.processors.transformations.readme.md) |
| `processors.calculated_columns` | [forklift.processors.calculated_columns.readme.md](../src/forklift/processors/calculated_columns/forklift.processors.calculated_columns.readme.md) |
| `processors.data_validation` | [forklift.processors.data_validation.readme.md](../src/forklift/processors/data_validation/forklift.processors.data_validation.readme.md) |
| `processors.schema_validator` | [forklift.processors.schema_validator.readme.md](../src/forklift/processors/schema_validator/forklift.processors.schema_validator.readme.md) |
| `inputs`, `inputs.sql` | [forklift.inputs.readme.md](../src/forklift/inputs/forklift.inputs.readme.md), [forklift.inputs.sql.readme.md](../src/forklift/inputs/sql/forklift.inputs.sql.readme.md) |
| `io` | [forklift.io.readme.md](../src/forklift/io/forklift.io.readme.md) |
| `outputs` | [forklift.outputs.readme.md](../src/forklift/outputs/forklift.outputs.readme.md) |
| `metadata` | [forklift.metadata.readme.md](../src/forklift/metadata/forklift.metadata.readme.md) |
| `schema` and sub-packages | [forklift.schema.readme.md](../src/forklift/schema/forklift.schema.readme.md) |
| `utils`, `utils.transformations`, `utils.date_parser` | [forklift.utils.readme.md](../src/forklift/utils/forklift.utils.readme.md), [forklift.utils.transformations.readme.md](../src/forklift/utils/transformations/forklift.utils.transformations.readme.md), [forklift.utils.date_parser.readme.md](../src/forklift/utils/date_parser/forklift.utils.date_parser.readme.md) |

## Other

- [scripts/README.md](../scripts/README.md): developer shell scripts
- [tests/README_COVERAGE.md](../tests/README_COVERAGE.md): coverage scripts
