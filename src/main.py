from __future__ import annotations
from pathlib import Path
from pprint import pprint

def main() -> None:

    schema = '/Users/matthewkingsbury/PycharmProjects/forklift/tests/test-files/largecsv/parquet_types.json'
    from forklift.schema.csv_schema_importer import CsvSchemaImporter
    importer = CsvSchemaImporter(schema)
    print("Schema as dict:")
    pprint(importer.as_dict())


if __name__ == "__main__":  # simple manual smoke test
    main()
