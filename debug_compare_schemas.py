import pyarrow.parquet as pq
from pathlib import Path
import json
from forklift.engine.engine import Engine

def main():
    golden_path = Path('tests/test-files/goodcsv/good_csv1.txt.parquet')
    print('Golden exists:', golden_path.exists())
    golden = pq.read_table(golden_path)
    print('Golden schema:')
    print(golden.schema)
    for f in golden.schema:
        print('G:', f.name, f.type)

    # Produce new output
    out_dir = Path('debug_schema_out')
    if out_dir.exists():
        import shutil; shutil.rmtree(out_dir)
    schema = json.loads(Path('tests/test-files/goodcsv/good_csv1.json').read_text())
    eng = Engine(input_kind='csv', output_kind='parquet', schema=schema, preprocessors=['type_coercion'], delimiter=',', encoding_priority=['utf-8'], header_mode='auto')
    eng.run('tests/test-files/goodcsv/good_csv1.txt', str(out_dir))
    produced_path = out_dir / 'good_csv1.txt.parquet'
    print('Produced exists:', produced_path.exists())
    produced = pq.read_table(produced_path)
    print('Produced schema:')
    print(produced.schema)
    for f in produced.schema:
        print('P:', f.name, f.type)

if __name__ == '__main__':
    main()

