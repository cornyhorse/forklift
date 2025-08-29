import json
from pathlib import Path
from forklift.engine.engine import Engine

def run():
    data_dir = Path('tests/test-files/goodcsv')
    src = data_dir / 'good_csv1.txt'
    schema = json.loads((data_dir / 'good_csv1.json').read_text())
    out_dir = Path('debug_out')
    if out_dir.exists():
        import shutil; shutil.rmtree(out_dir)
    eng = Engine(input_kind='csv', output_kind='parquet', schema=schema,
                 preprocessors=['type_coercion'], delimiter=',',
                 encoding_priority=['utf-8'], header_mode='auto')
    eng.run(str(src), str(out_dir))
    manifest_path = out_dir / '_manifest.json'
    print('Manifest:', manifest_path.read_text())
    qpath = out_dir / '_quarantine.jsonl'
    if qpath.exists():
        print('Quarantine sample:')
        for i, line in enumerate(qpath.open()):
            if i >= 5: break
            print(line.strip())
    else:
        print('No quarantine file')

if __name__ == '__main__':
    run()

