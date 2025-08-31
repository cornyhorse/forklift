import pytest
from pathlib import Path
import polars as pl
from forklift import read_csv

# Helper to write a temporary file

def write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(content, encoding='utf-8')
    return p


def test_atomic_enforce_regex_bad_rows(tmp_path):
    # Two junk lines, then header, then one data row
    p = write(tmp_path, 'regex_header.csv', 'JUNK1,x\nJUNK2,y\nHEADERX,val\na,1\n')
    df = read_csv(str(p), schema_mode='enforce', header_comment_detection_mode='regex', header_regex='^HEADER', collect_bad_rows=True)
    assert df.shape == (1, 2)
    assert df.columns == ['HEADERX', 'val']


def test_header_mode_error_with_accept(tmp_path):
    p = write(tmp_path, 'accept.csv', 'a,b\n1,2\n')
    with pytest.raises(ValueError):
        read_csv(str(p), schema_mode='accept', header_comment_detection_mode='regex', header_regex='^a')


def test_atomic_schema_mode_unknown_raises(tmp_path):
    p = write(tmp_path, 'unknown.csv', 'a,b\n1,2\n')
    with pytest.raises(ValueError):
        read_csv(str(p), schema_mode='weird')


def test_atomic_infer_path(tmp_path):
    p = write(tmp_path, 'infer.csv', 'a,b\n1,2\n')
    df = read_csv(str(p), schema_mode='infer')
    assert df.shape == (1, 2)


def test_chunk_infer_casting(tmp_path):
    # First chunk integers, second chunk introduces float .5 -> cast attempt
    content = 'a,value\nX,1\nY,2\nZ,3.5\n'
    p = write(tmp_path, 'chunk_infer.csv', content)
    df = read_csv(str(p), schema_mode='infer', processing_mode='chunk', chunk_size=2)
    assert df.shape == (3, 2)


def test_processing_mode_invalid(tmp_path):
    p = write(tmp_path, 'pmode.csv', 'a\n1\n')
    with pytest.raises(ValueError):
        read_csv(str(p), processing_mode='invalid')


def test_chunk_size_invalid(tmp_path):
    p = write(tmp_path, 'chunksize.csv', 'a\n1\n')
    with pytest.raises(ValueError):
        read_csv(str(p), processing_mode='chunk', chunk_size=0)


def test_chunk_enforce_header_mode_invalid(tmp_path):
    p = write(tmp_path, 'hdr_invalid.csv', 'a,b\n1,2\n')
    with pytest.raises(ValueError):
        read_csv(str(p), schema_mode='enforce', processing_mode='chunk', header_comment_detection_mode='bogus')


def test_chunk_enforce_header_off_maps(tmp_path):
    p = write(tmp_path, 'hdr_off.csv', 'a,b\n1,2\n')
    df = read_csv(str(p), schema_mode='enforce', processing_mode='chunk', header_comment_detection_mode='off')
    assert df.shape == (1,2)


def test_chunk_enforce_has_header_false_detection_error(tmp_path):
    p = write(tmp_path, 'hdr_false.csv', 'a,b\n1,2\n')
    with pytest.raises(ValueError):
        read_csv(str(p), schema_mode='enforce', processing_mode='chunk', has_header=False, header_comment_detection_mode='regex', header_regex='^a')


def test_provided_header_chunk_mode(tmp_path):
    p = write(tmp_path, 'provided.csv', '1,2\n3,4\n')
    schema = {
        'type': 'object',
        'properties': {},
        'x-csv': {
            'header': {'mode': 'provided', 'columns': ['col1','col2']},
            'delimiter': ',',
            'extraColumns': 'drop'
        }
    }
    df = read_csv(str(p), processing_mode='chunk', forklift_schema=schema)
    assert df.columns == ['col1','col2']
    assert df.shape == (2,2)


def test_chunk_enforce_regex_header_detection(tmp_path):
    content = '# meta\n# another\nHDR,val,other\n1,2,3\n4,5,6\n'
    p = write(tmp_path, 'regex_chunk.csv', content)
    df = read_csv(str(p), schema_mode='enforce', processing_mode='chunk', header_comment_detection_mode='regex', header_regex='^HDR')
    assert df.columns[0:2] == ['HDR','val']
    assert df.shape[0] == 2


