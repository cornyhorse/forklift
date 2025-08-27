import runpy
import pytest

def test_main_entrypoint_runs():
    # This will run src/forklift/__main__.py as __main__
    try:
        runpy.run_module('forklift', run_name='__main__')
    except SystemExit:
        # Allow sys.exit() calls
        pass

