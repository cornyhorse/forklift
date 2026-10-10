"""``python -m forklift_web <command>``: the same as ``forklift-web <command>``."""

import sys

from forklift_web.manage import main

if __name__ == "__main__":
    main(sys.argv)
