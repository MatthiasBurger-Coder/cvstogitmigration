"""Console entry point for the CVS snapshot migration tool."""

import sys

from cvstogitmigration.migrator import main


if __name__ == '__main__':
    sys.exit(main())
