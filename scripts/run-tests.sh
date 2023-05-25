#!/bin/bash

PY_MIN_VER=`python -c "import sys; print(sys.version_info[1])"`

OPTS=("--verbose")
if [ "$PY_MIN_VER" -lt 11 ]; then
    # tests/dbapi2 are based on cpython-3.11 test suite
    OPTS+=("--ignore" "tests/dbapi2")
fi

set -x
exec poetry run \
    python hrana-test-server/server_v2.py \
    pytest "${OPTS[@]}"
