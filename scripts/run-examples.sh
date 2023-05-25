#!/bin/bash

export URL="ws://localhost:8080"
EXAMPLES=(
    examples/books.py
    examples/readme.py
    examples/dbapi2.py
)

function die() {
    echo "ERROR: $*" 1>&2
    exit 1
}

for e in ${EXAMPLES[@]}; do
    echo -e "\nEXAMPLE: $e"
    poetry run \
        python hrana-test-server/server_v2.py \
        python $e ||
            die "failed to execute $e"
done
