#!/bin/bash
set -e

echo "System details:" $(uname -a)
echo "Machine:" $(uname -m)

pip install -e .[dev-noxfile]

exec "$@"
