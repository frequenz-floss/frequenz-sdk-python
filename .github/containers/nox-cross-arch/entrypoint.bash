#!/bin/bash
set -e

echo "System details:" $(uname -a)
echo "Machine:" $(uname -m)

exec "$@"
