#!/bin/bash

set -eo pipefail

# This virtualenv is created by the Dockerfile
source /app/venv/bin/activate

exec "$@"
