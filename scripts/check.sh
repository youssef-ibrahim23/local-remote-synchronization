#!/usr/bin/env bash
# check.sh
# Run analysis, formatting check, and tests across all packages.
set -euo pipefail

echo "==> Running static analysis..."
melos analyze

echo "==> Checking formatting..."
melos format

echo "==> Running tests..."
melos test

echo "==> All checks passed."
