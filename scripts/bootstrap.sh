#!/usr/bin/env bash
# bootstrap.sh
# Run melos bootstrap to link all packages.
set -euo pipefail

echo "==> Activating Melos..."
dart pub global activate melos

echo "==> Running melos bootstrap..."
melos bootstrap

echo "==> Done. All packages linked."
