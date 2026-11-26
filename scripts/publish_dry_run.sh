#!/usr/bin/env bash
# publish_dry_run.sh
# Run a dry-run publish check on all packages.
set -euo pipefail

echo "==> Running publish dry-run on all packages..."
melos publish:dry

echo "==> Dry-run complete."
