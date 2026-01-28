#!/usr/bin/env bash
# Run all workflow tests. Used by CI and by `just test-github-workflows`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo "Running scenario tests (job triggering/skipping)..."
bash "${SCRIPT_DIR}/test-pull-request-scenarios.sh"
echo ""
echo "All workflow tests passed."
