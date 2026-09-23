#!/usr/bin/env bash
# Regenerates docs/screenshots from a realistic demo workload rendered by the real UI.
# Requires Google Chrome for headless capture.
set -euo pipefail

cd "$(dirname "$0")/.."
chrome="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
cargo test --quiet ui::demo_screens -- --ignored >/dev/null
for screen in overview traces llm metrics logs; do
  "$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=2 \
    --window-size=1249,732 --screenshot="docs/screenshots/$screen.png" \
    "file://$PWD/target/ui-preview/readme/$screen.html" >/dev/null 2>&1
done
echo "wrote docs/screenshots/{overview,traces,llm,metrics,logs}.png"
