#!/usr/bin/env bash
# Installs the NightOwl skill into your OpenClaw workspace.
set -euo pipefail
PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${OPENCLAW_SKILLS_DIR:-$HOME/.openclaw/workspace/skills}/nightowl"

mkdir -p "$DEST/scripts"
cp "$PROJECT/openclaw/skills/nightowl/SKILL.md" "$DEST/SKILL.md"
sed "s#__NIGHTOWL_HOME__#$PROJECT#" "$PROJECT/openclaw/skills/nightowl/scripts/nightowl.sh" > "$DEST/scripts/nightowl.sh"
chmod +x "$DEST/scripts/nightowl.sh"

echo "Installed skill to $DEST"
echo "Smoke test:"
"$DEST/scripts/nightowl.sh" status || echo "(status failed: is OpenSearch up and 'nightowl setup' done?)"
if command -v openclaw >/dev/null; then
  openclaw skills list 2>/dev/null | grep -i nightowl || echo "Start a new OpenClaw session (/new) so it picks up the skill."
fi
