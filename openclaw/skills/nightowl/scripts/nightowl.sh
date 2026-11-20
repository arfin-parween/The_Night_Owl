#!/usr/bin/env bash
# Wrapper so the OpenClaw agent can call the NightOwl CLI from any working directory.
# install_openclaw_skill.sh replaces __NIGHTOWL_HOME__ with the real project path.
set -uo pipefail
NIGHTOWL_HOME="__NIGHTOWL_HOME__"
export NIGHTOWL_HOME
cd "$NIGHTOWL_HOME" || exit 1
# Merge stderr into stdout (so the agent sees refusals/errors) but drop Spark/JVM chatter.
"$NIGHTOWL_HOME/.venv/bin/nightowl" "$@" 2>&1 | grep -vE \
  "^(WARNING: |Picked up |Using Spark's|Setting default log level|To adjust logging|[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (WARN|INFO)|\s*$)"
exit "${PIPESTATUS[0]}"
