#!/usr/bin/env bash
# Cache-bust bumper — invoked by the Claude Code PreToolUse Bash hook
# in ~/.claude/settings.json. Reads the hook input JSON from stdin; if
# the matched Bash command is a `git push` targeting THIS repo, bumps
# the ?v=YYYYMMDD.HHMM strings in web/index.html and web/admin.html
# and commits the bump so the bumped version actually ships with the
# push the user just initiated.
#
# Bails silently for anything that isn't a `git push` against this
# repo — every Bash tool call goes through the hook, so the fast path
# is critical.
#
# Can also be run manually: `scripts/bump-cache-bust.sh` from anywhere
# inside the repo will bump + commit the cache-bust string.
#
# Why no jq: this runs in Git Bash on Windows where jq isn't always
# installed. We just substring-match the raw JSON payload — that's
# sufficient because we're only checking for the literal "git push"
# command and the repo path, both of which appear verbatim.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
REPO_NAME="$(basename "$REPO_DIR")"

# Slurp stdin (hook input JSON) if present. Manual runs have no stdin
# so INPUT stays empty and we fall through to the cwd-based gate.
INPUT=""
if [ ! -t 0 ]; then
  INPUT="$(cat || true)"
fi

# Hook path: gate on the literal "git push" substring AND a reference
# to this repo. Both must be present in the JSON command field.
# Manual path: INPUT is empty, so we rely on PWD being inside the repo.
if [ -n "$INPUT" ]; then
  case "$INPUT" in
    *"git push"*) ;;
    *) exit 0 ;;
  esac
  # Confirm the push targets THIS repo (path mentioned in command, or
  # cwd inside it). Otherwise some other repo's push would trigger us.
  if [[ "$INPUT" != *"$REPO_NAME"* ]]; then
    case "$PWD" in
      "$REPO_DIR"|"$REPO_DIR"/*) ;;
      *) exit 0 ;;
    esac
  fi
else
  # Manual invocation — only proceed if we're inside the repo.
  case "$PWD" in
    "$REPO_DIR"|"$REPO_DIR"/*) ;;
    *) exit 0 ;;
  esac
fi

cd "$REPO_DIR"
[ -f web/index.html ] && [ -f web/admin.html ] || exit 0

TS="$(date +%Y%m%d.%H%M)"
CUR="$(grep -oE 'v=[0-9]{8}\.[0-9]{4}' web/index.html | head -1 | cut -d= -f2 || true)"
# Already at this timestamp — skip (prevents an empty commit when two
# pushes land in the same minute).
if [ "$CUR" = "$TS" ]; then exit 0; fi

sed -i "s/v=[0-9]\{8\}\.[0-9]\{4\}/v=$TS/g" web/index.html web/admin.html

# Nothing actually changed (e.g. files had no matching pattern) —
# nothing to commit, let the push proceed.
if git diff --quiet -- web/index.html web/admin.html; then exit 0; fi

git add web/index.html web/admin.html
git commit -m "chore: bump cache-bust to $TS" >/dev/null

# JSON output → Claude Code surfaces this as a system message in the
# transcript so the user sees the bump landed before the push went out.
printf '%s\n' "{\"systemMessage\":\"Cache-bust bumped to $TS and committed before push\"}"
