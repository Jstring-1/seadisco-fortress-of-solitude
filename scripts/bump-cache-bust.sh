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
# is critical AND the gate has to be tight. v1 substring-matched
# "git push" anywhere in the JSON payload, which fired on commit
# messages whose HEREDOC bodies happened to mention "git push" — the
# hook then ran the bump pre-commit and stole every staged change
# into a commit titled "chore: bump cache-bust". v2 (this file)
# extracts the literal tool_input.command field from the JSON, strips
# any leading `cd … &&` prefixes, and only fires when the LEFTOVER
# command is `git push`.
#
# Can also be run manually: `scripts/bump-cache-bust.sh` from anywhere
# inside the repo will bump + commit the cache-bust string.
#
# Why no jq: this runs in Git Bash on Windows where jq isn't always
# installed. Bash's built-in regex (`=~`) is enough to pull the
# command field out of the JSON payload.
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

# Extract the literal command field from the hook JSON. The regex
# follows JSON's quoted-string grammar: a `"command":"..."` pair where
# the inner string is a sequence of escape-sequences (\\.) or any
# non-quote/non-backslash chars. BASH_REMATCH[1] captures the body.
# Manual invocations don't have INPUT, so CMD stays empty.
CMD=""
if [ -n "$INPUT" ]; then
  re='"command"[[:space:]]*:[[:space:]]*"((\\.|[^"\\])*)"'
  if [[ "$INPUT" =~ $re ]]; then
    CMD="${BASH_REMATCH[1]}"
  fi
fi

# Hook path: only fire when the actual command (after stripping any
# `cd … && ` prefixes) starts with `git push`. This deliberately
# misses chains like `git pull && git push` — those are rare enough
# that the user can rerun, and the strict gate is worth the tradeoff
# (the v1 loose match committed every push-mentioning command).
if [ -n "$CMD" ]; then
  STRIPPED="$CMD"
  # Strip leading `cd <path> && ` (or `cd <path>;`) — possibly
  # multiple times. Stops once the next token isn't `cd`.
  while [[ "$STRIPPED" =~ ^[[:space:]]*cd[[:space:]]+[^[:space:]]+[[:space:]]*(\&\&|\;)[[:space:]]*(.*)$ ]]; do
    STRIPPED="${BASH_REMATCH[2]}"
  done
  # Also strip a leading env-var assignment block (e.g. `FOO=bar git push`).
  while [[ "$STRIPPED" =~ ^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*=[^[:space:]]+[[:space:]]+(.*)$ ]]; do
    STRIPPED="${BASH_REMATCH[1]}"
  done
  case "$STRIPPED" in
    "git push"|"git push "*|"git push;"*|"git push&"*)
      ;;
    *)
      exit 0
      ;;
  esac
  # Confirm the push targets THIS repo: command mentions repo path,
  # OR the cwd is inside the repo. Otherwise some other clone's push
  # could trigger a bump in this repo.
  if [[ "$CMD" != *"$REPO_NAME"* ]]; then
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

# 1. Cache-bust query string in HTML (forces the browser to refetch
#    each /shared.js?v=… etc. on a deploy).
sed -i "s/v=[0-9]\{8\}\.[0-9]\{4\}/v=$TS/g" web/index.html web/admin.html
# 2. SITE_VERSION constant in shared.js — the small grey "build …"
#    tag under the logo. Same timestamp as the cache-bust so the
#    visible label always matches what the user just received.
if [ -f web/shared.js ]; then
  sed -i "s/build [0-9]\{8\}\.[0-9]\{4\}/build $TS/g" web/shared.js
fi

# Nothing actually changed (e.g. files had no matching pattern) —
# nothing to commit, let the push proceed.
if git diff --quiet -- web/index.html web/admin.html web/shared.js; then exit 0; fi

git add web/index.html web/admin.html web/shared.js
git commit -m "chore: bump cache-bust to $TS" >/dev/null

# JSON output → Claude Code surfaces this as a system message in the
# transcript so the user sees the bump landed before the push went out.
printf '%s\n' "{\"systemMessage\":\"Cache-bust bumped to $TS and committed before push\"}"
