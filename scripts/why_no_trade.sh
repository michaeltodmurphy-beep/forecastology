#!/usr/bin/env bash
# =============================================================================
# why_no_trade.sh — explain, per ticker per day, why a bracket did not trade.
#
# USAGE:
#   ./scripts/why_no_trade.sh                       # uses TICKER/DATE below
#   ./scripts/why_no_trade.sh KXLOWTATL-26SEP14-B74.5 2026-09-14
#   TICKER=KXLOWTBOS-26SEP14-B70.5 DATE=2026-09-14 ./scripts/why_no_trade.sh
#
#   # Whole series + day (every bracket), not one ticker:
#   ./scripts/why_no_trade.sh KXLOWTATL 2026-09-14
#
# EDIT THESE TWO LINES EACH TIME (or pass as args / env vars):
TICKER="${1:-KXLOWTATL-26SEP14-B74.5}"
DATE="${2:-2026-09-14}"
# =============================================================================

LOG_GLOB="${LOG_GLOB:-logs/run.log*}"   # includes rotated run.log.1, .2 ...
LEVEL_INCLUDES_DEBUG="${LEVEL_INCLUDES_DEBUG:-0}"  # set 1 if bot logs at DEBUG

# --- pretty ------------------------------------------------------------------
BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; RST=$'\033[0m'
hr() { printf '%s\n' "------------------------------------------------------------------"; }

if [ ! -e logs/run.log ] && ! compgen -G "$LOG_GLOB" >/dev/null; then
  echo "${RED}ERROR: no log files match '$LOG_GLOB'${RST}" >&2
  exit 1
fi

# Inputs: TICKER may be a full ticker OR just a series prefix (e.g. KXLOWTATL).
TICKER_U=$(printf '%s' "$TICKER" | tr '[:lower:]' '[:upper:]')
SERIES_U=$(printf '%s' "$TICKER_U" | cut -d- -f1)   # KXLOWTATL
echo "${BOLD}>> Diagnosing:${RST} ${YEL}${TICKER_U}${RST}  ${DIM}on ${DATE}${RST}"
echo "${DIM}   (log glob: ${LOG_GLOB}; series: ${SERIES_U})${RST}"
hr

# Grep that spans all log files (rotated too). -h => no filename prefix.
LOGS=$(compgen -G "$LOG_GLOB")
loggrep() { grep -hE "$@" $LOGS; }

# -----------------------------------------------------------------------------
# STEP 0 — Was the ticker even evaluated on that day?
# -----------------------------------------------------------------------------
# Match the timestamp date at line start, AND the ticker (or series) anywhere.
day_ticker_lines() {
  loggrep "^${DATE} .*(${TICKER_U}|${SERIES_U})"
}

total=$(day_ticker_lines | wc -l | tr -d ' ')
if [ "$total" -eq 0 ]; then
  echo "${RED}No log lines at all for ${TICKER_U} (or series ${SERIES_U}) on ${DATE}.${RST}"
  echo
  echo "Checks:"
  echo "  1) Is that date present in the log at all?"
  echo "       grep -h '^${DATE}' $LOG_GLOB | wc -l"
  echo "  2) What dates ARE present?"
  echo "       grep -hoE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' $LOG_GLOB | sort -u"
  echo "  3) Is the ticker on ANY date (wrong date-prefix in ticker?)"
  echo "       grep -hF '${TICKER_U}' $LOG_GLOB | tail -3"
  exit 0
fi
echo "${GRN}Found ${total} log lines for ${TICKER_U} / ${SERIES_U} on ${DATE}.${RST}"
hr

# -----------------------------------------------------------------------------
# STEP 1 — Did it TRADE? (the decisive positive)
# -----------------------------------------------------------------------------
filled=$(day_ticker_lines | grep -cE 'phase\.b\.entry_filled')
buying=$(day_ticker_lines | grep -cE 'phase\.b\.buying')
if [ "$filled" -gt 0 ] || [ "$buying" -gt 0 ]; then
  echo "${GRN}IT DID TRADE.${RST} phase.b.buying=${buying}, phase.b.entry_filled=${filled}"
  day_ticker_lines | grep -E 'phase\.b\.(buying|entry_filled|recovery_sized_entry|entry_failed)' | tail -10
  hr
  echo "${DIM}(Continuing to show any later blocks anyway.)${RST}"
else
  echo "${RED}NO TRADE.${RST} No phase.b.buying / entry_filled for it on ${DATE}. Reasons below, in code order."
fi
hr

# -----------------------------------------------------------------------------
# STEP 2 — Reason tally (ordered like the code path in _evaluate_watchlist)
# -----------------------------------------------------------------------------
# Events that carry ticker= and are emitted during entry evaluation.
# (below_trigger is DEBUG; it only appears if the bot runs at DEBUG level.)
REASON_RE='phase\.b\.(below_trigger|missed_entry|falling_knife_blocked|entry_blocked_am_low_forecast|entry_blocked_by_config|entry_blocked_existing_position|entry_blocked_unknown_family|spread_too_wide|recovery_cap_reached)'\
'|entry\.(blocked_low_after_2200_et|blocked_local_settle_gate|blocked_nws_temp_gate|blocked_nws_temp_gate_no_data|blocked_nws_temp_gate_error|blocked_day_min_below_bracket|blocked_forecast_dips_below_bracket|blocked_morning_forecast_dip_below_bracket|blocked_unknown_family|blocked_nws_gate_final)'\
'|hedge\.cap_blocked'\
'|gate\.blocked_below_bracket|sunrise\.(gate_blocked|am_low_blocked|temp_rising_blocked|temp_rise_latched|obs_unavailable|gate_window_closed|blocked_morning_forecast_dip_below_bracket)'

echo "${BOLD}Reason tally:${RST}"
tally=$(day_ticker_lines | grep -oE "$REASON_RE" | sort | uniq -c | sort -rn)
if [ -n "$tally" ]; then
  printf '%s\n' "$tally"
else
  echo "  ${YEL}(none — it never reached a logged entry-block gate)${RST}"
fi
hr

# -----------------------------------------------------------------------------
# STEP 3 — The single "first gate" that stopped it (earliest line)
# -----------------------------------------------------------------------------
first=$(day_ticker_lines | grep -oE "$REASON_RE" | head -1)
echo "${BOLD}First blocking gate seen:${RST} ${YEL}${first:-<none logged>}${RST}"
hr

# -----------------------------------------------------------------------------
# STEP 4 — Full detail for each reason (only the interesting payloads)
# -----------------------------------------------------------------------------
echo "${BOLD}Detail:${RST}"

echo "${DIM}-- price preconditions --${RST}"
day_ticker_lines | grep -E 'phase\.b\.(below_trigger|missed_entry)' | tail -5 \
  | sed -E 's/.*(price=[^ ]* .*)/  \1/' || true

echo "${DIM}-- below-trigger note --${RST}"
if [ "$LEVEL_INCLUDES_DEBUG" = "0" ]; then
  echo "  ${DIM}(phase.b.below_trigger is logged at DEBUG and is likely ABSENT —"
  echo "   the bot runs at INFO. If NO other gate fired, the price most likely"
  echo "   never hit the buy trigger. Enable DEBUG to confirm.)${RST}"
fi

echo "${DIM}-- config / direction toggles --${RST}"
day_ticker_lines | grep -E 'entry_blocked_by_config' | grep -oE 'reason="[^"]+"' | sort | uniq -c

echo "${DIM}-- AM-low daily-brief keyword gate (per-series; matched keywords) --${RST}"
# ticker line carries matched=[...]; per-series evaluated line carries series= + matched=[...]
day_ticker_lines | grep -E 'entry_blocked_am_low_forecast' \
  | grep -oE 'matched=\[[^]]*\]' | sort | uniq -c
loggrep "^${DATE} .*am_low_brief\.evaluated.*series=${SERIES_U}\b" \
  | grep 'blocked=True' | tail -3

echo "${DIM}-- day-min / below-bracket observed gate --${RST}"
day_ticker_lines | grep -E 'day_min_below_bracket|gate\.blocked_below_bracket' | tail -3

echo "${DIM}-- overnight 9pm-1am / morning forecast-dip gates --${RST}"
day_ticker_lines | grep -E 'blocked_forecast_dips_below_bracket|blocked_morning_forecast_dip_below_bracket' \
  | grep -oE 'projected_min_f=[^ ]*' | sort | uniq -c | tail -5

echo "${DIM}-- low-ticker ET halt / local settle gate --${RST}"
day_ticker_lines | grep -E 'blocked_low_after_2200_et|blocked_local_settle_gate' | tail -3

echo "${DIM}-- NWS temp-window gate --${RST}"
day_ticker_lines | grep -oE 'entry\.blocked_nws_temp_gate[a-z_]*' | sort | uniq -c

echo "${DIM}-- spread too wide --${RST}"
day_ticker_lines | grep -E 'phase\.b\.spread_too_wide' | grep -oE 'spread=[^ ]*' | sort | uniq -c | tail -5

echo "${DIM}-- falling knife --${RST}"
day_ticker_lines | grep -oE 'phase\.b\.falling_knife_[a-z]+' | sort | uniq -c

echo "${DIM}-- recovery cap --${RST}"
day_ticker_lines | grep -E 'recovery_cap_reached|hedge\.cap_blocked' | tail -3

hr

# -----------------------------------------------------------------------------
# STEP 5 — Held-position / Phase-C context (why it may be stuck, not entering)
# -----------------------------------------------------------------------------
echo "${BOLD}Position-management context (Phase C):${RST}"
day_ticker_lines | grep -oE 'phase\.c\.[a-z_]+|ownership\.classified' | sort | uniq -c | sort -rn | head -10
day_ticker_lines | grep -E 'ownership\.classified' | tail -2

hr
echo "${BOLD}Done.${RST} Change TICKER and DATE at the top of the script, or pass as args."
