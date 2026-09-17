#!/usr/bin/env bash
# =============================================================================
# today_postmortem.sh — WHOLE-DAY, ALL-CITIES view of why almost nothing traded.
#
# Complements why_no_trade.sh (which is single-ticker / single-series).
#
# READ-ONLY DIAGNOSTIC.  This script does NOT place, cancel, or modify any
# order, and does NOT read or write any trading state.  It only greps logs.
#
# USAGE:
#   ./scripts/today_postmortem.sh                    # uses DATE below
#   ./scripts/today_postmortem.sh 2026-09-17
#   DATE=2026-09-17 ./scripts/today_postmortem.sh
#
#   # Focus one LOW series but still show global context:
#   SERIES_FOCUS=KXLOWTSEA ./scripts/today_postmortem.sh 2026-09-17
# =============================================================================
DATE="${1:-${DATE:-2026-09-17}}"
SERIES_FOCUS="${SERIES_FOCUS:-}"     # optional: KXLOWTSEA, KXLOWTMIA, ...
LOG_GLOB="${LOG_GLOB:-logs/run.log*}" # includes rotated run.log.1, .2 ...

BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; RST=$'\033[0m'
hr() { printf '%s\n' "=================================================================="; }

LOGS=$(compgen -G "$LOG_GLOB")
if [ -z "$LOGS" ]; then
  echo "${RED}No log files match '$LOG_GLOB'${RST}" >&2
  exit 1
fi
loggrep() { grep -hE "$@" $LOGS; }
day()     { loggrep "^${DATE} "; }     # every line for that date

echo "${BOLD}>> Postmortem for ${YEL}${DATE}${RST}  ${DIM}(glob: ${LOG_GLOB})${RST}"
[ -n "$SERIES_FOCUS" ] && echo "${DIM}   focus series: ${SERIES_FOCUS}${RST}"

# Sanity: is that date even present?
if [ "$(day | wc -l | tr -d ' ')" -eq 0 ]; then
  echo "${RED}No log lines for ${DATE}.${RST}"
  echo "${DIM}  dates present:${RST}"
  loggrep '^[0-9]{4}-[0-9]{2}-[0-9]{2}' | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' | sort -u | tail -10
  exit 0
fi
hr

# -----------------------------------------------------------------------------
# 1 — What actually traded (the positive result)
# -----------------------------------------------------------------------------
echo "${BOLD}[1] TRADES THAT HAPPENED${RST}"
day | grep -E 'phase\.b\.(buying|entry_filled)' | tail -40
echo "${DIM}  counts:${RST}"
printf '    buying=%s  entry_filled=%s  entry_failed=%s\n' \
  "$(day | grep -cE 'phase\.b\.buying')" \
  "$(day | grep -cE 'phase\.b\.entry_filled')" \
  "$(day | grep -cE 'phase\.b\.entry_failed')"
hr

# -----------------------------------------------------------------------------
# 2 — Price-band histogram (tests the narrow-band theory)
#    Every gate=price_trigger verdict line carries the ask price.
# -----------------------------------------------------------------------------
echo "${BOLD}[2] LOW PRICE BAND (prices on gate=price_trigger lines)${RST}"
asks=$(day | grep -E 'gate=price_trigger' | grep -oE 'price=[0-9]+' | grep -oE '[0-9]+')
if [ -z "$asks" ]; then
  echo "  ${YEL}(no gate=price_trigger lines for this date)${RST}"
else
  lt74=$(printf '%s\n' $asks | awk '$1<74'        | wc -l | tr -d ' ')
  inb=$( printf '%s\n' $asks | awk '$1>=74 && $1<=96' | wc -l | tr -d ' ')
  gt96=$(printf '%s\n' $asks | awk '$1>96'        | wc -l | tr -d ' ')
  printf '    <74  (below BUY_TRIGGER_PRICE_LOW=0.74): %s\n' "$lt74"
  printf '    74-96 (TRADEABLE BAND):                 %s\n' "$inb"
  printf '    >96  (above SPREAD_MONITOR_PRICE=0.96): %s\n' "$gt96"
fi
hr

# -----------------------------------------------------------------------------
# 3 — Per-LOW-city funnel: sunrise verdict + first blocking gate
# -----------------------------------------------------------------------------
echo "${BOLD}[3] PER-LOW-CITY FUNNEL (sunrise -> first block)${RST}"
printf '  %-26s %-9s %-16s %-24s %s\n' "TICKER" "SUNRISE" "FIRST-BLOCK" "REASON" "PASS#"
found=0
for tk in $(day | grep -oE 'ticker=KXLOW[A-Z0-9.\-]+' | sed 's/ticker=//' | sort -u); do
  if [ -n "$SERIES_FOCUS" ]; then
    case "$tk" in ${SERIES_FOCUS}*) ;; *) continue;; esac
  fi
  found=1
  tl=$(day | grep -F "ticker=$tk")
  sr=$(printf '%s\n' "$tl" | grep -E 'gate=sunrise ' | grep -oE 'verdict=[A-Z]+' | tail -1 | cut -d= -f2)
  firstblock=$(printf '%s\n' "$tl" | grep -E 'verdict=BLOCKED' | head -1)
  bgate=$(printf '%s\n' "$firstblock" | grep -oE 'gate=[a-z_]+' | head -1 | cut -d= -f2)
  breason=$(printf '%s\n' "$firstblock" | grep -oE 'reason="[^"]+"' | head -1)
  npass=$(printf '%s\n' "$tl" | grep -cE 'verdict=(PASS|OPEN)')
  printf '  %-26s %-9s %-16s %-24s %s\n' \
    "$tk" "${sr:-?}" "${bgate:-none}" "${breason:-}" "$npass"
done
[ "$found" -eq 0 ] && echo "  ${YEL}(no KXLOW tickers found for this filter/date)${RST}"
hr

# -----------------------------------------------------------------------------
# 4 — Sunrise CHILD gates (the hidden sub-reason behind a parent BLOCKED)
# -----------------------------------------------------------------------------
echo "${BOLD}[4] SUNRISE CHILD-GATE TALLY${RST}"
child=$(day | grep -oE 'sunrise\.(gate_blocked|gate_window_closed|am_low_blocked|am_low_check|temp_rising_blocked|temp_rise_latched|temp_rise_reset|obs_unavailable|blocked_morning_forecast_dip_below_bracket|computed)' \
        | sort | uniq -c | sort -rn)
if [ -n "$child" ]; then printf '%s\n' "$child"; else echo "  ${YEL}(none)${RST}"; fi
hr

# -----------------------------------------------------------------------------
# 5 — AM_LOW_FORECAST keyword gate (global keyword block, per city)
# -----------------------------------------------------------------------------
echo "${BOLD}[5] AM_LOW_FORECAST KEYWORD BLOCKS${RST}"
echo "${DIM}  per-series evaluated decisions (blocked=True means the city's daily"
echo "  brief matched a configured keyword and its LOW entry was shut for the day):${RST}"
day | grep -E 'am_low_brief\.evaluated' \
    | sed -E 's/.*(series=[A-Z0-9]+).*blocked=(True|False).*matched=\[([^]]*)\].*/    \1 blocked=\2 matched=[\3]/' \
    | sort -u
echo "${DIM}  firings of the per-ticker block event:${RST}"
day | grep -E 'entry_blocked_am_low_forecast' \
    | grep -oE 'ticker=[A-Z0-9.\-]+|matched=\[[^]]*\]' | sort | uniq -c
hr

# -----------------------------------------------------------------------------
# 6 — Config toggles + recovery caps
# -----------------------------------------------------------------------------
echo "${BOLD}[6] CONFIG / CAP BLOCKS${RST}"
echo "${DIM}  entry_blocked_by_config reasons:${RST}"
day | grep -E 'entry_blocked_by_config' | grep -oE 'reason="[^"]+"' | sort | uniq -c
printf '    recovery_cap events (recovery_cap_reached|hedge.cap_blocked)=%s\n' \
  "$(day | grep -cE 'recovery_cap_reached|hedge\.cap_blocked')"
hr

echo "${BOLD}Done.${RST}  Tip: SERIES_FOCUS=KXLOWTMIA ./scripts/today_postmortem.sh ${DATE}"
