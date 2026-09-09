$ErrorActionPreference = 'Stop'


$p = 'app/config.py'
$text = [System.IO.File]::ReadAllText($p)

# --- Edit 1: add config comment + field after intraday_exit_entry_grace_minutes field ---
$anchor1 = "    intraday_exit_entry_grace_minutes: int = 90"
$add1 = @"

    # INTRADAY_EXIT_SPREAD=0..99  (default: 0 = disabled / unlimited)
    #   Max allowed ask-bid spread (in cents) before a scheduled intraday
    #   checkpoint exit will actually sell a held KXLOW* position.  On each
    #   exit evaluation, if the live (yes_ask - yes_bid) exceeds this value
    #   (e.g. ask 90 / bid 78 -> spread 12 > 10), the exit is deferred and
    #   re-evaluated on the next ~30 s cycle instead of selling into a thin
    #   or widened book.  0 disables the gate entirely.
    intraday_exit_spread: int = 0
"@
if (-not $text.Contains($anchor1)) { throw 'anchor1 not found' }
$text = $text.Replace($anchor1, $anchor1 + $add1)

# --- Edit 2: parse env var in from_env after the grace-minutes parser ---
$anchor2 = "        intraday_exit_entry_grace_minutes = _parse_positive_int(
            os.getenv(""INTRADAY_EXIT_ENTRY_GRACE_MINUTES""),
            ""INTRADAY_EXIT_ENTRY_GRACE_MINUTES"",
            default=90,
        )"
$add2 = @"

        intraday_exit_spread = _parse_non_negative_int(
            os.getenv("INTRADAY_EXIT_SPREAD"),
            "INTRADAY_EXIT_SPREAD",
            default=0,
        )
"@
if (-not $text.Contains($anchor2)) { throw 'anchor2 not found' }
$text = $text.Replace($anchor2, $anchor2 + $add2)

# --- Edit 3: pass field into cls(...) kwargs after grace-minutes kwarg ---
$anchor3 = "            intraday_exit_entry_grace_minutes=intraday_exit_entry_grace_minutes,"
$add3 = "            intraday_exit_spread=intraday_exit_spread,
"
if (-not $text.Contains($anchor3)) { throw 'anchor3 not found' }
$text = $text.Replace($anchor3, $anchor3 + "`n" + $add3.TrimEnd("`n"))

[System.IO.File]::WriteAllText($p, $text)
Write-Output 'config.py edits applied'
