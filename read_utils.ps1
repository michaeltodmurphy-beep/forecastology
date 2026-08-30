param([int]$Start=1,[int]$Count=50,[string]$File='core/state_machine.py')
$lines = Get-Content $File
$end = $Start + $Count - 1
for ($i = $Start; $i -le [Math]::Min($end, $lines.Count); $i++) {
    '{0,5}: {1}' -f $i, $lines[$i-1]
}
