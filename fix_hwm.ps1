$ErrorActionPreference = 'Stop'
$encoding = [System.Text.UTF8Encoding]::new($false)

$p = 'app/config.py'
$c = [System.IO.File]::ReadAllText((Resolve-Path $p))

# 1. Remove the hard-coded default from the field declaration
$old1 = "    hwm_exit_enabled: bool = False`n    hwm_arm_price: int = 93`n    hwm_exit_price: int = 88"
$new1 = "    hwm_exit_enabled: bool = False`n    hwm_arm_price: int = 93`n    # HWM_EXIT_PRICE comes ONLY from the .env (or environment). It has no`n    # hard-coded default; a missing value is surfaced instead of silent.`n    hwm_exit_price: int"
if ($c.Contains($old1)) {
    $c = $c.Replace($old1, $new1)
    Write-Output "field default removed"
} else {
    Write-Output "field pattern not found"
}

# 2. Remove the fallback default in from_env()
$old2 = 'hwm_exit_price = os.getenv("HWM_EXIT_PRICE", "0.88")'
$new2 = 'hwm_exit_price = os.getenv("HWM_EXIT_PRICE")'
if ($c.Contains($old2)) {
    $c = $c.Replace($old2, $new2)
    Write-Output "from_env fallback removed"
} else {
    Write-Output "from_env pattern not found"
}

[System.IO.File]::WriteAllText((Resolve-Path $p), $c, $encoding)
