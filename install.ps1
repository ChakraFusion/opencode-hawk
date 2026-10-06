# Hawk installer for Windows.
#   irm https://raw.githubusercontent.com/ChakraFusion/opencode-hawk/main/install.ps1 | iex
# Installs pipx if needed, installs (or upgrades) Hawk with it, then runs the guided `hawk setup`.
# Options when run as a file: -Source <path or URL>  -Yes (setup takes every default)  -SkipSetup
param(
    [string]$Source = "https://github.com/ChakraFusion/opencode-hawk/archive/refs/heads/main.zip",
    [switch]$Yes,
    [switch]$SkipSetup
)
# Native tools write notes to stderr; Windows PowerShell would turn that into a fatal error under "Stop".
# Every step checks its exit code instead.
$ErrorActionPreference = "Continue"

# pipx: an existing `pipx` command (pipx, scoop, ...) first, else Python's module (installed when missing).
$pipx = $null
if (Get-Command pipx -ErrorAction SilentlyContinue) {
    $pipx = @((Get-Command pipx).Source)
} else {
    $pyExe = $null; $pyArgs = @()
    foreach ($cand in @(@("python"), @("py", "-3"))) {
        $exe = Get-Command $cand[0] -ErrorAction SilentlyContinue
        if (-not $exe) { continue }
        $flags = @($cand | Select-Object -Skip 1)
        $ok = & $exe.Source @flags -c "import sys; print(int(sys.version_info >= (3, 10)))" 2>$null
        if ($ok -eq "1") { $pyExe = $exe.Source; $pyArgs = $flags; break }
    }
    if (-not $pyExe) {
        Write-Host "Hawk needs Python 3.10 or newer. Install it (python.org or: winget install Python.Python.3.12), then run this again." -ForegroundColor Yellow
        return
    }
    & $pyExe @pyArgs -m pipx --version 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing pipx ..."
        & $pyExe @pyArgs -m pip install --user --quiet pipx
        if ($LASTEXITCODE -ne 0) { Write-Host "Could not install pipx." -ForegroundColor Red; return }
        & $pyExe @pyArgs -m pipx ensurepath 2>&1 | Out-Null
    }
    $pipx = @($pyExe) + $pyArgs + @("-m", "pipx")
}
$pipxExe = $pipx[0]; $pipxArgs = @($pipx | Select-Object -Skip 1)

Write-Host "Installing Hawk from $Source ..."
& $pipxExe @pipxArgs install --force $Source
if ($LASTEXITCODE -ne 0) { Write-Host "Install failed (see above)." -ForegroundColor Red; return }

$bin = (& $pipxExe @pipxArgs environment --value PIPX_BIN_DIR 2>$null | Select-Object -Last 1).Trim()
$hawk = Join-Path $bin "hawk.exe"
Write-Host "Hawk installed: $hawk"
if ($SkipSetup) { return }
if ($Yes) { & $hawk setup --yes } else { & $hawk setup }
Write-Host "Open a new terminal to use the hawk command ($bin is on your PATH)."
