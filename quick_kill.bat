@echo off
setlocal
title Hawk QuickKill
echo ==================================================
echo   Hawk coordinator - quick kill
echo ==================================================
echo.

REM 1) stop the python processes quickstart launches (monitor + dashboard),
REM    matched by script name in the command line so arg tweaks still work.
powershell -NoProfile -ExecutionPolicy Bypass -Command "$p = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'opencode_hawk|coordinator\.py|dashboard\.py' }; if ($p) { $p | ForEach-Object { Write-Host ('killing PID {0}: {1}' -f $_.ProcessId, $_.CommandLine); Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }; Write-Host ('killed {0} process(es).' -f $p.Count) } else { Write-Host 'no coordinator.py / dashboard.py python process running.' }"

REM 2) close any leftover hawk console windows by title (belt and braces)
taskkill /fi "WINDOWTITLE eq hawk-monitor" /T /F >nul 2>nul
taskkill /fi "WINDOWTITLE eq hawk-dashboard" /T /F >nul 2>nul

echo.
echo Done.
endlocal
pause
