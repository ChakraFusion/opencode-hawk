@echo off
setlocal
cd /d "%~dp0"

title Hawk QuickStart
echo ==================================================
echo   Hawk coordinator - quickstart
echo ==================================================
echo.

REM --- pick a Python interpreter (python, else the py launcher) ---
set "PY=python"
where python >nul 2>nul || set "PY=py"
%PY% --version >nul 2>nul
if errorlevel 1 (
    echo [error] Python not found on PATH. Install Python 3 and re-run.
    pause
    exit /b 1
)
echo Using:
%PY% --version
echo.

REM --- first run: create config.json from the template ---
if not exist config.json (
    %PY% -c "import psutil" >nul 2>nul || %PY% -m pip install -r requirements.txt
    %PY% -m opencode_hawk init
    echo         Edit config.json ^(at least project_dir^), then re-run.
    pause
    exit /b 0
)

REM --- dependency check ---
%PY% -c "import psutil" >nul 2>nul || %PY% -m pip install -r requirements.txt

REM --- 1) monitor: the hawk poll loop (own console window) ---
echo [1/2] monitor   : %PY% -m opencode_hawk monitor --interval 3
start "hawk-monitor" "%PY%" -m opencode_hawk monitor --interval 3

REM --- 2) dashboard: web UI, auto-opens browser (own console window) ---
echo [2/2] dashboard : %PY% -m opencode_hawk dashboard --port 8765 --open
start "hawk-dashboard" "%PY%" -m opencode_hawk dashboard --port 8765 --open

echo.
echo Both processes launched in their own console windows:
echo   - hawk-monitor   : the coordinator poll loop
echo   - hawk-dashboard : http://127.0.0.1:8765  (browser opens automatically)
echo.
echo This window is just the launcher - you may close it now.
echo The two hawk windows keep running until you close them.
endlocal
pause
