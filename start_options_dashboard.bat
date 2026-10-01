@echo off
setlocal

cd /d "%~dp0"

if not exist ".venv\Scripts\streamlit.exe" (
    echo Streamlit was not found in .venv.
    echo Run setup first: .venv\Scripts\python.exe -m pip install -r requirements.txt
    pause
    exit /b 1
)

rem Use 8501 if it is free, otherwise the next free port above it.
set "PORT=8501"
for /f %%p in ('powershell -NoProfile -Command "$p = 8501; while (Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue) { $p++ }; $p"') do set "PORT=%%p"

rem stop_options_dashboard.bat reads this to know which port to stop.
> ".dashboard.port" echo %PORT%

echo Starting Options Screener on port %PORT%...
start "" "http://localhost:%PORT%"
".venv\Scripts\streamlit.exe" run app.py --server.port %PORT%

del ".dashboard.port" 2>nul
endlocal
