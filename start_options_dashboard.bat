@echo off
setlocal

cd /d "%~dp0"

if not exist ".venv\Scripts\streamlit.exe" (
    echo Streamlit was not found in .venv.
    echo Run setup first: .venv\Scripts\python.exe -m pip install -r requirements.txt
    pause
    exit /b 1
)

echo Starting Options Screener...
start "" "http://localhost:8501"
".venv\Scripts\streamlit.exe" run app.py --server.port 8501

endlocal
