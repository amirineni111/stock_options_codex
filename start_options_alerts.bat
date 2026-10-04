@echo off
cd /d "%~dp0"
echo Starting option alerts: scans every 15 minutes, 09:45-16:20 ET on weekdays.
echo New signals and their outcomes are pushed to the ntfy topic in .env. Close this window to stop.
".venv\Scripts\python.exe" scripts\run_alerts.py %*
pause
