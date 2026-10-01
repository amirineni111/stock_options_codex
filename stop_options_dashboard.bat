@echo off
setlocal

cd /d "%~dp0"

if not exist ".dashboard.port" (
    echo Options Screener does not appear to be running ^(no .dashboard.port file^).
    pause
    exit /b 0
)

set /p PORT=<".dashboard.port"
set "APP_DIR=%CD%"

echo Stopping Options Screener on port %PORT%...

rem Only stop a listener that was launched from this folder, in case the port file is stale.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$pids = Get-NetTCPConnection -LocalPort $env:PORT -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if (-not $pids) { Write-Host ('No Options Screener process is listening on port ' + $env:PORT + '.'); exit 0 }; foreach ($processId in $pids) { $cmd = [string](Get-CimInstance Win32_Process -Filter ('ProcessId=' + $processId)).CommandLine; if ($cmd.ToLower().Contains($env:APP_DIR.ToLower())) { Stop-Process -Id $processId -Force; Write-Host ('Stopped process ' + $processId) } else { Write-Host ('Process ' + $processId + ' on port ' + $env:PORT + ' is not the Options Screener; left it running.') } }"

del ".dashboard.port" 2>nul

pause
endlocal
