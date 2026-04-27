@echo off
setlocal

echo Stopping Options Screener on port 8501...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$pids = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if (-not $pids) { Write-Host 'No Options Screener process is listening on port 8501.'; exit 0 }; foreach ($processId in $pids) { Stop-Process -Id $processId -Force; Write-Host ('Stopped process ' + $processId) }"

pause
endlocal
