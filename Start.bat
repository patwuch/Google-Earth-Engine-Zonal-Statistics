@echo off
setlocal EnableDelayedExpansion

cd /d "%~dp0"

:: --- Docker check ---
docker info >nul 2>&1
if errorlevel 1 (
    echo.
    echo  Docker is not running.
    echo  Please start Docker Desktop, then try again.
    echo.
    pause
    exit /b 1
)

:: --- Find a free port ---
set APP_PORT=
for %%p in (8501 8502 8503 8504 8505) do (
    if not defined APP_PORT (
        netstat -an 2>nul | findstr /C:":%%p " | findstr "LISTENING" >nul 2>&1
        if errorlevel 1 (
            set APP_PORT=%%p
        )
    )
)

if not defined APP_PORT (
    echo No available port found ^(tried 8501-8505^). Free a port and try again.
    pause
    exit /b 1
)

echo Selected port: %APP_PORT%

:: --- Write APP_PORT to .env so docker compose reliably picks it up ---
powershell -Command "$lines = (Get-Content '.env') -notmatch '^APP_PORT='; $lines + 'APP_PORT=%APP_PORT%' | Set-Content '.env'"

:: --- Build and start ---
echo Starting GEE Batch Processor on port %APP_PORT%...
docker compose build app
docker compose up -d --force-recreate app

:: --- Wait for UI, then open browser ---
echo Waiting for the app to start...
powershell -Command "$port='%APP_PORT%'; for($i=0;$i-lt 30;$i++){try{Invoke-WebRequest -Uri \"http://localhost:$port\" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop | Out-Null; exit 0}catch{Start-Sleep 1}}; exit 1"
if errorlevel 1 (
    echo.
    echo  The app did not respond after 30 seconds.
    echo  Run: docker compose logs -f app
    echo.
    pause
    exit /b 1
)

start "" "http://localhost:%APP_PORT%"