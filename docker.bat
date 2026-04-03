@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

:: ============================================================
:menu
:: ============================================================
cls
echo.
echo  GEE Web App - Docker Manager
echo  ==============================
echo.
echo   [1] Start    Build and launch all services
echo   [2] Stop     Shut down all containers
echo   [3] Logs     Stream live container logs
echo   [4] Status   Show running containers
echo   [5] Exit
echo.
set /p CHOICE=" Select an option [1-5]: "

if "%CHOICE%"=="1" goto :start
if "%CHOICE%"=="2" goto :stop
if "%CHOICE%"=="3" goto :logs
if "%CHOICE%"=="4" goto :status
if "%CHOICE%"=="5" exit /b 0

echo.
echo  Invalid choice. Please enter 1-5.
timeout /t 2 >nul
goto :menu

:: ============================================================
:start
:: ============================================================
cls
echo.
echo  GEE Web App - React + FastAPI
echo  ================================
echo.


:: --- Docker check ---
echo  [1/5] Checking Docker...
powershell -NoProfile -Command "$r = Start-Process 'docker' -ArgumentList 'info' -PassThru -NoNewWindow -RedirectStandardOutput '%TEMP%\docker_info.txt' -RedirectStandardError '%TEMP%\docker_info_err.txt'; if (-not $r.WaitForExit(5000)) { $r.Kill(); exit 1 } else { exit $r.ExitCode }"
if errorlevel 1 (
    echo.
    echo  ERROR: Docker is not running or not responding.
    echo  Please start Docker Desktop and try again.
    echo.
    pause
    goto :menu
)
echo        Docker is running.
echo.

:: --- Check for conflicting Pixi process ---
echo  [2/5] Checking for conflicts...
if exist ".pixi.pid" (
    set /p PIXI_PID=<.pixi.pid
    if defined PIXI_PID (
        powershell -NoProfile -Command "Get-Process -Id %PIXI_PID% -ErrorAction SilentlyContinue" >nul 2>&1
        if not errorlevel 1 (
            set /p PIXI_PORT=<.pixi.port
            echo.
            echo  ERROR: A Pixi-managed backend is already running.
            echo  PID: !PIXI_PID!   Port: !PIXI_PORT!
            echo  Stop it first with: Stop-pixi.bat
            echo.
            pause
            goto :menu
        ) else (
            del /f /q .pixi.pid 2>nul
            del /f /q .pixi.port 2>nul
        )
    )
)
echo        No conflicts found.
echo.

:: --- Find a free port for the backend (8000-8003) ---
echo  [3/5] Resolving ports...

set BACKEND_PORT=
for %%p in (8000 8001 8002 8003) do (
    if not defined BACKEND_PORT (
        powershell -NoProfile -Command "if (Get-NetTCPConnection -LocalPort %%p -ErrorAction SilentlyContinue) { exit 1 } else { exit 0 }"
        if not errorlevel 1 set "BACKEND_PORT=%%p"
    )
)
if not defined BACKEND_PORT (
    echo.
    echo  ERROR: No free backend port found ^(tried 8000-8003^).
    echo  Free a port and try again.
    echo.
    pause
    goto :menu
)

set FRONTEND_PORT=
for %%p in (3000 3001 3002 3003) do (
    if not defined FRONTEND_PORT (
        powershell -NoProfile -Command "if (Get-NetTCPConnection -LocalPort %%p -ErrorAction SilentlyContinue) { exit 1 } else { exit 0 }"
        if not errorlevel 1 set "FRONTEND_PORT=%%p"
    )
)
if not defined FRONTEND_PORT (
    echo.
    echo  ERROR: No free frontend port found ^(tried 3000-3003^).
    echo  Free a port and try again.
    echo.
    pause
    goto :menu
)

echo        Backend  port : %BACKEND_PORT%
echo        Frontend port : %FRONTEND_PORT%
echo.


:: --- Update .env ---
echo  Updating .env...
if exist ".env" findstr /v /r /c:"^BACKEND_PORT=" /c:"^APP_PORT=" ".env" > ".env.tmp" 2>nul
if exist ".env.tmp" move /y ".env.tmp" ".env" >nul 2>&1
echo BACKEND_PORT=%BACKEND_PORT%>> ".env"
echo APP_PORT=%FRONTEND_PORT%>> ".env"
echo        .env updated.
echo.


:: --- Build images ---
echo  [4/5] Building images...
echo.
echo        Building backend...
docker compose build backend
echo  DEBUG: Backend build errorlevel = %ERRORLEVEL%
pause
if errorlevel 1 (
    echo.
    echo  ERROR: Backend build failed. See output above.
    echo.
    pause
    goto :menu
)
echo.
echo        Building frontend...
docker compose build frontend
if errorlevel 1 (
    echo.
    echo  ERROR: Frontend build failed. See output above.
    echo.
    pause
    goto :menu
)
echo.
echo        Build complete.
echo.

:: --- Start services ---
echo  [5/5] Starting containers...
docker compose --profile prod up -d --force-recreate backend frontend
if errorlevel 1 (
    echo.
    echo  ERROR: Failed to start containers. See output above.
    echo.
    pause
    goto :menu
)
echo.

:: --- Wait for backend ---
echo  Waiting for backend  ^(http://localhost:%BACKEND_PORT%^)...
powershell -NoProfile -Command ^
    "$port='%BACKEND_PORT%';" ^
    "for($i=0;$i-lt 40;$i++){" ^
    "  try {" ^
    "    $r=(Invoke-WebRequest -Uri \"http://localhost:$port/api/gee-key\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode;" ^
    "    if($r -eq 200){ Write-Host '       Backend ready.'; exit 0 }" ^
    "  } catch {};" ^
    "  Write-Host -NoNewLine '.';" ^
    "  Start-Sleep 1" ^
    "};" ^
    "Write-Host '';" ^
    "Write-Host '       WARNING: Backend timed out after 40s.';" ^
    "exit 1"

:: --- Wait for frontend ---
echo  Waiting for frontend ^(http://localhost:%FRONTEND_PORT%^)...
powershell -NoProfile -Command ^
    "$port='%FRONTEND_PORT%';" ^
    "for($i=0;$i-lt 60;$i++){" ^
    "  try {" ^
    "    $r=(Invoke-WebRequest -Uri \"http://localhost:$port/\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode;" ^
    "    if($r -lt 500){ Write-Host '       Frontend ready.'; exit 0 }" ^
    "  } catch {};" ^
    "  Write-Host -NoNewLine '.';" ^
    "  Start-Sleep 1" ^
    "};" ^
    "Write-Host '';" ^
    "Write-Host '       WARNING: Frontend timed out after 60s.';" ^
    "exit 1"
if errorlevel 1 (
    echo.
    echo  WARNING: Frontend did not respond. Check logs:
    echo  docker compose logs -f frontend
    echo.
    pause
    goto :menu
)

echo.
echo  ==========================================
echo   GEE Web App is ready^^!
echo   Frontend : http://localhost:%FRONTEND_PORT%
echo   Backend  : http://localhost:%BACKEND_PORT%
echo  ==========================================
echo.
start "" "http://localhost:%FRONTEND_PORT%"
echo  Press any key to return to the menu...
pause >nul
goto :menu

:: ============================================================
:stop
:: ============================================================
cls
echo.
echo  Stopping GEE Web App...
echo.
docker compose --profile prod down
if errorlevel 1 (
    echo  WARNING: Some containers may still be running.
    echo  Check with: docker compose --profile prod ps
) else (
    echo  All services stopped successfully.
)
echo.
pause
goto :menu

:: ============================================================
:logs
:: ============================================================
cls
echo.
echo  Streaming logs ^(press Ctrl+C to stop^)...
echo.
set /p SVC=" Stream which service? [backend / frontend / all]: "
echo.
if /i "%SVC%"=="backend"  docker compose logs -f backend
if /i "%SVC%"=="frontend" docker compose logs -f frontend
if /i "%SVC%"=="all"      docker compose logs -f
echo.
pause
goto :menu

:: ============================================================
:status
:: ============================================================
cls
echo.
echo  Running containers:
echo.
docker compose --profile prod ps
echo.
pause
goto :menu