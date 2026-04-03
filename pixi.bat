@echo off
cd /d "%~dp0"

:menu
echo.
echo  Pixi Service Manager
echo  =====================
echo  [1] start  -- build frontend and start backend
echo  [2] stop   -- stop all Pixi-managed processes
echo  [3] exit
echo.
set /p choice=Enter choice (start/stop/1/2/3): 

if /i "%choice%"=="1"     goto start
if /i "%choice%"=="start" goto start
if /i "%choice%"=="2"     goto stop
if /i "%choice%"=="stop"  goto stop
if /i "%choice%"=="3"     goto end
if /i "%choice%"=="exit"  goto end

echo  Invalid choice. Try again.
goto menu

:start
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0pixi.ps1" start
goto end

:stop
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0pixi.ps1" stop
goto end

:end
echo.
pause