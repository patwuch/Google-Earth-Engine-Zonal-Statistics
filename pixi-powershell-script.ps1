# pixi-powershell-script.ps1  –  GEE Web App launcher (Pixi, no Docker)
Set-Location $PSScriptRoot
Write-Host ""
Write-Host "  GEE Web App - Pixi (no Docker)"
Write-Host "  ================================"
Write-Host ""

# --- Check pixi ---
if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
    Write-Host "  Pixi not found."
    Write-Host ""
    $answer = Read-Host "  Install Pixi now? [Y/N]"
    if ($answer -imatch '^y') {
        Write-Host "  Installing Pixi..."
        try {
            Invoke-RestMethod https://pixi.sh/install.ps1 | Invoke-Expression
        } catch {
            Write-Host "  Pixi installation failed. Please install manually and try again."
            Write-Host ""
            exit 1
        }
        # Refresh PATH so pixi is visible in this session
        $env:PATH = "$env:USERPROFILE\.pixi\bin;$env:PATH"
        if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
            Write-Host "  Pixi installed but not found in PATH."
            Write-Host "  Please open a new terminal and run this script again."
            Write-Host ""
            exit 1
        }
        Write-Host "  Pixi ready."
        Write-Host ""
    } else {
        Write-Host "  Pixi is required. Exiting."
        Write-Host ""
        exit 1
    }
}

# --- Check for conflicting Docker containers ---
if (Get-Command docker -ErrorAction SilentlyContinue) {
    docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $runningContainers = docker ps --format '{{.Names}}' 2>$null |
            Where-Object { $_ -match '^gee_(backend|frontend|frontend_dev)$' }
        if ($runningContainers) {
            Write-Host "  ERROR: Docker containers for this app are already running:"
            $runningContainers | ForEach-Object { Write-Host "    $_" }
            Write-Host "  Stop them first with: Stop-docker.bat"
            Write-Host ""
            exit 1
        }
    }
}

# --- Find a free port (8000-8003) ---
$PORT = $null
foreach ($p in 8000, 8001, 8002, 8003) {
    $inUse = (netstat -an 2>$null) -match ":$p\s+.*LISTENING"
    if (-not $inUse) {
        $PORT = $p
        break
    }
}
if (-not $PORT) {
    Write-Host "  No free port found (tried 8000-8003). Free a port and try again."
    Write-Host ""
    exit 1
}

# --- Warn if GEE key missing ---
if (-not (Test-Path "config\gee-key.json")) {
    Write-Host "  WARNING: config\gee-key.json not found."
    Write-Host "  The app will start but GEE operations will fail until a key is uploaded."
    Write-Host ""
}

Write-Host "  App port : $PORT"
Write-Host ""

# --- Build frontend ---
$frontendPath = Join-Path $PSScriptRoot "frontend"
$packageJson  = Join-Path $frontendPath "package.json"
$nodeModules  = Join-Path $frontendPath "node_modules"
$lockFile     = Join-Path $frontendPath "package-lock.json"

# Only reinstall if node_modules is missing or package.json is newer than it
$needsInstall = (-not (Test-Path $nodeModules)) -or
                ((Get-Item $packageJson).LastWriteTime -gt (Get-Item $nodeModules).LastWriteTime)

if ($needsInstall) {
    Write-Host "  Dependencies outdated or missing - reinstalling..."
    if (Test-Path $nodeModules) { Remove-Item -Recurse -Force $nodeModules }
    if (Test-Path $lockFile)    { Remove-Item -Force $lockFile }

    & pixi run npm-install-frontend
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Frontend dependency install failed."
        exit 1
    }
} else {
    Write-Host "  Frontend dependencies up to date, skipping install."
}

Write-Host "  Building frontend..."
& pixi run build-frontend
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Frontend build failed."
    exit 1
}

# --- Launch uvicorn via cmd so stdout+stderr can both be redirected to one log ---
Write-Host "  Starting backend..."
$env:GOOGLE_APPLICATION_CREDENTIALS = "config\gee-key.json"
$logPath = Join-Path $PSScriptRoot "pixi.log"
Start-Process cmd `
    -ArgumentList "/c pixi run uvicorn backend.app:app --host 0.0.0.0 --port $PORT >> `"$logPath`" 2>&1" `
    -WindowStyle Hidden

Set-Content -Path ".pixi.port" -Value $PORT

# --- Wait for app to be ready ---
Write-Host "  Waiting for app (this may take a few seconds)..."
Write-Host ""

Start-Sleep 5

$ready = $false
for ($i = 5; $i -lt 60; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:$PORT/api/gee-key" `
                    -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
            $ready = $true
            break
        }
    } catch {
        # Not ready yet — also check if the port is at least open
        $tcp = New-Object System.Net.Sockets.TcpClient
        try {
            $tcp.Connect("127.0.0.1", $PORT)
            if ($tcp.Connected) { $ready = $true; break }
        } catch {} finally { $tcp.Close() }
    }
    Write-Host -NoNewline "`r  Still waiting... ($i s)  "
    Start-Sleep 1
}
# Clear the waiting line
Write-Host -NoNewline "`r                                    `r"

if (-not $ready) {
    Write-Host ""
    Write-Host "  ERROR: App did not respond after 60 s."
    Write-Host "  Check pixi.log for details."
    Write-Host ""
    exit 1
}

Write-Host ""
Write-Host "  =========================================="
Write-Host "   GEE Web App is ready"
Write-Host "   http://localhost:$PORT"
Write-Host "  =========================================="
Write-Host ""
Start-Process "http://localhost:$PORT"
Write-Host "  Run Stop-pixi.bat when you are done."
Write-Host ""