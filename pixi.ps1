param(
    [Parameter(Position=0)]
    [string]$Command = ""
)

Set-Location $PSScriptRoot

# ── Functions ──────────────────────────────────────────────────────────────────

function Stop-App {
    Write-Host ""
    Write-Host "  Stopping GEE Web App (Pixi)..."
    Write-Host ""

    $PORT = 8000
    if (Test-Path ".pixi.port") {
        $PORT = Get-Content ".pixi.port"
    }

    if (Test-Path ".pixi.pid") {
        $savedPid = Get-Content ".pixi.pid" -ErrorAction SilentlyContinue
        if ($savedPid) {
            $p = Get-Process -Id $savedPid -ErrorAction SilentlyContinue
            if ($p) {
                Write-Host "  Stopping PID $savedPid..."
                try { $p.Kill($true) } catch {}
            }
        }
    }

    $listeners = netstat -aon 2>$null | Select-String ":${PORT}\s+.*LISTENING"
    foreach ($line in $listeners) {
        $linePid = ($line -split '\s+')[-1]
        Write-Host "  Killing PID $linePid on port $PORT..."
        taskkill /pid $linePid /f /t 2>$null | Out-Null
    }

    taskkill /im uvicorn.exe /f /t 2>$null | Out-Null
    taskkill /im pixi.exe    /f /t 2>$null | Out-Null

    Remove-Item -Force ".pixi.port" -ErrorAction SilentlyContinue
    Remove-Item -Force ".pixi.pid"  -ErrorAction SilentlyContinue

    Write-Host "  Done."
    Write-Host ""
}

function Start-App {
    Write-Host ""
    Write-Host "  GEE Web App - Pixi (no Docker)"
    Write-Host "  ================================"
    Write-Host ""

    # --- Clean up any previous run ---
    if (Test-Path ".pixi.pid") {
        $oldPid = Get-Content ".pixi.pid" -ErrorAction SilentlyContinue
        if ($oldPid) {
            $oldProc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
            if ($oldProc) {
                Write-Host "  Found leftover process (PID $oldPid), stopping it..."
                try { $oldProc.Kill($true) } catch {}
            }
        }
        Remove-Item -Force ".pixi.pid" -ErrorAction SilentlyContinue
    }
    Remove-Item -Force ".pixi.port" -ErrorAction SilentlyContinue

    # --- Rotate log ---
    if (Test-Path "pixi.log") {
        Move-Item -Force "pixi.log" "pixi.last.log" -ErrorAction SilentlyContinue
    }

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
                Write-Host "  Pixi installation failed. Please install manually."
                exit 1
            }
            $env:PATH = "$env:USERPROFILE\.pixi\bin;$env:PATH"
            if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
                Write-Host "  Pixi installed but not found in PATH."
                Write-Host "  Please open a new terminal and run again."
                exit 1
            }
            Write-Host "  Pixi ready."
            Write-Host ""
        } else {
            Write-Host "  Pixi is required. Exiting."
            exit 1
        }
    }

    # --- Check for conflicting Docker containers ---
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        docker info 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $running = docker ps --format '{{.Names}}' 2>$null |
                Where-Object { $_ -match '^gee_' }
            if ($running) {
                Write-Host "  ERROR: Docker containers are already running:"
                $running | ForEach-Object { Write-Host "    $_" }
                Write-Host "  Stop them first with: docker.bat stop"
                Write-Host ""
                exit 1
            }
        }
    }

    # --- Find a free port ---
    $PORT = $null
    foreach ($p in 8000, 8001, 8002, 8003) {
        $inUse = Get-NetTCPConnection -LocalPort $p -ErrorAction SilentlyContinue
        if (-not $inUse) { $PORT = $p; break }
    }
    if (-not $PORT) {
        Write-Host "  No free port found (tried 8000-8003). Free a port and try again."
        exit 1
    }

    # --- Warn if GEE key missing ---
    if (-not (Test-Path "config\gee-key.json")) {
        Write-Host "  WARNING: config\gee-key.json not found."
        Write-Host "  The app will start but GEE operations will fail."
        Write-Host ""
    }

    Write-Host "  App port : $PORT"
    Write-Host ""

    $logPath = Join-Path $PSScriptRoot "pixi.log"

    try {
        # --- Build frontend ---
        $needsInstall = (-not (Test-Path "frontend\node_modules")) -or
                        ((Get-Item "frontend\package.json").LastWriteTime -gt
                         (Get-Item "frontend\node_modules").LastWriteTime)
        if ($needsInstall) {
            Write-Host "  Installing frontend dependencies..."
            & pixi run npm-install
            if ($LASTEXITCODE -ne 0) { throw "Frontend install failed." }
        } else {
            Write-Host "  Frontend dependencies up to date, skipping install."
        }

        Write-Host "  Building frontend..."
        & pixi run build-frontend
        if ($LASTEXITCODE -ne 0) { throw "Frontend build failed." }

        # --- Launch backend ---
        Write-Host "  Starting backend..."

        $pinfo = New-Object System.Diagnostics.ProcessStartInfo
        $pinfo.FileName               = "pixi"
        $pinfo.Arguments              = "run python -m uvicorn backend.app:app --host 0.0.0.0 --port $PORT"
        $pinfo.UseShellExecute        = $false
        $pinfo.CreateNoWindow         = $true
        $pinfo.RedirectStandardOutput = $true
        $pinfo.RedirectStandardError  = $true
        $pinfo.WorkingDirectory       = $PSScriptRoot
        $pinfo.EnvironmentVariables["GOOGLE_APPLICATION_CREDENTIALS"] = `
            Join-Path $PSScriptRoot "config\gee-key.json"

        $proc = New-Object System.Diagnostics.Process
        $proc.StartInfo           = $pinfo
        $proc.EnableRaisingEvents = $true
        $proc.Start() | Out-Null

        $logFile           = [System.IO.StreamWriter]::new($logPath, $false)
        $logFile.AutoFlush = $true

        $lines = [System.Collections.Generic.List[string]]::new()

        $msgData = @{ Log = $logFile; Lines = $lines }

        $stdoutEvent = Register-ObjectEvent -InputObject $proc `
            -EventName OutputDataReceived -MessageData $msgData -Action {
            if ($null -ne $EventArgs.Data) {
                $Event.MessageData.Lines.Add($EventArgs.Data)
                $Event.MessageData.Log.WriteLine($EventArgs.Data)
            }
        }
        $stderrEvent = Register-ObjectEvent -InputObject $proc `
            -EventName ErrorDataReceived -MessageData $msgData -Action {
            if ($null -ne $EventArgs.Data) {
                $Event.MessageData.Lines.Add($EventArgs.Data)
                $Event.MessageData.Log.WriteLine($EventArgs.Data)
            }
        }

        $proc.BeginOutputReadLine()
        $proc.BeginErrorReadLine()

        # --- Poll for uvicorn ready line (up to 60 s) ---
        Write-Host "  Waiting for backend..."
        Write-Host ""

        $confirmedPort = $null
        $startupFailed = $false
        $deadline      = (Get-Date).AddSeconds(60)

        while ((Get-Date) -lt $deadline) {
            foreach ($line in $lines.ToArray()) {
                if ($line -match 'Uvicorn running on http://[^:]+:(\d+)') {
                    $confirmedPort = [int]$Matches[1]
                }
                if ($line -match 'Address already in use|failed to bind') {
                    $startupFailed = $true
                }
            }

            if ($confirmedPort) { break }

            if ($startupFailed) {
                throw "Backend failed to bind to port. Check pixi.log for details."
            }

            if ($proc.HasExited) {
                throw "Backend process exited unexpectedly (code $($proc.ExitCode)). Check pixi.log for details."
            }

            $elapsed = [int]((Get-Date) - $deadline.AddSeconds(-60)).TotalSeconds
            Write-Host -NoNewline "`r  Still waiting... ($elapsed s)  "
            Start-Sleep -Milliseconds 500
        }
        Write-Host -NoNewline "`r                                    `r"

        Unregister-Event -SourceIdentifier $stdoutEvent.Name -ErrorAction SilentlyContinue
        Unregister-Event -SourceIdentifier $stderrEvent.Name -ErrorAction SilentlyContinue

        if (-not $confirmedPort) {
            throw "Backend did not report ready within 60 s. Check pixi.log for details."
        }

        # --- Verify HTTP response ---
        Write-Host "  Verifying HTTP response..."
        Write-Host ""
        $ready = $false
        for ($i = 0; $i -lt 10; $i++) {
            try {
                $r = Invoke-WebRequest -Uri "http://127.0.0.1:$PORT/api/gee-key" `
                         -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
                Write-Host "  Attempt $($i+1): HTTP $($r.StatusCode)"
                if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
                    $ready = $true; break
                }
            } catch {
                Write-Host "  Attempt $($i+1) failed: $_"
            }
            Start-Sleep 1
        }

        if (-not $ready) {
            throw "Uvicorn started but app did not respond on port $PORT. Check pixi.log for details."
        }

        # --- Write state files only after confirmed ready ---
        Set-Content -Path ".pixi.port" -Value $PORT
        Set-Content -Path ".pixi.pid"  -Value $proc.Id

        Write-Host ""
        Write-Host "  =========================================="
        Write-Host "   GEE Web App is ready"
        Write-Host "   http://localhost:$PORT"
        Write-Host "  =========================================="
        Write-Host ""
        Start-Process "http://localhost:$PORT"
        Write-Host "  Run: pixi.bat stop -- when you are done."
        Write-Host ""

    } catch {
        Write-Host ""
        Write-Host "  ERROR: $_"
        Write-Host ""

        if ($null -ne $stdoutEvent) {
            Unregister-Event -SourceIdentifier $stdoutEvent.Name -ErrorAction SilentlyContinue
        }
        if ($null -ne $stderrEvent) {
            Unregister-Event -SourceIdentifier $stderrEvent.Name -ErrorAction SilentlyContinue
        }

        if ($null -ne $logFile) {
            try { $logFile.Close() } catch {}
        }

        if ($null -ne $proc -and -not $proc.HasExited) {
            Write-Host "  Killing backend process (PID $($proc.Id))..."
            try { $proc.Kill($true) } catch {}
        }

        taskkill /im uvicorn.exe /f /t 2>$null | Out-Null
        taskkill /im pixi.exe    /f /t 2>$null | Out-Null

        Remove-Item -Force ".pixi.port" -ErrorAction SilentlyContinue
        Remove-Item -Force ".pixi.pid"  -ErrorAction SilentlyContinue

        if (Test-Path "pixi.last.log") {
            Write-Host "  Check pixi.last.log for the previous run if this is a repeat failure."
        }
        Write-Host ""
        exit 1
    }
}

# ── Entry point ────────────────────────────────────────────────────────────────

switch ($Command) {
    "start" { Start-App }
    "stop"  { Stop-App }
    default {
        Write-Host ""
        Write-Host "  Usage: pixi.bat [start|stop]"
        Write-Host ""
        Write-Host "    start  -- build frontend and start backend via Pixi"
        Write-Host "    stop   -- stop all Pixi-managed processes"
        Write-Host ""
        exit 1
    }
}