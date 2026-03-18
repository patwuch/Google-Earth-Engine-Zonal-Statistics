# GEE Batch Processor — User Manual

A practical guide to using the app, with setup and troubleshooting for when things go wrong.

---

## Table of Contents

1. [Using the downloader (Streamlit UI)](#1-using-the-downloader-streamlit-ui)
   - [Section 0 — Run Session (sidebar)](#section-0--run-session-sidebar)
   - [Section 1 — Data Parameters (sidebar)](#section-1--data-parameters-sidebar)
   - [Section 2 — Area of Interest](#section-2--area-of-interest)
   - [Section 3 — Results](#section-3--results)
   - [Section 4 — Partial Checkout](#section-4--partial-checkout)
2. [Prerequisites](#2-prerequisites)
3. [Getting the code](#3-getting-the-code)
4. [GEE credentials](#4-gee-credentials)
5. [Building and launching Docker](#5-building-and-launching-docker)
6. [Stopping, resuming, and housekeeping](#6-stopping-resuming-and-housekeeping)
7. [Troubleshooting](#7-troubleshooting)
8. [For developers](#8-for-developers)

---

## 1. Using the downloader (Streamlit UI)

The UI is divided into numbered sections. Work through them top-to-bottom for a new run.

---

### Section 0 — Run Session (sidebar)

Located at the top of the left sidebar.

| Field | Purpose |
|-------|---------|
| **RUN ID** (text box) | Leave blank to auto-generate a 6-character ID for a new run. Type an existing ID to resume or monitor a previous run. |

When you type an existing RUN ID, a status badge (`running`, `completed`, `failed`) appears below the input. If the run is still active, a **Stop This RUN ID** button is also shown.

The **Local RUN registry** expander beneath shows a table of every run ever started, with status and timestamps. Click any row in the **Inspect a stored RUN** selector to see its full configuration.

---

### Section 1 — Data Parameters (sidebar)

Each dataset is collapsed inside its own expander. Expand one or more to configure them.

**Available datasets:**

| Dataset | What it measures | Native resolution | Date range |
|---------|-----------------|-------------------|------------|
| **CHIRPS** | Precipitation | 0.05° (~5.6 km) | 1981-01 → present |
| **ERA5_LAND** | Temperature, precipitation, evaporation (11 variables) | ~9 km | 1950-01 → present |
| **MODIS_LST** | Land Surface Temperature (day + night) | 1 km | 2000-02 → present |
| **MODIS_NDVI_EVI** | Vegetation indices | 250 m | 2000-02 → present |
| **WorldCover_v100** | Land cover classification 2020 | 10 m | 2020 only |
| **WorldCover_v200** | Land cover classification 2021 | 10 m | 2021 only |
| **MODIS_LULC** | Land cover types | 500 m | 2001–2023 |

**For each dataset you enable:**

1. Tick **Enable `<dataset>`**.
2. **Select Bands** — choose which variables to extract (all are ticked by default).
3. **Select Statistics (Reducers)** — `mean`, `sum`, `min`, `max`, `median`. The default is pre-filled per dataset.
4. **Select date range** — Products with composite cadence will automatically extract data within the selected range.

You can enable multiple datasets in one run; they are processed in parallel subject to GEE rate limits.

---

### Section 2 — Area of Interest

This is the left main column, labelled **2. Area of Interest**.

#### Uploading your geometry

Click **Browse files** and upload one of:

| Format | Notes |
|--------|-------|
| **ZIP shapefile** | Must contain `.shp`, `.shx`, `.dbf`, `.prj` together in one ZIP |
| **GeoJSON** | Single `.geojson` file |
| **GeoParquet** | `.parquet` or `.geoparquet` with a geometry column |

The app validates the geometry and stores it inside the run directory at `data/runs/<run_id>/inputs/`, keeping all inputs and outputs together in one place.

#### New run vs. resume

- Leave **RUN ID** blank in Section 0 to start fresh.
- Enter an existing RUN ID to resume — the geometry is already stored with that run, so no re-upload is needed.

#### Running the pipeline

Once at least one dataset is configured and the AOI is uploaded, the **Run Analysis** button becomes active. Click it to:

1. Freeze the run configuration.
2. Auto-generate or reuse the RUN ID.
3. Show an **Execution Plan** with job counts.
4. Launch the extraction pipeline in the background.

Progress is shown below as chunk and final-file counts update. You can safely navigate away and come back — the pipeline runs in the background inside the container.

---

### Section 3 — Results

Located in the right main column, labelled **3. Results**.

Once a run has completed, result files appear in the UI with two options:

- **GeoParquet (recommended)** — written to `data/runs/<run_id>/results/` automatically when the pipeline finishes. The file is already on your computer; the UI button is just a convenience shortcut.
- **CSV** — clicking the CSV button converts the GeoParquet to a flat table (geometry removed) and saves it next to the parquet as `<product>_<start>_to_<end>.csv`. Subsequent clicks read from that saved file. Like the parquet, it is directly accessible in `data/runs/<run_id>/results/`.

File naming convention:

```
<product>_<start_date>_to_<end_date>.parquet
# e.g. CHIRPS_1986-01-01_to_2026-02-28.parquet
```

---

### Section 4 — Partial Checkout

Appears when a Results RUN ID is entered. Useful when a long run is still in progress and you need intermediate results.

1. Click **Prepare/Refresh Partial Checkout Files**.
2. The app merges all completed chunk files (even if the full pipeline hasn't finished) into a single GeoParquet per product.
3. Download buttons appear for the merged partial files.

Re-click the button at any time to include newer completed chunks.

---

## 2. Prerequisites

| Requirement | Minimum version | Notes |
|-------------|-----------------|-------|
| Docker Engine | 24+ | Includes `docker compose` plugin |
| Docker Compose | v2 | Called as `docker compose` (no hyphen) |
| Disk space | ~5 GB free | Per run; varies with AOI size and date range |
| GEE service account | — | JSON key file; see §4 |

On **macOS/Windows** use Docker Desktop. On **Linux** install Docker Engine and the Compose plugin from the official packages.

Verify your install:

```bash
docker --version        # Docker version 24.x or later
docker compose version  # Docker Compose version v2.x or later
```

---

## 3. Getting the code

Clone the repository and enter the directory:

```bash
git clone https://github.com/<your-org>/gee_web_app.git
cd gee_web_app
```

If you received the code as a ZIP archive instead, unzip it and `cd` into the resulting folder. The rest of the steps are identical.

**What you'll see inside the folder:**

```
gee_web_app/
├── main.py               ← Streamlit application (UI + pipeline launcher)
├── Snakefile_parquet     ← Snakemake workflow (GeoParquet pipeline)
├── scripts/              ← Worker scripts called by Snakemake
├── config/               ← Key stored here after first-run setup (see §4)
├── data/                 ← Created automatically; holds all run data
├── Start.command         ← Launcher for macOS (double-click)
├── Start.bat             ← Launcher for Windows (double-click)
├── Stop.command          ← Stop the app on macOS (double-click)
├── Stop.bat              ← Stop the app on Windows (double-click)
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## 4. GEE credentials

The app connects to Google Earth Engine using a **service account key**. You set this up entirely inside the browser — no manual file copying required.

**Getting a key from Google Cloud:**

1. Open the [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and select your project.
2. Create (or open) a service account that has the **Earth Engine** role.
3. Go to **Keys → Add Key → Create new key → JSON** and download the file.

**Uploading the key in the app:**

The first time you open the app, it shows a setup screen instead of the normal UI. Drag-and-drop (or click to browse) the downloaded `.json` file into the upload widget. The app validates it and confirms the service-account email — then loads the full interface automatically.

The key is saved to `config/gee-key.json` on the host and reused on every subsequent restart. You will not be prompted again unless you remove it.

**Removing or replacing the key:**

Open the **GEE credentials** expander in the left sidebar and click **Remove / replace key**. The setup screen reappears so you can upload a different key.

> `config/gee-key.json` is listed in `.gitignore` and will not be committed to git.

---

## 5. Building and launching Docker

**macOS** — double-click `Start.command` in Finder.

**Windows** — double-click `Start.bat`.

**Linux** — run `./quickstart.sh` from a terminal inside the project folder.

All three do the same thing:

1. Check that Docker Desktop is running and show a clear message if it isn't.
2. Build the image on first launch (downloads base image and installs dependencies — **takes 3–10 minutes the first time**, cached for every launch after).
3. Start the container, pick the first free port from 8501–8505, wait for the UI to respond, then open it in your browser automatically.

> **macOS — first launch only:** If you see "cannot be opened because the developer cannot be verified", right-click `Start.command` and choose **Open**, then click **Open** in the dialog. You won't be asked again.

**To stop the app** when you are done:

- **macOS:** double-click `Stop.command`
- **Windows:** double-click `Stop.bat`
- **Linux:** run `./stop.sh` in a terminal

**To follow live logs:**

```bash
docker compose logs -f app
```

---

## 6. Stopping, resuming, and housekeeping

### Stopping a running pipeline

- **From the UI:** Enter the RUN ID in Section 0 → click **Stop This RUN ID**.
- **From the terminal:** `docker compose exec app kill -TERM <snakemake_pid>` (PID shown in the sidebar).
- **Nuclear option:** `docker compose down` — stops everything; you can restart and resume.

### Resuming a failed or stopped run

1. Enter the RUN ID in the Section 0 sidebar input.
2. Configure the same products and date range.
3. Click **Run Analysis** — the stored geometry is reused automatically and the pipeline picks up from where it left off (only missing chunks are re-extracted).

### Unlocking a stale Snakemake lock

If the app crashed mid-run, Snakemake may leave a `.snakemake/locks/` directory inside the run folder. The app automatically attempts an unlock before each new run. If problems persist, run the unlock command for the specific run:

```bash
docker compose exec app bash -c "cd /app/data/runs/<run_id> && snakemake --unlock --snakefile /app/Snakefile_parquet --directory /app/data/runs/<run_id>"
```

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| App doesn't open after double-clicking the launcher | Docker is not running | Start Docker Desktop, then try again |
| UI never loads (browser stays blank) | Port conflict or image build failed | `docker compose logs -f app` to inspect |
| "GEE authentication error" | `gee-key.json` missing or wrong path | Confirm file is at `config/gee-key.json` |
| Pipeline hangs at 0 chunks | GEE rate limit or network issue | Wait 2 min; check `data/runs/<run_id>/logs/snakemake_run.log` |
| Run shows `failed` immediately | Snakemake lock from a prior crash | Run the unlock command above, then retry |
| File ownership issues on Linux | UID/GID mismatch | `./quickstart.sh` fixes this automatically by exporting `HOST_UID`/`HOST_GID` |

**Log locations:**

| Log | Path |
|-----|------|
| Snakemake run log | `data/runs/<run_id>/logs/snakemake_run.log` |
| Container stdout | `docker compose logs app` |
| Run event history | `data/runs/run_state.duckdb` → `run_events` table |

---

## 8. For developers

### Directory layout

```
gee_web_app/
├── data/              # runtime data (not committed to git)
│   └── runs/          # all per-run output
│       ├── <run_id>/
│       │   ├── inputs/       # uploaded geometry file (shapefile, GeoJSON, or GeoParquet)
│       │   ├── results/      # final merged .parquet files
│       │   ├── logs/         # snakemake_run.log and per-job logs
│       │   └── intermediate/ # chunk working files (GeoJSON → GeoParquet)
│       └── run_state.duckdb  # run status and event history
├── config/            # GEE service account key (not committed to git)
│   └── gee-key.json
├── scripts/           # worker scripts called by Snakemake
├── main.py            # Streamlit application (UI + pipeline launcher)
├── Snakefile_parquet  # Snakemake workflow orchestrator
├── quickstart.sh      # Linux launcher; picks a free port (8501–8505)
├── stop.sh            # Linux stop script
├── Start.command      # macOS launcher (double-click)
├── Start.bat          # Windows launcher (double-click)
├── Stop.command       # macOS stop (double-click)
├── Stop.bat           # Windows stop (double-click)
├── docker-compose.yml # service definition
├── Dockerfile         # base image, system deps, pip install
└── requirements.txt
```

### Live container logs

```bash
docker compose logs -f app
```

### Open a shell inside the container

```bash
docker compose exec app bash
```

### Port selection

The launchers try ports 8501–8505 and use the first one that is free. To change the candidate list, edit the `PORTS` array at the top of `quickstart.sh` or `Start.command` / `Start.bat`.

### File ownership on Linux

`quickstart.sh` exports `HOST_UID` and `HOST_GID` before starting the container so that files written inside the container are owned by your host user. If you start the container manually (e.g. `docker compose up -d`), export those variables first:

```bash
export HOST_UID=$(id -u) HOST_GID=$(id -g)
docker compose up -d app
```
