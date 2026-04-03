# GEE Web App — User Manual

A step-by-step guide to downloading satellite data from Google Earth Engine using the browser interface.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Starting and stopping the app](#3-starting-and-stopping-the-app)
4. [Uploading your GEE key](#4-uploading-your-gee-key)
5. [Working with runs](#5-working-with-runs)
6. [Uploading an Area of Interest](#6-uploading-an-area-of-interest)
7. [Configuring datasets](#7-configuring-datasets)
8. [Running the analysis](#8-running-the-analysis)
9. [Monitoring progress](#9-monitoring-progress)
10. [Downloading results](#10-downloading-results)
11. [Partial checkout (mid-run downloads)](#11-partial-checkout-mid-run-downloads)
12. [Stopping, retrying, and resuming runs](#12-stopping-retrying-and-resuming-runs)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Overview

GEE Web App lets you extract satellite data from Google Earth Engine for any geographic area you choose. You define:

- **Where** — by uploading a boundary file (your Area of Interest)
- **What** — by selecting one or more datasets and their variables
- **When** — by setting a date range per dataset

The app sends those requests to Google Earth Engine in the background and produces downloadable files (GeoParquet and CSV) when the extraction is done. Runs can take anywhere from a few minutes to several hours depending on area size and date range.

---

## 2. Prerequisites

You need **one** of the following runtime options, plus a GEE key:

### Option A — Docker

Best for: complete environment isolation. Docker runs everything in containers so nothing is installed on your machine beyond Docker itself.

Install [Docker Desktop](https://www.docker.com/products/docker-desktop) and make sure it is open and running before launching the app. No other software is needed.

### Option B — Pixi

Best for: environments where Docker is blocked or unavailable (e.g. managed corporate machines, university networks, or anywhere Docker Desktop cannot be installed).

Pixi is a lightweight package manager that installs the app's dependencies directly on your machine without containers.

**Pixi is installed automatically if not already present.** On both Windows and Linux, if Pixi is not found the start script will detect this and prompt you to install it before continuing. No manual steps needed.

### GEE service account key

A `.json` file that gives the app access to Google Earth Engine. See [Getting a key](README.md#getting-a-key) in the README if you do not have one yet.

---

## 3. Starting and stopping the app

### Start

**Docker option:**

| Platform | Action |
|----------|--------|
| Windows | Double-click `Start-docker.bat` |
| Linux | Run `./Start-docker.sh` in a terminal |

**Pixi option:**

| Platform | Action |
|----------|--------|
| Windows | Double-click `Start-pixi.bat` |
| Linux | Run `./Start-pixi.sh` in a terminal |

The first launch downloads and installs all dependencies (3–10 minutes). Every launch after that is much faster. When the app is ready, your browser opens automatically.

### Stop

**Docker option:**

| Platform | Action |
|----------|--------|
| Windows | Double-click `Stop-docker.bat` |
| Linux | Run `./Stop-docker.sh` in a terminal |

**Pixi option:**

| Platform | Action |
|----------|--------|
| Windows | Double-click `Stop-pixi.bat` |
| Linux | Run `./Stop-pixi.sh` in a terminal |

Closing the browser tab does **not** stop the app. Any extraction that is in progress will keep running in the background until you use the stop file — or until it finishes.

---

## 4. Uploading your GEE key

The first time you open the app you will see an upload prompt instead of the main interface. Drag your `.json` key file onto the upload area, or click it to browse. The app validates the key and shows the service account email when successful.

The key is stored locally and reused every time you restart the app. You will not be asked again unless you replace it.

**To replace the key:** find the **GEE Credentials** section at the top of the left sidebar. It shows the connected account email with a **Replace** button. Click it to upload a different key.

---

## 5. Working with runs

Each extraction is organised into a **run** — a self-contained record that holds your configuration, input geometry, logs, and output files.

### Creating a new run

Click the **+ New** button in the **Run Session** section of the sidebar. The app pre-fills a unique run ID; you can keep it or type your own. Click **Confirm** to create the run.

### Selecting a previous run

Open the **Previous runs** dropdown in the sidebar. Each row shows a run ID, its status (running, completed, failed, stopped), and the datasets it included. Click any row to load that run and see its full state, logs, and results.

### Run ID

The run ID is a short identifier that links all the files for that run together. You can use it to find your output files in `data/runs/<run_id>/results/`.

---

## 6. Uploading an Area of Interest

With a run selected, the main area on the left shows the **Area of Interest** upload zone. Drag your boundary file onto it, or click to browse. Accepted formats:

| Format | File extension |
|--------|---------------|
| Shapefile | `.zip` containing `.shp`, `.shx`, `.dbf`, `.prj` |
| GeoJSON | `.geojson` |
| GeoParquet | `.parquet` |

After upload the app shows a map preview of your geometry (in green), along with the feature count, coordinate system, and bounding box.

Once you submit a run, the geometry is locked to that run and cannot be changed. If you load a previous run, the geometry is already stored — no re-upload is needed.

---

## 7. Configuring datasets

The **Datasets** section of the sidebar lists all available products. Each one is collapsed by default.

**To enable a dataset:**

1. Tick its checkbox to enable it. This expands the configuration card.
2. Set the **date range** (start and end date). Valid dates for each dataset are enforced automatically.
3. Select the **bands** you want to extract. All bands are selected by default — deselect any you do not need.
4. Select the **statistics** to compute (e.g. mean, min, max, median). The appropriate defaults are pre-filled.

You can enable multiple datasets in one run; they are processed in parallel.

**Available datasets:**

| Dataset | What it measures | Resolution | Date coverage |
|---------|-----------------|------------|---------------|
| **CHIRPS** | Precipitation | ~5.6 km | 1981 → present |
| **ERA5-Land** | Temperature, precipitation, evaporation (11 variables) | ~9 km | 1950 → present |
| **MODIS LST** | Land surface temperature (day + night) | 1 km | Feb 2000 → present |
| **MODIS NDVI/EVI** | Vegetation indices | 250 m | Feb 2000 → present |
| **WorldCover v1.0** | Land cover classification | 10 m | 2020 only |
| **WorldCover v2.0** | Land cover classification | 10 m | 2021 only |
| **MODIS LULC** | Land use and land cover types | 500 m | 2001–2023 |

---

## 8. Running the analysis

Once you have uploaded an AOI and configured at least one dataset (with bands and statistics selected), the **Run Analysis** button in the right panel becomes active. Click it to submit.

The app freezes the configuration, logs a start event, and begins extracting data from Google Earth Engine in the background. You can safely close the browser tab — the extraction continues in the background and your results will be waiting when you come back.

---

## 9. Monitoring progress

While a run is active you can watch its progress in the right panel:

**Progress bar** — shows how many chunks (sub-tasks) have completed out of the total. Large areas and long date ranges are split into many smaller chunks; this bar lets you track how far along the run is. Failed chunks are shown in red.

**Snakemake log** — displayed below the map on the left. This is the live output of the extraction pipeline, colour-coded: green for completed steps, red for errors, yellow for warnings. It updates every few seconds.

**Event log** — at the bottom of the sidebar, showing timestamped messages across all runs. Errors appear in red.

---

## 10. Downloading results

When a run finishes (status: **completed**), the **Download Results** section appears in the right panel. For each dataset product you will see two buttons:

- **GeoParquet** — the full result file with geometry included, ready for use in Python (GeoPandas), QGIS, or similar tools.
- **CSV** — a flat table version of the same data (geometry removed). Useful for spreadsheet tools.

Both files are also saved directly on your computer at:

```
data/runs/<run_id>/results/<product>_<start>_to_<end>.parquet
data/runs/<run_id>/results/<product>_<start>_to_<end>.csv
```

---

## 11. Partial checkout (mid-run downloads)

If you need results before a run finishes — for example, the run is still processing later years but you want early results now — use **partial checkout**.

1. With the run selected, find the **Build Partial Checkout** button in the right panel.
2. Click it. The app merges all chunks that have completed so far into a single file per product.
3. Download buttons appear labelled **GeoParquet** and **CSV**, just like for a completed run.

Click **Build Partial Checkout** again at any time to include newer completed chunks.

---

## 12. Stopping, retrying, and resuming runs

### Stopping a running run

Click the **Stop Run** button (red) in the right panel while the run is active. The pipeline halts and the run is marked **stopped**.

### Retrying a failed or stopped run

If a run shows status **failed** or **stopped**, a **Retry Run** button appears. Clicking it restarts the pipeline from where it left off — completed chunks are not re-extracted.

### Resuming a run from a previous session

Select the run from the **Previous runs** dropdown in the sidebar. The run state, logs, and any available results load automatically. If the run was still in progress when you last closed the app, its status reflects what happened while it ran in the background.

---

## 13. Troubleshooting

| Symptom | Likely cause | What to do |
|---------|-------------|------------|
| Browser does not open after launching (Docker) | Docker is not running | Open Docker Desktop and wait for it to start, then try again |
| Browser does not open after launching (Pixi) | Pixi not installed or not on PATH | Re-run the start script and accept the install prompt; if it still fails, open a new terminal and try again |
| `pixi` command not found after the install prompt | PATH not updated in current session | Close and reopen your terminal, then run the start script again |
| App opens but shows an upload prompt immediately | No GEE key stored yet | Upload your `.json` service account key |
| "Authentication error" after uploading key | Key is invalid or the service account lacks Earth Engine access | Check that the service account has the **Earth Engine** role in Google Cloud Console |
| Run shows **failed** immediately | Configuration or geometry issue | Check the event log for an error message; correct the issue and click **Retry Run** |
| Progress bar has been stuck for a long time | GEE rate limit or network issue | Wait a few minutes; check the Snakemake log for error lines |
| Download button does nothing | No result file yet | Ensure the run is completed or use **Build Partial Checkout** for in-progress runs |
| File ownership errors on Linux (Docker) | UID/GID mismatch | Always use `./Start-docker.sh` to start the app rather than starting Docker manually |
| App stops responding after I close the browser | Expected — the app still runs | Re-open the browser and navigate to the address shown when you started the app |
