# GEE Web App

A browser-based tool for downloading satellite data (precipitation, temperature, vegetation, land cover, and more) from Google Earth Engine for any area you choose.

---

## Before you start

You need two things:

**1. Docker Desktop**
This is the only software you need to install. Download it from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and follow the installer. Once it is installed and running, you are ready.

**2. A Google Earth Engine key file**
A small file (ending in `.json`) that gives the app access to Google Earth Engine. If you do not have one yet, see [Getting a key](#getting-a-key) below.

---

## Starting the app

1. Put the `gee_web_app` folder somewhere on your computer.

2. Launch the app:
   - **Mac:** double-click `Start.command`
   - **Windows:** double-click `Start.bat`
   - **Linux:** open a terminal in the folder and run `./quickstart.sh`

   > **Mac — first launch only:** if you see "cannot be opened because the developer cannot be verified", right-click `Start.command`, choose **Open**, then click **Open**. You won't be asked again.

   The first time you launch it will take a few minutes to set up. You could take this time to read the [User Manual](USER_MANUAL.md), which has more detailed technical information about the app. When it is ready, your browser will open automatically.

3. The app opens in your browser. While it is downloading, you can safely close the browser tab and the download will continue in the background.

---

## Stopping the app

- **Mac:** double-click `Stop.command`
- **Windows:** double-click `Stop.bat`
- **Linux:** run `./stop.sh` in a terminal

Closing the browser tab does not stop the app — you need to use the stop file. 

---

## Getting a key

1. Go to [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and select your project.
2. Open or create a service account that has the **Earth Engine** role.
3. Click **Keys → Add Key → Create new key → JSON** and download the file.

The first time you open the app it will ask you to upload this file. After that it remembers it and you will not be asked again.


