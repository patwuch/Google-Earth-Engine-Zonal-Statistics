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

---

# GEE Web App（繁體中文說明）

簡單從 Google Earth Engine 下載您所選區域的衛星資料（降水量、氣溫、植被、土地覆蓋等）。

---

## 開始之前

您需要準備兩樣東西：

**1. Docker Desktop**
這是您唯一需要安裝的軟體。請從 [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) 下載並依照安裝程式操作。安裝並執行後即可開始使用。

**2. Google Earth Engine 金鑰檔案**
一個小檔案（副檔名為 `.json`），用於授權應用程式存取 Google Earth Engine。如果您還沒有，請參閱下方的[取得金鑰](#取得金鑰)說明。

---

## 啟動應用程式

1. 將 `gee_web_app` 資料夾放到您電腦上的任意位置。

2. 啟動應用程式：
   - **Mac：**  `Start.command`點兩下
   - **Windows：**  `Start.bat`點兩下
   - **Linux：** 在資料夾中開啟終端機並執行 `./quickstart.sh`

   > **Mac — 首次啟動：** 若出現「無法開啟，因為無法驗證開發者」的提示，請右鍵點擊 `Start.command`，選擇**開啟**，然後點擊**開啟**。之後不會再出現此提示。

   首次啟動需要幾分鐘進行初始化。您可以利用這段時間閱讀[使用手冊](USER_MANUAL.md)，其中包含更詳細的說明。準備就緒後，瀏覽器將自動開啟。

3. 應用程式在瀏覽器中開啟。下載過程中可以安全地關閉瀏覽器分頁，下載將在背景繼續進行。

---

## 停止應用程式

- **Mac：** 雙擊 `Stop.command`
- **Windows：** 雙擊 `Stop.bat`
- **Linux：** 在終端機中執行 `./stop.sh`

關閉瀏覽器分頁不會停止應用程式——您需要使用對應的停止檔案。

---

## 取得金鑰

1. 前往 [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) 並選擇您的專案。
2. 開啟或建立一個具有 **Earth Engine** 角色的服務帳戶。
3. 點擊**金鑰 → 新增金鑰 → 建立新金鑰 → JSON**，然後下載該檔案。

首次開啟應用程式時，系統會要求您上傳此檔案。之後應用程式會記住它，不會再次詢問。


