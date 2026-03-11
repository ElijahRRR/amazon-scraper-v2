@echo off
title Amazon Scraper v2 - Export Tool Builder
cd /d "%~dp0"

echo =========================================
echo   Amazon Scraper v2 - Export Tool Builder
echo   Auto detect environment and build .exe
echo =========================================
echo.

REM == Step 1: Check Python ==
echo [1/4] Checking Python...
python --version >nul 2>&1
if %errorlevel% equ 0 (
    echo       Python found:
    python --version
    goto :python_ready
)

echo       Python not found, installing automatically...
echo.

set PYTHON_VERSION=3.12.8
set PYTHON_INSTALLER=python-%PYTHON_VERSION%-amd64.exe
set PYTHON_URL=https://registry.npmmirror.com/-/binary/python/%PYTHON_VERSION%/%PYTHON_INSTALLER%

echo       Downloading Python %PYTHON_VERSION% (China mirror)...
echo       URL: %PYTHON_URL%
powershell -Command "& { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('%PYTHON_URL%', '%PYTHON_INSTALLER%') }"

if not exist "%PYTHON_INSTALLER%" (
    echo.
    echo [ERROR] Python download failed!
    echo        Please install manually: https://www.python.org/downloads/
    echo        Make sure to check "Add Python to PATH"
    pause
    exit /b 1
)

echo       Installing Python (silent, adding to PATH)...
%PYTHON_INSTALLER% /quiet InstallAllUsers=0 PrependPath=1 Include_launcher=1

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Python install failed! Please install manually.
    del /f "%PYTHON_INSTALLER%" >nul 2>&1
    pause
    exit /b 1
)

echo       Python installed successfully!
del /f "%PYTHON_INSTALLER%" >nul 2>&1

REM Refresh PATH for current session
set "PATH=%LocalAppData%\Programs\Python\Python312;%LocalAppData%\Programs\Python\Python312\Scripts;%PATH%"

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Python still not available. Please close this window and try again.
    pause
    exit /b 1
)
echo       Verified:
python --version

:python_ready
echo.

REM == Step 2: Check export_local.py ==
echo [2/4] Checking export_local.py...
if exist "export_local.py" (
    echo       Found export_local.py
) else (
    echo.
    echo [ERROR] export_local.py not found!
    echo        Please download it from the server Settings page
    echo        and place it in the same folder as this script.
    echo        Current folder: %cd%
    pause
    exit /b 1
)

REM == Step 3: Install build dependencies ==
echo [3/4] Installing dependencies (Tsinghua mirror)...
python -m pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple/ pyinstaller openpyxl
if %errorlevel% neq 0 (
    echo [ERROR] Dependency install failed!
    pause
    exit /b 1
)
echo       Dependencies ready

REM == Step 4: Build .exe ==
echo.
echo [4/4] Building .exe (may take 1-2 minutes)...
python -m PyInstaller --onefile --name ExportTool --clean --noconfirm export_local.py >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Build failed! Showing details:
    python -m PyInstaller --onefile --name ExportTool --clean --noconfirm export_local.py
    pause
    exit /b 1
)

REM == Done ==
echo.
echo =========================================
echo   Build complete!
echo   File: %cd%\dist\ExportTool.exe
echo =========================================
echo.
echo   Usage: Double-click ExportTool.exe, select .db file to generate Excel.
echo   You can copy the .exe to any Windows PC - no Python needed.
echo.

REM Cleanup temp files
rd /s /q build >nul 2>&1
del /f ExportTool.spec >nul 2>&1

REM Open dist folder
explorer dist

pause
