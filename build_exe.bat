@echo off
title Amazon Scraper v2 - Export Tool Builder
cd /d "%~dp0"

echo =========================================
echo   Amazon Scraper v2 - Export Tool Builder
echo =========================================
echo.
echo   Prerequisites:
echo     1. Python installed (with "Add to PATH")
echo     2. export_local.py in the same folder
echo     3. Run: pip install pyinstaller openpyxl
echo.

REM == Check Python ==
echo Checking Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Python not found!
    echo        Install from: https://www.python.org/downloads/
    echo        Make sure to check "Add Python to PATH"
    pause
    exit /b 1
)
python --version
echo.

REM == Check export_local.py ==
if not exist "export_local.py" (
    echo [ERROR] export_local.py not found!
    echo        Download it from the server Settings page.
    pause
    exit /b 1
)

REM == Check PyInstaller ==
python -m PyInstaller --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] PyInstaller not installed!
    echo        Run: pip install pyinstaller openpyxl
    pause
    exit /b 1
)

REM == Build ==
echo Building ExportTool.exe (may take 1-2 minutes)...
python -m PyInstaller --onefile --name ExportTool --clean --noconfirm export_local.py >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Build failed! Showing details:
    python -m PyInstaller --onefile --name ExportTool --clean --noconfirm export_local.py
    pause
    exit /b 1
)

echo.
echo =========================================
echo   Build complete!
echo   File: %cd%\dist\ExportTool.exe
echo =========================================
echo.
echo   Double-click ExportTool.exe to use.
echo   Copy to any PC - no Python needed.
echo.

rd /s /q build >nul 2>&1
del /f ExportTool.spec >nul 2>&1
explorer dist
pause
