@echo off
SETLOCAL

:: --- CONFIGURATION ---
SET CONDA_ENV_NAME=trend-lens

:: Get script directory
SET "SCRIPT_DIR=%~dp0"
CD /D "%SCRIPT_DIR%"

echo ==========================================
echo   Starting Trend Lens Streamlit App...
echo ==========================================
echo.

:: 1. Activate Conda Environment
echo [1/2] Activating Conda environment: %CONDA_ENV_NAME%...
call conda activate %CONDA_ENV_NAME%
IF %ERRORLEVEL% NEQ 0 (
    echo Error: Could not activate Conda environment '%CONDA_ENV_NAME%'.
    pause
    exit /b
)

:: 2. Launch Streamlit
echo [2/2] Launching Streamlit app...
streamlit run app.py

echo.
echo Application closed.
pause