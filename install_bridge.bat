@echo off
REM ============================================
REM HVC MT5 Bridge - Installation Script
REM Run as Administrator on Windows VPS
REM ============================================

echo ==========================================
echo HVC MT5 Bridge - Setup
echo ==========================================
echo.

REM 1. Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Install Python 3.11+ first.
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 2. Install dependencies
echo [1/4] Installing Python packages...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install packages
    pause
    exit /b 1
)

REM 3. Create directories
echo [2/4] Creating directories...
if not exist state mkdir state
if not exist logs mkdir logs

REM 4. Discover symbols
echo [3/4] Discovering MT5 symbols...
echo.
echo Make sure MT5 terminal is RUNNING and LOGGED IN before continuing.
echo.
pause
python hvc_mt5_bridge.py --discover-symbols

REM 5. Remind to configure
echo.
echo ==========================================
echo SETUP COMPLETE
echo ==========================================
echo.
echo NEXT STEPS:
echo 1. Edit config.json with your Startrader credentials
echo 2. Update symbol_map with the correct symbol names from MT5
echo 3. Generate a random API key for security.api_key
echo 4. Test: python hvc_mt5_bridge.py --dry-run
echo 5. Test with curl:
echo    curl -X POST http://localhost:8080/signal -H "Content-Type: application/json" -H "X-API-Key: YOUR_KEY" -d "{\"symbol\":\"EURUSD\",\"direction\":\"LONG\",\"entry_price\":1.0850,\"sl_price\":1.0825,\"tp_price\":1.0925}"
echo.
echo 6. Install as Windows service (optional):
echo    nssm install HVC-MT5-Bridge "C:\Python311\python.exe" "C:\HVC\mt5-bridge\hvc_mt5_bridge.py"
echo.
pause
