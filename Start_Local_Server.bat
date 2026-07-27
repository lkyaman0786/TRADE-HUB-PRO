@echo off
title Trade Hub Pro - Local Server Launcher
echo ===================================================
echo   TRADE HUB PRO - LOCAL ENGINE LAUNCHER (OFFLINE)
echo ===================================================
echo.
echo [INFO] Starting Flask Backend Trade Engine...
echo [INFO] Local Web Dashboard will open at: http://127.0.0.1:5000
echo.
echo Do not close this window while trading.
echo ===================================================
echo.
python algo.py
pause
