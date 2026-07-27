@echo off
title Trade Hub Pro - Online Server Launcher
echo ===================================================
echo   TRADE HUB PRO - ONLINE TUNNEL LAUNCHER
echo ===================================================
echo.
echo [1/2] Starting Flask Backend Engine...
start "Trade Hub Engine" python algo.py
echo.
echo [2/2] Launching Secure HTTPS Tunnel...
echo.
.\cloudflared.exe tunnel --protocol http2 --edge-ip-version 4 --url http://127.0.0.1:5000
pause
