@echo off
chcp 65001 >nul
cd /d "%~dp0"

:: Lanzar la interfaz (abre el navegador automaticamente)
start "" python ui.py
if errorlevel 1 (
    echo.
    echo [ERROR] No se pudo iniciar AgentIVA.
    python ui.py
    pause
)
