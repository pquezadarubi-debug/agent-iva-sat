@echo off
chcp 65001 >nul
echo ============================================================
echo   INSTALADOR — Agente IVA Devoluciones SAT
echo ============================================================
echo.

:: ─── Verificar Python 3.9+ ───────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 goto INSTALAR_PYTHON

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
for /f "tokens=1,2 delims=." %%a in ("%PYVER%") do (
    set PYMAJ=%%a
    set PYMIN=%%b
)
if %PYMAJ% LSS 3 goto INSTALAR_PYTHON
if %PYMAJ% EQU 3 if %PYMIN% LSS 9 goto INSTALAR_PYTHON

echo [OK] Python %PYVER% detectado.
goto INSTALAR_DEPS

:INSTALAR_PYTHON
echo [!] Python 3.9+ no encontrado. Descargando Python 3.12...
echo.
set PY_URL=https://www.python.org/ftp/python/3.12.0/python-3.12.0-amd64.exe
set PY_INST=%TEMP%\python-3.12.0-amd64.exe

:: Intentar descarga con PowerShell
powershell -Command "Invoke-WebRequest -Uri '%PY_URL%' -OutFile '%PY_INST%'" 2>nul
if not exist "%PY_INST%" (
    echo [ERROR] No se pudo descargar Python. Instálalo manualmente desde:
    echo         https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [*] Instalando Python 3.12 (modo silencioso)...
"%PY_INST%" /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
if errorlevel 1 (
    echo [ERROR] Falló la instalación de Python.
    pause
    exit /b 1
)
echo [OK] Python 3.12 instalado correctamente.
del "%PY_INST%" >nul 2>&1

:: Refrescar PATH
call refreshenv >nul 2>&1

:INSTALAR_DEPS
echo.
echo [*] Instalando dependencias Python...
python -m pip install --upgrade pip --quiet
python -m pip install openpyxl pandas pdfplumber pymupdf python-docx num2words pywebview --quiet
if errorlevel 1 (
    echo [ERROR] Falló la instalación de dependencias.
    echo Intenta ejecutar manualmente:
    echo   pip install openpyxl pandas pdfplumber pymupdf python-docx num2words pywebview
    pause
    exit /b 1
)
echo [OK] Dependencias instaladas.

:: ─── Crear estructura de carpetas C:\AgentIVA\ ───────────────
echo.
echo [*] Creando estructura de carpetas en C:\AgentIVA\...
if not exist "C:\AgentIVA" mkdir "C:\AgentIVA"
if not exist "C:\AgentIVA\input" mkdir "C:\AgentIVA\input"
if not exist "C:\AgentIVA\input\cfdi" mkdir "C:\AgentIVA\input\cfdi"
if not exist "C:\AgentIVA\input\estado_cuenta" mkdir "C:\AgentIVA\input\estado_cuenta"
if not exist "C:\AgentIVA\input\auxiliar" mkdir "C:\AgentIVA\input\auxiliar"
if not exist "C:\AgentIVA\input\machote" mkdir "C:\AgentIVA\input\machote"
if not exist "C:\AgentIVA\output" mkdir "C:\AgentIVA\output"
echo [OK] Carpetas creadas.

:: ─── Copiar archivos del proyecto ────────────────────────────
echo.
echo [*] Copiando archivos del agente...
set SRC=%~dp0
copy /Y "%SRC%agente_iva.py" "C:\AgentIVA\agente_iva.py" >nul
copy /Y "%SRC%ui.py" "C:\AgentIVA\ui.py" >nul
copy /Y "%SRC%AgentIVA.bat" "C:\AgentIVA\AgentIVA.bat" >nul
echo [OK] Archivos copiados.

:: ─── Crear config.json vacío si no existe ────────────────────
if not exist "C:\AgentIVA\input\config.json" (
    echo {> "C:\AgentIVA\input\config.json"
    echo   "empresa":    "",>> "C:\AgentIVA\input\config.json"
    echo   "rfc":        "",>> "C:\AgentIVA\input\config.json"
    echo   "domicilio":  "",>> "C:\AgentIVA\input\config.json"
    echo   "clabe":      "",>> "C:\AgentIVA\input\config.json"
    echo   "rep_legal":  "",>> "C:\AgentIVA\input\config.json"
    echo   "rfc_rep":    "",>> "C:\AgentIVA\input\config.json"
    echo   "autorizados": "",>> "C:\AgentIVA\input\config.json"
    echo   "folio_sat":  "">> "C:\AgentIVA\input\config.json"
    echo }>> "C:\AgentIVA\input\config.json"
    echo [OK] config.json creado. Edítalo con los datos de tu empresa.
) else (
    echo [OK] config.json existente conservado.
)

echo.
echo ============================================================
echo   Instalación completa. Ya puedes usar AgentIVA.bat
echo   Archivos en: C:\AgentIVA\
echo   Antes de usar, edita: C:\AgentIVA\input\config.json
echo ============================================================
echo.
pause
