@echo off
chcp 65001 >nul
cd /d "%~dp0"
title grokcli-2api

echo.
echo  === grokcli-2api ===
echo  Working dir: %CD%
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] 未找到 python，请先安装 Python 3.10+ 并加入 PATH
  pause
  exit /b 1
)

REM Ensure git submodule for registration engine (best-effort)
if exist ".gitmodules" (
  if not exist "grok-build-auth\xconsole_client\__init__.py" (
    echo [INFO] 初始化 git submodule: grok-build-auth ...
    where git >nul 2>nul
    if not errorlevel 1 (
      git submodule update --init --recursive
    ) else (
      echo [WARN] 未找到 git，无法自动初始化 submodule
      echo        请手动执行: git submodule update --init --recursive
    )
  )
)

python -c "import fastapi,uvicorn,httpx" 2>nul
if errorlevel 1 (
  echo Installing dependencies...
  python -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [ERROR] 依赖安装失败
    pause
    exit /b 1
  )
)

REM Optional registration deps (curl_cffi lives here too)
if exist "grok-build-auth\requirements.txt" (
  python -c "import curl_cffi" 2>nul
  if errorlevel 1 (
    echo Installing grok-build-auth dependencies...
    python -m pip install -r grok-build-auth\requirements.txt
  )
)

REM Put submodule on PYTHONPATH so xconsole_client is importable
set "PYTHONPATH=%CD%\grok-build-auth;%PYTHONPATH%"

REM 默认自动打开浏览器；设 GROK2API_OPEN_BROWSER=0 可关闭
if not defined GROK2API_OPEN_BROWSER set GROK2API_OPEN_BROWSER=1
if not defined GROK2API_HOST set GROK2API_HOST=127.0.0.1
if not defined GROK2API_PORT set GROK2API_PORT=3000

echo Starting grokcli-2api on http://%GROK2API_HOST%:%GROK2API_PORT% ...
echo Admin: http://127.0.0.1:%GROK2API_PORT%/admin
echo.

python -c "import admin_routes, app; print('import-check ok')" 2>nul
if errorlevel 1 (
  echo [WARN] 预检查 import 失败，仍尝试启动。若立刻退出请把完整报错发出来。
  echo.
)

python app.py
set EXITCODE=%ERRORLEVEL%
if not %EXITCODE%==0 (
  echo.
  echo [ERROR] 服务退出，代码 %EXITCODE%
  echo 常见修复:
  echo   1^) git submodule update --init --recursive
  echo   2^) python -m pip install -r requirements.txt
  echo   3^) python -m pip install -r grok-build-auth\requirements.txt
  pause
)
exit /b %EXITCODE%
