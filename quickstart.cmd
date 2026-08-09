@echo off
REM Windows convenience wrapper so quickstart.py can be double-clicked or run as
REM `quickstart` from cmd/PowerShell. All arguments pass straight through.
REM
REM `py` is the Python launcher that the python.org installer puts on PATH; it is
REM more reliable than `python`, which on a stock Windows resolves to the Microsoft
REM Store stub that prints an advert instead of running anything.

setlocal
where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%~dp0quickstart.py" %*
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    python "%~dp0quickstart.py" %*
  ) else (
    echo Python 3 was not found on PATH.
    echo Install it from https://www.python.org/downloads/ ^(tick "Add python.exe to PATH"^)
    echo or run: winget install Python.Python.3.12
    exit /b 1
  )
)
endlocal
