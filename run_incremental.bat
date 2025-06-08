@echo OFF
CHCP 65001 > NUL

ECHO =======================================================
ECHO ==      Starting Daily Incremental ETL Pipeline      ==
ECHO =======================================================
ECHO Pipeline started at %TIME% on %DATE%.
ECHO.

SET PYTHON_EXE="C:\Users\Bakir Subic\PyCharmMiscProject\.venv\Scripts\python.exe"

CD /D "%~dp0"
ECHO Now running from: %CD%
ECHO.

ECHO Running incremental load...
%PYTHON_EXE% incremental_load.py
IF %ERRORLEVEL% NEQ 0 ( GOTO :Error )
ECHO.

ECHO =======================================================
ECHO ==        Daily Incremental Pipeline Finished        ==
ECHO =======================================================
GOTO :End

:Error
ECHO.
ECHO ******************** An error occurred. Script halted. ********************
ECHO.

:End
PAUSE