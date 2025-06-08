@echo OFF
CHCP 65001 > NUL

ECHO ====================================================================
ECHO ==  ONE-TIME SETUP: Populating Source DB ^& Initial DWH Load  ==
ECHO ====================================================================
ECHO.

SET PYTHON_EXE="C:\Users\Bakir Subic\PyCharmMiscProject\.venv\Scripts\python.exe"

CD /D "%~dp0"
ECHO Now running from: %CD%
ECHO.

ECHO [SETUP 1/2] Running source_data_generator.py...
%PYTHON_EXE% source_data_generator.py
IF %ERRORLEVEL% NEQ 0 ( GOTO :Error )
ECHO.

ECHO [SETUP 2/2] Running initial_load.py...
%PYTHON_EXE% initial_load.py
IF %ERRORLEVEL% NEQ 0 ( GOTO :Error )
ECHO.

ECHO ====================================================================
ECHO ==                      SETUP COMPLETE                          ==
ECHO ====================================================================
GOTO :End

:Error
ECHO.
ECHO ********************************************************************
ECHO *   An error occurred. The script has been halted.
ECHO *   Please review the error message above to diagnose the issue.
ECHO ********************************************************************
ECHO.

:End
PAUSE
