@echo off

:: Check if already admin
net session >nul 2>&1
if %errorLevel% == 0 (
    cd /d "C:\Program Files\metabase"
    java --add-opens java.base/java.nio=ALL-UNNAMED -jar metabase.jar
    pause
) else (
    echo Requesting admin privileges...
    powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\"' -Verb RunAs"
)