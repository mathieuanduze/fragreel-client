@echo off
echo ============================================
echo  FragReel Client — Build Windows .exe
echo ============================================

:: Install dependencies
pip install -r requirements.txt

:: Clean previous build
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build

:: Build
pyinstaller FragReel.spec

echo.
echo ============================================
echo  Build concluido!
echo  Executavel em: dist\FragReel.exe
echo ============================================
pause
