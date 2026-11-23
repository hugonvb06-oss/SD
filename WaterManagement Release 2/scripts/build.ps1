# Compila los modulos de WaterManagement Release 2 sin Maven (usa las librerias de WaterManagement Release 2/lib).
# WM_W y WM_Registry se compilan aparte con Maven (pom.xml).
$ROOT = Split-Path -Parent $PSScriptRoot
$CP = "$ROOT\lib\jackson-databind-2.18.3.jar;$ROOT\lib\jackson-core-2.18.3.jar;$ROOT\lib\jackson-annotations-2.18.3.jar;$ROOT\lib\kafka-clients-3.9.1.jar;$ROOT\lib\sqlite-jdbc-3.53.4.0.jar;$ROOT\lib\slf4j-api-1.7.36.jar;$ROOT\lib\slf4j-simple-1.7.36.jar"

javac --release 11 -encoding UTF-8 -cp $CP -d "$ROOT\WM_Central\out" (Get-ChildItem -Path "$ROOT\WM_Central\src" -Filter *.java).FullName
javac --release 11 -encoding UTF-8 -cp $CP -d "$ROOT\WM_FO\out" (Get-ChildItem -Path "$ROOT\WM_FO\src" -Filter *.java).FullName
javac -encoding UTF-8 -cp $CP -d "$ROOT\WM_WS\bin" (Get-ChildItem -Path "$ROOT\WM_WS\src" -Filter *.java).FullName
# API_Central usa com.sun.net.httpserver (JDK) y no se compila con --release 11.
javac -encoding UTF-8 -cp $CP -d "$ROOT\WM_API_Central\out" (Get-ChildItem -Path "$ROOT\WM_API_Central\src" -Filter *.java).FullName
Write-Host "OK: WM_Central, WM_FO, WM_WS y WM_API_Central compilados."
