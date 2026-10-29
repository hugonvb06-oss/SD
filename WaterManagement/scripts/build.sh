#!/bin/bash
# Compila los modulos de WaterManagement sin Maven (usa las librerias de WaterManagement/lib).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "Compilando WM_Central..."
javac --release 11 -encoding UTF-8 -cp "$ROOT/lib/*" -d "$ROOT/WM_Central/out" "$ROOT"/WM_Central/src/*.java
echo "Compilando WM_FO..."
javac --release 11 -encoding UTF-8 -cp "$ROOT/lib/*" -d "$ROOT/WM_FO/out" "$ROOT"/WM_FO/src/*.java
echo "Compilando WM_WS..."
javac -encoding UTF-8 -d "$ROOT/WM_WS/bin" "$ROOT"/WM_WS/src/*.java
echo "OK: WM_Central, WM_FO y WM_WS compilados."
