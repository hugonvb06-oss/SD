#!/bin/bash
# Uso: ./scripts/run.sh central  [portSocket] [ipBroker] [portBroker] [rutaBD]
#      ./scripts/run.sh fo     [ipBroker] [portBroker] [idOperario]
#      ./scripts/run.sh ws-mon [idWS] [ubicacion] [caudalLMin] [portMonitor] [ipCentral] [portCentral]
#      ./scripts/run.sh ws-eng [ipMonitor] [portMonitor] [dirEstado]
#      ./scripts/run.sh all     # imprime ayuda
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ "$#" -lt 1 ]; then
  echo "Uso: $0 central|fo|ws-mon|ws-eng all"
  exit 2
fi

case "$1" in
  central)
    shift
    exec java -cp "$ROOT/WM_Central/out:$ROOT/lib/*" ServidorCentral "$@"
    ;;
  fo)
    shift
    exec java -cp "$ROOT/WM_FO/out:$ROOT/lib/*" WM_FO "$@"
    ;;
  ws-mon)
    shift
    exec java -cp "$ROOT/WM_WS/bin" WM_WS_M "$@"
    ;;
  ws-eng)
    shift
    exec java -cp "$ROOT/WM_WS/bin" WM_WS_E "$@"
    ;;
  all)
    echo "Este script arranca el modulo indicado. Veje:"
    echo "  ./scripts/run.sh central   java ServidorCentral [puertoSocket] [ipBroker] [puertoBroker] [rutaBD]"
    echo "  ./scripts/run.sh fo        java WM_FO [ipBroker] [puertoBroker] [idOperario]"
    echo "  ./scripts/run.sh ws-mon    java WM_WS_M <puertoMotor> <ipCentral> <puertoCentral> <idWS> <ubicacion> <caudalLMin> [intervaloMs]"
    echo "  ./scripts/run.sh ws-eng    java WM_WS_E <ipMonitor> <puertoMonitor> [dirEstado]"
    ;;
  *)
    echo "Modulo desconocido: $1" >&2
    exit 2
    ;;
esac
