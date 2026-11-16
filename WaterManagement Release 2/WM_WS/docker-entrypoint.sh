#!/bin/sh
# Arranca el monitor (WM_WS_M) y/o el motor (WM_WS_E) segun WM_ROLE:
#   monitor -> solo el monitor (se conecta a WM_Central)
#   engine  -> solo el motor (se conecta al monitor indicado por WM_MONITOR_HOST)
#   both    -> monitor y motor en el mismo contenedor (por defecto)
set -eu

ROLE="${WM_ROLE:-both}"
PORT="${WM_MONITOR_PORT:-6000}"
STATE_DIR="${WM_WS_STATE_DIR:-/data}"
MONITOR_HOST="${WM_MONITOR_HOST:-127.0.0.1}"

mkdir -p "$STATE_DIR"

case "$ROLE" in
    monitor)
        exec java -cp /app/bin WM_WS_M
        ;;
    engine)
        exec java -cp /app/bin WM_WS_E "$MONITOR_HOST" "$PORT" "$STATE_DIR"
        ;;
    both)
        # El motor se conecta al monitor por loopback dentro del contenedor.
        java -cp /app/bin WM_WS_E 127.0.0.1 "$PORT" "$STATE_DIR" &
        ENGINE_PID=$!
        java -cp /app/bin WM_WS_M &
        MONITOR_PID=$!

        terminar() {
            kill -TERM "$MONITOR_PID" "$ENGINE_PID" 2>/dev/null || true
            wait "$MONITOR_PID" "$ENGINE_PID" 2>/dev/null || true
            exit 0
        }
        trap terminar TERM INT

        # Si cualquiera de los dos procesos termina, se cierra el contenedor para
        # que la politica de reinicio lo recupere. Para reinicios independientes
        # (requisito de resiliencia) ejecute dos contenedores con WM_ROLE=monitor
        # y WM_ROLE=engine.
        while kill -0 "$MONITOR_PID" 2>/dev/null && kill -0 "$ENGINE_PID" 2>/dev/null; do
            sleep 1
        done

        kill -TERM "$MONITOR_PID" "$ENGINE_PID" 2>/dev/null || true
        wait "$MONITOR_PID" "$ENGINE_PID" 2>/dev/null || true
        echo "WM_WS: uno de los procesos termino; se cierra el contenedor." >&2
        exit 1
        ;;
    *)
        echo "WM_ROLE no valido: '$ROLE' (use monitor, engine o both)." >&2
        exit 2
        ;;
esac
