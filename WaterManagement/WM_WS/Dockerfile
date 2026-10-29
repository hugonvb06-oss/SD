# WM_WS - estacion de riego (monitor + motor).
# El contexto de compilacion es la carpeta WaterManagement:
#   docker build -f WM_WS/Dockerfile -t wm-ws .
FROM eclipse-temurin:17-jdk-alpine AS build

WORKDIR /app
COPY WM_WS/src ./src
RUN mkdir -p bin && javac -encoding UTF-8 -d bin src/*.java

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app
COPY --from=build /app/bin ./bin
COPY WM_WS/docker-entrypoint.sh /usr/local/bin/wm-ws-entrypoint.sh
RUN chmod +x /usr/local/bin/wm-ws-entrypoint.sh

# Identidad y destino de la estacion. Se pueden sobreescribir en el despliegue.
ENV WM_ROLE=both \
    WM_WS_ID=1 \
    WM_WS_LOCATION="Jardin principal" \
    WM_WS_FLOW=10.0 \
    WM_STATUS_INTERVAL_MS=1000 \
    WM_MONITOR_PORT=6000 \
    WM_MONITOR_HOST=127.0.0.1 \
    WM_CENTRAL_HOST=central \
    WM_CENTRAL_PORT=5000 \
    WM_WS_ALLOWED_IDS="" \
    WM_WS_STATE_DIR=/data

RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 6000

ENTRYPOINT ["/usr/local/bin/wm-ws-entrypoint.sh"]
