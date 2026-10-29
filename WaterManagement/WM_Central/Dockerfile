# WM_Central - servicio central (socket de monitores + Kafka + SQLite).
# Contexto de compilacion: la carpeta WaterManagement
#   docker build -f WM_Central/Dockerfile -t wm-central .
FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /src
COPY WM_Central/pom.xml ./pom.xml
COPY WM_Central/src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:11-jre

WORKDIR /app
COPY --from=build /src/target/wm-central-1.0.0.jar ./wm-central.jar

# Todo es configurable por entorno; no hay IPs ni rutas fijas.
ENV WM_CENTRAL_PORT=5000 \
    WM_KAFKA_HOST=kafka \
    WM_KAFKA_PORT=9092 \
    WM_DB_PATH=/data/watermanagement.db \
    WM_WS_ALLOWED_IDS=""

RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 5000

ENTRYPOINT ["java", "-jar", "/app/wm-central.jar"]
