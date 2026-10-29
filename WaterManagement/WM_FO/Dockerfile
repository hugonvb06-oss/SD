# WM_FO - aplicacion de operario de campo (interactiva).
# Contexto de compilacion: la carpeta WaterManagement
#   docker build -f WM_FO/Dockerfile -t wm-fo .
#
# Para usarla hacen falta stdin y TTY:
#   docker run -it --rm -e WM_KAFKA_HOST=central -e WM_FO_ID=7 wm-fo
FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /src
COPY WM_FO/pom.xml ./pom.xml
COPY WM_FO/src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /src/target/wm-fo.jar ./wm-fo.jar

ENV WM_KAFKA_HOST=kafka \
    WM_KAFKA_PORT=9092 \
    WM_FO_ID=1 \
    WM_FO_MANAGE_TOPICS=true

ENTRYPOINT ["java", "-jar", "/app/wm-fo.jar"]
