
FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /app/WM_Central

COPY WM_Central/pom.xml ./pom.xml
COPY WM_Central/src/ ./src/

RUN mvn -B clean package

FROM eclipse-temurin:11-jre

WORKDIR /app/WM_Central/src

COPY --from=build /app/WM_Central/target/wm-central-1.0.0.jar ./wm-central.jar

EXPOSE 5000

CMD ["java", "-jar", "wm-central.jar", "5000", "kafka", "9092"]
