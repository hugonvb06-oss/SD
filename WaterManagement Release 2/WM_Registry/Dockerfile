FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /src
COPY pom.xml ./pom.xml
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /src/target/wm-registry-1.0.0.jar ./wm-registry.jar
ENV WM_REGISTRY_HOST=0.0.0.0 \
    WM_REGISTRY_PORT=8443 \
    WM_DB_PATH=/data/watermanagement.db \
    WM_REGISTRY_KEYSTORE=/run/secrets/registry.p12
VOLUME ["/data"]
EXPOSE 8443
ENTRYPOINT ["java", "-jar", "/app/wm-registry.jar"]
