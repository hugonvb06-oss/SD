# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests clean package

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/wm-fo.jar /app/wm-fo.jar
ENTRYPOINT ["java", "-jar", "/app/wm-fo.jar"]
