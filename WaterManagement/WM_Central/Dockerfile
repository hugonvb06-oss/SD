
FROM eclipse-temurin:11-jdk

WORKDIR /app/WM_Central/src

COPY WM_Central/src/ ./
COPY lib/sqlite-jdbc-3.53.4.0.jar ../../lib/sqlite-jdbc-3.53.4.0.jar

RUN javac -cp ".:../../lib/sqlite-jdbc-3.53.4.0.jar" \
    BaseDatos.java HiloCentral.java ServidorCentral.java

EXPOSE 5000

CMD ["java", "-cp", ".:../../lib/sqlite-jdbc-3.53.4.0.jar", "ServidorCentral"]
