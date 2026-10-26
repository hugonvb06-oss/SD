# --- ETAPA 1: Compilación ---
FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

# Copiar las librerías (.jar de Kafka, SQLite, etc.)
COPY WaterManagement/lib/ ./lib/

# Copiar el código fuente de WM_WS
COPY WaterManagement/WM_WS/src/ ./src/

# Crear la carpeta para los archivos compilados (.class)
RUN mkdir -p bin

# Compilar todos los archivos .java incluyendo las librerías del classpath
RUN javac -cp "lib/*:." -d bin src/*.java

# --- ETAPA 2: Ejecución ---
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copiar las librerías y los .class compilados desde la etapa de compilación
COPY --from=builder /app/lib ./lib
COPY --from=builder /app/bin ./bin

# Comando para ejecutar la clase principal
CMD ["java", "-cp", "lib/*:bin", "WM_WS_E"]