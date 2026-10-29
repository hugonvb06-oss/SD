# Water Management (SD 26/27 - Release 1)

Sistema de gestión de riego inteligente distribuido en tres procesos independientes:

- **WM_Central**: servicio central. Escucha en un socket el protocolo de monitores
  (`WM_Central`), valida y persiste las solicitudes de riego enviadas por el operario
  (WM_FO) a través de Kafka, y publica los eventos de resultado en el topic de respuestas.

- **WM_WS**: estación de agua compuesta por dos procesos.
  - **WM_WS_M (monitor)**: se autentica contra WM_Central (`ENQ`/`ACK` + `HELLO`), envía
    telemetría (`STATUS`) cada segundo y recibe órdenes (`CMD`) incorporadas por WM_Central
    en la respuesta a la telemetría.
  - **WM_WS_E (motor)**: escucha en un puerto propio, ejecuta el riego simulado,
    conserva el volumen acumulado y persiste el resultado final pendiente de confirmación.

- **WM_FO**: operador de campo. Se conecta a Kafka, consulta las estaciones disponibles,
  publica solicitudes de riego y consume los eventos de resultado, avisando cuando
  WM_Central deja de enviar latidos.

## Funcionalidad verificada

- Autenticación y aceptación/rechazo de monitores contra WM_Central (`WM_WS_ALLOWED_IDS`,
  `WM_WS_REQUIRE_KNOWN`).
- Estados de la estación con etiquetas y colores del panel: **Available** (verde),
  **Watering** (verde intermitente, con caudal, litros del servicio y operario), **Leak**
  (rojo), **Out of Service** (naranja, texto literal) y **Disconnected** (gris).
- Flujo completo de un riego: `CHECKING` → `AUTHORIZED` → `STARTED` → `PROGRESS` → `COMPLETED`
  (o `FAILED` / `REJECTED`).
- Bloqueo y activación desde WM_Central; aborto automático del riego en caso de fuga.
- Latido de WM_Central a `wm.central.latido`; WM_FO avisa al operario si deja de llegar.
- Persistencia del motor (`WM_WS_STATE_DIR`) y del acumulado de litros; el volumen
  suministrado nunca disminuye.
- Recuperación ante caídas: monitor, motor, CENTRAL, broker y operador.

## Protocolo

- Control: `ENQ` → `ACK`/`NACK` → respuesta.
- Datos: trama `STX ... ETX LRC` para telemetría (`STATUS`) y órdenes (`CMD`).
- La respuesta a cada `STATUS` llega como `ACK#STATUS#OK#<JSON con #CMD#...>`.
- El motor confirma mediante `POLL#1#<requestId>` / respuesta `STATUS` (petición-respuesta,
  sin ACK duplicado).
- Las órdenes se anexan a la respuesta del estado como `ACK#STATUS#OK#CMD#TIPO|requestId|operario|duracion#...`.

## Configuración

### WM_Central

```
java ServidorCentral [puertoSocket] [ipBroker] [puertoBroker] [rutaSQLite]
```

Variables de entorno: `WM_CENTRAL_PORT` (5000), `WM_KAFKA_HOST` (localhost), `WM_KAFKA_PORT`
(9092), `WM_DB_PATH` (database/watermanagement.db), `WM_WS_ALLOWED_IDS`.

### WM_WS

```
java WM_WS_M <puertoMotor> <ipCentral> <puertoCentral> <idWS> <ubicacion> <caudalLMin> [intervaloMs]
java WM_WS_E <ipMonitor> <puertoMonitor> [dirEstado]
```

Variables de entorno: `WM_MONITOR_PORT` (6000), `WM_CENTRAL_HOST` (localhost), `WM_CENTRAL_PORT`
(5000), `WM_WS_ID` (1), `WM_WS_LOCATION`, `WM_WS_FLOW`, `WM_STATUS_INTERVAL_MS`,
`WM_MONITOR_HOST`, `WM_WS_STATE_DIR`.

### WM_FO

```
java WM_FO [ipBroker] [puertoBroker] [idOperario]
```

Variables de entorno: `WM_KAFKA_HOST` (localhost), `WM_KAFKA_PORT` (9092), `WM_FO_ID` (1),
`WM_FO_MANAGE_TOPICS`, `WM_FO_TIMEOUT_SECONDS`.

## Ejecución

### Requisitos

- JDK 11+ (recomendado 17) y Maven o `javac` con las librerías de Kafka/Jackson/SQLite
  disponibles en `WaterManagement/lib` o descargadas.

### 1. Compilar

```bash
cd WaterManagement
javac --release 11 -encoding UTF-8 -cp "lib/*" -d WM_Central/out WM_Central/src/*.java
javac --release 11 -encoding UTF-8 -cp "lib/*" -d WM_FO/out WM_FO/src/*.java
javac -encoding UTF-8 -d WM_WS/bin WM_WS/src/*.java
```

### 2. Desplegar sin Docker

```bash
# Kafka (KRaft, local) -> ver scripts/docker o descarga de apache/kafka
java -cp "WM_Central/out:lib/*" ServidorCentral 5000 localhost 9092
java -cp "WM_WS/bin" WM_WS_E localhost 16000 ./state
java -cp "WM_WS/bin" WM_WS_M 16000 localhost 5000 1 "Jardin principal" 12.0
java -cp "WM_FO/out:lib/*" WM_FO localhost 9092 1
```

### 3. Despliegue con Docker

```bash
cd WaterManagement
docker build -f WM_Central/Dockerfile -t wm-central .
docker build -f WM_WS/Dockerfile -t wm-ws .
docker build -f WM_FO/Dockerfile -t wm-fo .
docker compose up -d --build
docker compose run --rm fo
```

La estación se ejecuta con `WM_ROLE=both` (monitor + motor), `WM_ROLE=monitor` o
`WM_ROLE=engine`. Para varios WS, duplique los servicios y ajuste `WM_WS_ID`.

## Estructura del repositorio

```
WaterManagement/
  WM_Central/    # central + panel + admin
  WM_WS/         # monitor + motor
  WM_FO/         # operador
  prompts/       # prompts de la practica
  Readme.md      # este documento
  Nombres.txt    # autores
  docker/        # docker-compose.yml
  lib/           # librerias de terceros usadas en compilacion local
  database/      # SQLite
```

## Pruebas realizadas

1. Integracion WM_FO -> Kafka -> WM_Central -> WM_WS -> WM_WS_E completo (riego 10 s:
   AVAILABLE -> Watering -> STARTED/PROGRESS -> COMPLETED con volumen de servicio).
2. Latido: la caida de WM_Central se detecta en WM_FO ("Imposible conectar con
   WM_Central").
3. Resiliencia: monitor caido -> Disconnected; motor KO -> Leak y aborto del riego;
   recuperacion -> Available con volumen conservado; bloqueo -> Out of Service; activacion.
4. Caida del motor durante un riego -> fuga -> reinicio conserva el volumen acumulado.
