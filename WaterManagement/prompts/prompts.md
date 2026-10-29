# Prompts de la práctica - Water Management

Esta carpeta recoge las instrucciones de usuario (prompts) que orientaron el desarrollo del sistema, así como la formulación funcional del Proyecto de Desarrollo (SD) relativo a *Water Management*.

## SD 26/27 - Release 1 (Water Management)

### Funcionalidad

- El operador de campo (WM_FO) debe poder solicitar el suministro de agua de forma manual
  a una estación de riego (WM_WS), recibiendo el resultado del servicio en tiempo real.

- WM_Central debe gestionar las solicitudes entrantes desde el broker de Kafka, las
  validar y delegarlas al monitor de una estación conectado.

- El monitor (WM_WS) se autentica contra WM_Central, envía telemetría y recibe órdenes
  de riego, bloqueo y activación.

- El motor (WM_WS_E) ejecuta el riego simulado y notifica el estado: disponible,
  regando, fuga, fuera de servicio o desconectado.

- El operador (WM_FO) debe conocer las estaciones disponibles y la evolución del
  servicio solicitado.

### Requisitos de no funcionalidad

- Los módulos deben estar desplegados como procesos independientes y ejecutarse sin IDE.

- Todas las configuraciones (puertos, hosts, IDs de estaciones y operadores, duraciones,
  límites de seguridad) deben ser configurables mediante argumentos o variables de entorno.

- El sistema debe tolerar fallos aislados: caídas del monitor, del motor, de CENTRAL,
  de Kafka o del operador. Tras una recuperación, el estado final debe reportarse y el
  volumen realmente suministrado debe conservarse.

- El protocolo de transporte entre WM_Central y WM_WS debe usar tramas de control
  (`ENQ`/`ACK`/`NACK`), tramas de datos (`STX ... ETX LRC`) y anexión de órdenes en la
  respuesta de telemetría.

### Interfaz WM_FO (operario)

1. Solicitar suministro manualmente.
2. Procesar servicios desde fichero.
3. Consultar WS disponibles.
4. Salir.

Los eventos recibidos por el operador corresponden únicamente a sus propias estaciones:
si una estación finaliza un servicio solicitado por otra identidad, no se muestra.

### Notas de integración

- Tópicos Kafka: `wm.riego.solicitudes`, `wm.riego.respuestas`, `wm.central.latido`.
- Los resultados del servicio llegan a WM_FO como eventos de estado con `type`:
  `RECEIVED`, `CHECKING`, `AUTHORIZED`, `STARTED`, `PROGRESS`, `COMPLETED`, `FAILED`, `REJECTED`.

- La ausencia de latidos de la topic `wm.central.latido` por encima de 8 segundos indica
  que WM_Central no está activo.
