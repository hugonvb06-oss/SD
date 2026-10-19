
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

/**
 * Aplicación de operario de campo (WM_FO).
 *
 * Requisitos cubiertos:
 * - argumentos: IP del broker, puerto e ID del operario;
 * - solicitud de riego manual para una WS y una duración;
 * - ejecución secuencial de solicitudes desde un fichero;
 * - espera de una conclusión real publicada por WM_Central;
 * - pausa de 4 segundos entre servicios del fichero;
 * - consulta de WS disponibles;
 * - correlación de respuestas, timeout y cierre controlado.
 *
 * El contrato de mensajes Kafka se documenta en ../README.md y debe ser respetado
 * por WM_Central para que la integración distribuida funcione.
 */
public final class WM_FO implements AutoCloseable {

    private static final int MAX_DURATION_SECONDS = 86400;
    private static final long DEFAULT_LIST_TIMEOUT_SECONDS = 15;
    private static final long DEFAULT_MIN_SERVICE_TIMEOUT_SECONDS = 120;
    private static final long EXTRA_SERVICE_TIMEOUT_SECONDS = 120;
    private static final long PAUSE_BETWEEN_FILE_SERVICES_MS = 4000;

    private final String ipBroker;
    private final int puertoBroker;
    private final int idOperador;
    private final Scanner teclado;

    private KafkaOperatorClient kafka;

    private WM_FO(String ipBroker, int puertoBroker, int idOperador) {
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperador = idOperador;
        this.teclado = new Scanner(System.in);
        crearClienteKafka();
    }

    private void crearClienteKafka() {
        String bootstrapServers = formatearBootstrap(ipBroker, puertoBroker);
        kafka = new KafkaOperatorClient(bootstrapServers, idOperador);
    }

    private static String formatearBootstrap(String host, int port) {
        // Kafka espera host:puerto. Los IPv6 literales deben ir entre corchetes.
        if (host.contains(":") && !host.startsWith("[")) {
            return "[" + host + "]:" + port;
        }
        return host + ":" + port;
    }

    public void iniciar() {
        imprimirCabecera();
        if (!conectarKafkaConReintentos()) {
            return;
        }

        boolean salir = false;
        while (!salir) {
            mostrarMenu();
            String opcion = teclado.nextLine().trim();
            switch (opcion) {
                case "1":
                    solicitarServicioManual();
                    break;
                case "2":
                    solicitarServiciosDesdeFichero();
                    break;
                case "3":
                    mostrarWsDisponibles();
                    break;
                case "4":
                    salir = true;
                    System.out.println("Saliendo de WM_FO...");
                    break;
                default:
                    System.out.println("Opción no válida. Selecciona 1, 2, 3 o 4.");
            }
        }
    }

    private void imprimirCabecera() {
        System.out.println("========================================");
        System.out.println("           WM_FO - OPERARIO");
        System.out.println("========================================");
        System.out.println("ID Operario: " + idOperador);
        System.out.println("Broker Kafka: " + formatearBootstrap(ipBroker, puertoBroker));
    }

    private boolean conectarKafkaConReintentos() {
        while (true) {
            try {
                System.out.println("Conectando con Kafka...");
                kafka.start();
                return true;
            } catch (Exception e) {
                System.err.println("No se pudo iniciar la comunicación Kafka: " + e.getMessage());
                System.out.print("Escribe R para reintentar o S para salir: ");
                String opcion = teclado.nextLine().trim();
                if (opcion.equalsIgnoreCase("s")) {
                    return false;
                }

                try {
                    kafka.close();
                } catch (Exception ignored) {
                    // No se permite que un error de limpieza oculte el fallo original.
                }
                crearClienteKafka();
            }
        }
    }

    private void mostrarMenu() {
        System.out.println();
        System.out.println("--------------- MENÚ ---------------");
        System.out.println("1. Solicitar suministro manualmente");
        System.out.println("2. Procesar servicios desde fichero");
        System.out.println("3. Consultar WS disponibles");
        System.out.println("4. Salir");
        System.out.print("Selecciona una opción: ");
    }

    private void solicitarServicioManual() {
        System.out.println();
        System.out.println("===== SOLICITUD DE RIEGO =====");
        int wsId = leerEnteroPositivo("ID de la estación (WS): ", Integer.MAX_VALUE);
        int duracion = leerEnteroPositivo("Duración del riego en segundos (1-86400): ", MAX_DURATION_SECONDS);
        ejecutarServicio(wsId, duracion, "Solicitud manual");
    }

    private void solicitarServiciosDesdeFichero() {
        System.out.print("Ruta del fichero de servicios: ");
        String nombreFichero = teclado.nextLine().trim();
        if (nombreFichero.isEmpty()) {
            System.out.println("No se ha indicado una ruta.");
            return;
        }

        final List<Servicio> servicios;
        try {
            servicios = leerServicios(Paths.get(nombreFichero));
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("No se ha podido cargar el fichero: " + e.getMessage());
            System.out.println("No se ha enviado ninguna solicitud de este fichero.");
            return;
        }

        if (servicios.isEmpty()) {
            System.out.println("El fichero no contiene servicios.");
            return;
        }

        System.out.println("Se han validado " + servicios.size() + " servicios. Comienza el envío secuencial.");
        for (int i = 0; i < servicios.size(); i++) {
            Servicio servicio = servicios.get(i);
            System.out.println();
            System.out.println("========== SERVICIO " + (i + 1) + "/" + servicios.size() + " ==========");
            System.out.println("Fichero, línea " + servicio.linea + ": WS=" + servicio.wsId
                    + ", duración=" + servicio.duracionSegundos + " s");

            Resultado resultado = ejecutarServicio(servicio.wsId, servicio.duracionSegundos,
                    "Servicio de fichero, línea " + servicio.linea);

            if (resultado == Resultado.DESCONOCIDO) {
                System.err.println("Se detiene el procesamiento del fichero porque no se conoce la conclusión "
                        + "del servicio. No se enviarán más solicitudes para evitar solapamientos.");
                return;
            }

            if (i < servicios.size() - 1) {
                System.out.println("Esperando 4 segundos antes del siguiente servicio...");
                try {
                    Thread.sleep(PAUSE_BETWEEN_FILE_SERVICES_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Procesamiento del fichero interrumpido; no se enviarán más servicios.");
                    return;
                }
            }
        }
        System.out.println("Se ha terminado el procesamiento del fichero.");
    }

    /**
     * Formato acordado para el fichero: wsId;duracionSegundos
     * Se admiten líneas vacías y comentarios que comiencen por #.
     * Se valida el fichero completo antes de enviar la primera petición.
     */
    private List<Servicio> leerServicios(Path ruta) throws IOException {
        List<Servicio> servicios = new ArrayList<>();
        int numeroLinea = 0;

        try (BufferedReader lector = Files.newBufferedReader(ruta, StandardCharsets.UTF_8)) {
            String linea;
            while ((linea = lector.readLine()) != null) {
                numeroLinea++;
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) {
                    continue;
                }

                String[] campos = contenido.split(";", -1);
                if (campos.length != 2) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": se esperaba el formato wsId;duracionSegundos.");
                }

                int wsId;
                int duracion;
                try {
                    wsId = Integer.parseInt(campos[0].trim());
                    duracion = Integer.parseInt(campos[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": el ID de WS y la duración deben ser números enteros.");
                }

                if (wsId <= 0) {
                    throw new IllegalArgumentException("Línea " + numeroLinea + ": el ID de WS debe ser positivo.");
                }
                if (duracion <= 0 || duracion > MAX_DURATION_SECONDS) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": la duración debe estar entre 1 y " + MAX_DURATION_SECONDS + " segundos.");
                }
                servicios.add(new Servicio(wsId, duracion, numeroLinea));
            }
        }
        return servicios;
    }

    private Resultado ejecutarServicio(int wsId, int duracionSegundos, String origen) {
        String requestId;
        try {
            System.out.println(origen + ". Operario=" + idOperador + ", WS=" + wsId
                    + ", duración=" + duracionSegundos + " s");
            requestId = kafka.solicitarRiego(wsId, duracionSegundos);
        } catch (Exception e) {
            System.err.println("No se pudo publicar la solicitud: " + e.getMessage());
            System.err.println("No se puede confirmar si CENTRAL recibió la petición. Comprueba el estado "
                    + "del sistema antes de volver a enviar un riego similar.");
            return Resultado.DESCONOCIDO;
        }

        try {
            long timeout = obtenerTimeoutServicio(duracionSegundos);
            System.out.println("Esperando la conclusión de CENTRAL. Request ID: " + requestId);
            long deadline = System.nanoTime() + Duration.ofSeconds(timeout).toNanos();

            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    System.err.println("Timeout: han pasado " + timeout
                            + " segundos sin recibir una conclusión del servicio.");
                    System.err.println("Esto no cancela el riego en CENTRAL/WS; el resultado es desconocido.");
                    return Resultado.DESCONOCIDO;
                }

                Map<String, Object> evento = kafka.esperarEvento(requestId,
                        Duration.ofNanos(Math.min(remaining, Duration.ofSeconds(1).toNanos())));
                if (evento == null) {
                    continue;
                }

                mostrarNotificacion(evento);
                String estado = texto(evento.get("status"));
                if (esEstadoExito(estado)) {
                    System.out.println("Riego finalizado correctamente.");
                    return Resultado.EXITO;
                }
                if (esEstadoFracaso(estado)) {
                    System.out.println("El servicio ha concluido sin éxito (" + estado + ").");
                    return Resultado.FRACASO;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Espera interrumpida. No se conoce el resultado final del servicio.");
            return Resultado.DESCONOCIDO;
        } catch (IOException | IllegalStateException e) {
            System.err.println("Se ha perdido la capacidad de recibir el resultado: " + e.getMessage());
            return Resultado.DESCONOCIDO;
        } finally {
            kafka.finalizarSolicitud(requestId);
        }
    }

    private long obtenerTimeoutServicio(int duracionSegundos) {
        long porDefecto = Math.max(DEFAULT_MIN_SERVICE_TIMEOUT_SECONDS,
                (long) duracionSegundos + EXTRA_SERVICE_TIMEOUT_SECONDS);
        String variable = System.getenv("WM_FO_TIMEOUT_SECONDS");
        if (variable == null || variable.trim().isEmpty()) {
            return porDefecto;
        }
        try {
            long configurado = Long.parseLong(variable.trim());
            if (configurado <= 0) {
                System.err.println("WM_FO_TIMEOUT_SECONDS debe ser positivo; se usará " + porDefecto + " s.");
                return porDefecto;
            }
            return configurado;
        } catch (NumberFormatException e) {
            System.err.println("WM_FO_TIMEOUT_SECONDS no es válido; se usará " + porDefecto + " s.");
            return porDefecto;
        }
    }

    private void mostrarNotificacion(Map<String, Object> evento) {
        String estado = texto(evento.get("status"));
        String mensaje = texto(evento.get("message"));
        if (estado != null && !estado.isEmpty()) {
            System.out.print("[CENTRAL] " + estado);
        } else {
            System.out.print("[CENTRAL] Evento recibido");
        }
        if (mensaje != null && !mensaje.isEmpty()) {
            System.out.print(" — " + mensaje);
        }
        System.out.println();

        Object caudal = evento.get("flowLMin");
        Object volumen = evento.get("volumeLitres");
        Object duracion = evento.get("elapsedSeconds");
        if (caudal != null || volumen != null || duracion != null) {
            System.out.print("  Telemetría");
            if (caudal != null) {
                System.out.print(" | Caudal: " + decimal(caudal) + " L/min");
            }
            if (volumen != null) {
                System.out.print(" | Volumen acumulado: " + decimal(volumen) + " L");
            }
            if (duracion != null) {
                System.out.print(" | Tiempo: " + texto(duracion) + " s");
            }
            System.out.println();
        }
    }

    private void mostrarWsDisponibles() {
        String requestId;
        try {
            requestId = kafka.solicitarListadoWsDisponibles();
        } catch (Exception e) {
            System.err.println("No se pudo solicitar el listado a CENTRAL: " + e.getMessage());
            return;
        }

        try {
            System.out.println("Consultando las estaciones disponibles...");
            long deadline = System.nanoTime() + Duration.ofSeconds(DEFAULT_LIST_TIMEOUT_SECONDS).toNanos();
            while (System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                Map<String, Object> evento = kafka.esperarEvento(requestId,
                        Duration.ofNanos(Math.min(remaining, Duration.ofSeconds(1).toNanos())));
                if (evento == null) {
                    continue;
                }

                String tipo = texto(evento.get("type"));
                if ("AVAILABLE_WS_LIST".equalsIgnoreCase(tipo)
                        || evento.get("stations") instanceof List<?>) {
                    mostrarListaWs(evento);
                    return;
                }
                mostrarNotificacion(evento);
                String estado = texto(evento.get("status"));
                if (esEstadoFracaso(estado)) {
                    System.out.println("CENTRAL no ha podido proporcionar el listado.");
                    return;
                }
            }
            System.err.println("CENTRAL no respondió al listado en " + DEFAULT_LIST_TIMEOUT_SECONDS + " segundos.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Consulta de WS interrumpida.");
        } catch (IOException | IllegalStateException e) {
            System.err.println("No se pudo recibir el listado: " + e.getMessage());
        } finally {
            kafka.finalizarSolicitud(requestId);
        }
    }

    private void mostrarListaWs(Map<String, Object> evento) {
        Object objetoLista = evento.get("stations");
        if (!(objetoLista instanceof List<?>)) {
            System.out.println("CENTRAL respondió sin una lista de estaciones.");
            return;
        }

        List<?> estaciones = (List<?>) objetoLista;
        int mostradas = 0;
        System.out.println();
        System.out.println("========== WS DISPONIBLES ==========");
        System.out.printf("%-8s %-32s %-18s%n", "ID WS", "Ubicación", "Estado");
        System.out.println("------------------------------------------------------------");

        for (Object elemento : estaciones) {
            if (!(elemento instanceof Map<?, ?>)) {
                continue;
            }
            Map<?, ?> estacion = (Map<?, ?>) elemento;
            String estado = texto(estacion.get("state"));
            Object availableValue = estacion.get("available");
            boolean disponible = Boolean.TRUE.equals(availableValue)
                    || "ACTIVADA".equalsIgnoreCase(estado)
                    || "AVAILABLE".equalsIgnoreCase(estado);
            // La respuesta de esta consulta debe incluir únicamente estaciones disponibles.
            if (availableValue != null && !Boolean.TRUE.equals(availableValue)) {
                continue;
            }
            if (availableValue == null && estado != null && !disponible) {
                continue;
            }

            String id = texto(estacion.containsKey("wsId") ? estacion.get("wsId") : estacion.get("id"));
            String ubicacion = texto(estacion.get("location"));
            if (id == null) id = "?";
            if (ubicacion == null) ubicacion = "Sin ubicación";
            if (estado == null) estado = "ACTIVADA";
            System.out.printf(Locale.ROOT, "%-8s %-32s %-18s%n", acortar(id, 8), acortar(ubicacion, 32), estado);
            mostradas++;
        }

        if (mostradas == 0) {
            System.out.println("No hay estaciones disponibles en este momento.");
        }
        System.out.println("====================================");
    }

    private int leerEnteroPositivo(String mensaje, int maximo) {
        while (true) {
            System.out.print(mensaje);
            String entrada = teclado.nextLine().trim();
            try {
                int valor = Integer.parseInt(entrada);
                if (valor > 0 && valor <= maximo) {
                    return valor;
                }
            } catch (NumberFormatException ignored) {
                // Se vuelve a solicitar el dato sin terminar la aplicación.
            }
            System.out.println("Introduce un número entero entre 1 y " + maximo + ".");
        }
    }

    private static boolean esEstadoExito(String estado) {
        String normalizado = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return normalizado.equals("COMPLETED") || normalizado.equals("COMPLETADO")
                || normalizado.equals("FINALIZADO") || normalizado.equals("SUCCESS")
                || normalizado.equals("EXITO") || normalizado.equals("ÉXITO");
    }

    private static boolean esEstadoFracaso(String estado) {
        String normalizado = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return normalizado.equals("REJECTED") || normalizado.equals("RECHAZADO")
                || normalizado.equals("FAILED") || normalizado.equals("FALLO")
                || normalizado.equals("ERROR") || normalizado.equals("CANCELLED")
                || normalizado.equals("CANCELADO") || normalizado.equals("FRACASO")
                || normalizado.equals("DENEGADO") || normalizado.equals("RECHAZADA");
    }

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static String decimal(Object valor) {
        try {
            double numero = valor instanceof Number
                    ? ((Number) valor).doubleValue()
                    : Double.parseDouble(String.valueOf(valor));
            return String.format(Locale.ROOT, "%.2f", numero);
        } catch (Exception e) {
            return String.valueOf(valor);
        }
    }

    private static String acortar(String texto, int maximo) {
        if (texto.length() <= maximo) return texto;
        return texto.substring(0, Math.max(0, maximo - 3)) + "...";
    }

    @Override
    public void close() {
        if (kafka != null) {
            kafka.close();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java WM_FO <IP_BROKER> <PUERTO_BROKER> <ID_OPERARIO>");
            System.out.println("Ejemplo local: java WM_FO localhost 9092 1");
            return;
        }

        String ipBroker = args[0].trim();
        int puertoBroker;
        int idOperador;
        try {
            puertoBroker = Integer.parseInt(args[1]);
            idOperador = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("El puerto del broker y el ID del operario deben ser números enteros.");
            return;
        }

        if (ipBroker.isEmpty() || puertoBroker < 1 || puertoBroker > 65535 || idOperador <= 0) {
            System.err.println("Argumentos inválidos. La IP no puede estar vacía, el puerto debe estar entre 1 y 65535 y el ID debe ser positivo.");
            return;
        }

        try (WM_FO operador = new WM_FO(ipBroker, puertoBroker, idOperador)) {
            operador.iniciar();
        } catch (Exception e) {
            System.err.println("WM_FO se ha cerrado por un error: " + e.getMessage());
        }
    }

    private enum Resultado {
        EXITO,
        FRACASO,
        DESCONOCIDO
    }

    private static final class Servicio {
        private final int wsId;
        private final int duracionSegundos;
        private final int linea;

        private Servicio(int wsId, int duracionSegundos, int linea) {
            this.wsId = wsId;
            this.duracionSegundos = duracionSegundos;
            this.linea = linea;
        }
    }
}
