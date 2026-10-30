
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
import javax.swing.SwingUtilities;

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
 * - recuperación de servicios pendientes tras cerrarse durante un riego;
 * - correlación de respuestas, timeout y cierre controlado.
 *
 * El contrato de mensajes Kafka se documenta en ../README.md y debe ser respetado
 * por WM_Central para que la integración distribuida funcione.
 */
public final class WM_FO implements AutoCloseable {

    private static final int DURACION_MAXIMA_SEGUNDOS = 86400;
    private static final long TIMEOUT_LISTADO_SEGUNDOS = 15;
    private static final long TIMEOUT_MINIMO_SERVICIO_SEGUNDOS = 120;
    private static final long SEGUNDOS_EXTRA_SERVICIO = 120;
    private static final long PAUSA_ENTRE_SERVICIOS_MS = 4000;
    private static final long TIMEOUT_RECUPERACION_SEGUNDOS = 30;

    private final String ipBroker;
    private final int puertoBroker;
    private final int idOperario;
    private final Scanner teclado;

    private ClienteKafkaOperario kafka;
    private Boolean ultimoEstadoCentral;

    private WM_FO(String ipBroker, int puertoBroker, int idOperario) {
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperario = idOperario;
        this.teclado = new Scanner(System.in);
        crearClienteKafka();
    }

    private void crearClienteKafka() {
        String servidoresKafka = formatearBootstrap(ipBroker, puertoBroker);
        kafka = new ClienteKafkaOperario(servidoresKafka, idOperario);
    }

    private static String formatearBootstrap(String anfitrion, int puerto) {
        // Kafka espera host:puerto. Los IPv6 literales deben ir entre corchetes.
        if (anfitrion.contains(":") && !anfitrion.startsWith("[")) {
            return "[" + anfitrion + "]:" + puerto;
        }
        return anfitrion + ":" + puerto;
    }

    public void iniciar() {
        imprimirCabecera();
        if (!conectarKafkaConReintentos()) {
            return;
        }
        recuperarServiciosPendientes();

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
        System.out.println("ID Operario: " + idOperario);
        System.out.println("Broker Kafka: " + formatearBootstrap(ipBroker, puertoBroker));
    }

    private boolean conectarKafkaConReintentos() {
        while (true) {
            try {
                System.out.println("Conectando con Kafka...");
                kafka.iniciar();
                return true;
            } catch (Exception e) {
                System.err.println("No se pudo iniciar la comunicación Kafka: " + e.getMessage());
                System.out.print("Escribe R para reintentar o S para salir: ");
                String opcion = teclado.nextLine().trim();
                if (opcion.equalsIgnoreCase("s")) {
                    return false;
                }

                try {
                    kafka.cerrar();
                } catch (Exception ignorada) {
                    // No se permite que un error de limpieza oculte el fallo original.
                }
                crearClienteKafka();
            }
        }
    }

    /** Recupera el estado de los riegos que quedaron pendientes de una ejecución anterior. */
    private void recuperarServiciosPendientes() {
        List<ClienteKafkaOperario.Pendiente> pendientes = kafka.cargarPendientes();
        if (pendientes.isEmpty()) {
            return;
        }
        System.out.println("Se han encontrado " + pendientes.size()
                + " servicios pendientes de sesiones anteriores. Consultando su estado...");
        for (ClienteKafkaOperario.Pendiente pendiente : pendientes) {
            try {
                kafka.solicitarEstado(pendiente.idSolicitud);
            } catch (Exception e) {
                System.err.println("No se pudo preguntar por " + pendiente.idSolicitud + ": " + e.getMessage());
                continue;
            }
            try {
                long limite = System.nanoTime()
                        + Duration.ofSeconds(TIMEOUT_RECUPERACION_SEGUNDOS).toNanos();
                boolean resuelto = false;
                while (System.nanoTime() < limite) {
                    long restante = limite - System.nanoTime();
                    Map<String, Object> evento = kafka.esperarEvento(pendiente.idSolicitud,
                            Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                    if (evento == null) {
                        continue;
                    }
                    System.out.print("[RECUPERADO] ");
                    mostrarNotificacion(evento);
                    String estado = texto(evento.get("status"));
                    if (esEstadoExito(estado) || esEstadoFracaso(estado)) {
                        kafka.resolverPendiente(pendiente.idSolicitud);
                        resuelto = true;
                        break;
                    }
                }
                if (!resuelto) {
                    System.err.println("No se conoce aún la conclusión de " + pendiente.idSolicitud
                            + "; seguirá pendiente para la próxima ejecución.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Recuperación de " + pendiente.idSolicitud + " interrumpida.");
            } catch (IOException | IllegalStateException e) {
                System.err.println("No se pudo recuperar " + pendiente.idSolicitud + ": " + e.getMessage());
            } finally {
                kafka.finalizarSolicitud(pendiente.idSolicitud);
            }
        }
    }

    private void mostrarMenu() {
        avisarCentral();
        System.out.println();
        System.out.println("--------------- MENÚ ---------------");
        System.out.println("1. Solicitar suministro manualmente");
        System.out.println("2. Procesar servicios desde fichero");
        System.out.println("3. Consultar WS disponibles");
        System.out.println("4. Salir");
        System.out.print("Selecciona una opción: ");
    }

    /** Avisa al operario cuando WM_Central deja de publicar latidos o se recupera. */
    private void avisarCentral() {
        if (kafka == null) return;
        boolean vivo = kafka.centralViva();
        if (ultimoEstadoCentral == null) {
            ultimoEstadoCentral = vivo;
            if (!vivo) {
                System.err.println("[CENTRAL] Todavía no se reciben latidos de WM_Central. "
                        + "Si la central no está desplegada, no se podrán atender solicitudes.");
            }
            return;
        }
        if (vivo != ultimoEstadoCentral.booleanValue()) {
            ultimoEstadoCentral = vivo;
            if (vivo) System.out.println("[CENTRAL] Conexión con WM_Central restablecida.");
            else System.err.println("Imposible conectar con WM_Central: no se reciben latidos de la central.");
        }
    }

    private void solicitarServicioManual() {
        System.out.println();
        System.out.println("===== SOLICITUD DE RIEGO =====");
        int idWs = leerEnteroPositivo("ID de la estación (WS): ", Integer.MAX_VALUE);
        int duracion = leerEnteroPositivo("Duración del riego en segundos (1-86400): ", DURACION_MAXIMA_SEGUNDOS);
        ejecutarServicio(idWs, duracion, "Solicitud manual");
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
            System.out.println("Fichero, línea " + servicio.linea + ": WS=" + servicio.idWs
                    + ", duración=" + servicio.duracionSegundos + " s");

            Resultado resultado = ejecutarServicio(servicio.idWs, servicio.duracionSegundos,
                    "Servicio de fichero, línea " + servicio.linea);

            if (resultado == Resultado.DESCONOCIDO) {
                System.err.println("Se detiene el procesamiento del fichero porque no se conoce la conclusión "
                        + "del servicio. No se enviarán más solicitudes para evitar solapamientos.");
                return;
            }

            if (i < servicios.size() - 1) {
                System.out.println("Esperando 4 segundos antes del siguiente servicio...");
                try {
                    Thread.sleep(PAUSA_ENTRE_SERVICIOS_MS);
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

                int idWs;
                int duracion;
                try {
                    idWs = Integer.parseInt(campos[0].trim());
                    duracion = Integer.parseInt(campos[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": el ID de WS y la duración deben ser números enteros.");
                }

                if (idWs <= 0) {
                    throw new IllegalArgumentException("Línea " + numeroLinea + ": el ID de WS debe ser positivo.");
                }
                if (duracion <= 0 || duracion > DURACION_MAXIMA_SEGUNDOS) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": la duración debe estar entre 1 y " + DURACION_MAXIMA_SEGUNDOS + " segundos.");
                }
                servicios.add(new Servicio(idWs, duracion, numeroLinea));
            }
        }
        return servicios;
    }

    private Resultado ejecutarServicio(int idWs, int duracionSegundos, String origen) {
        String idSolicitud;
        try {
            System.out.println(origen + ". Operario=" + idOperario + ", WS=" + idWs
                    + ", duración=" + duracionSegundos + " s");
            idSolicitud = kafka.solicitarRiego(idWs, duracionSegundos);
        } catch (Exception e) {
            System.err.println("No se pudo publicar la solicitud: " + e.getMessage());
            System.err.println("No se puede confirmar si CENTRAL recibió la petición. Comprueba el estado "
                    + "del sistema antes de volver a enviar un riego similar.");
            return Resultado.DESCONOCIDO;
        }

        try {
            long timeout = obtenerTimeoutServicio(duracionSegundos);
            System.out.println("Esperando la conclusión de CENTRAL. Request ID: " + idSolicitud);
            long limite = System.nanoTime() + Duration.ofSeconds(timeout).toNanos();

            while (true) {
                long restante = limite - System.nanoTime();
                if (restante <= 0) {
                    System.err.println("Timeout: han pasado " + timeout
                            + " segundos sin recibir una conclusión del servicio.");
                    System.err.println("Esto no cancela el riego en CENTRAL/WS; el resultado es desconocido.");
                    avisarCentral();
                    return Resultado.DESCONOCIDO;
                }

                Map<String, Object> evento = kafka.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                avisarCentral();
                if (evento == null) {
                    continue;
                }

                mostrarNotificacion(evento);
                String estado = texto(evento.get("status"));
                if (esEstadoExito(estado)) {
                    System.out.println("Riego finalizado correctamente.");
                    kafka.resolverPendiente(idSolicitud);
                    return Resultado.EXITO;
                }
                if (esEstadoFracaso(estado)) {
                    System.out.println("El servicio ha concluido sin éxito (" + estado + ").");
                    kafka.resolverPendiente(idSolicitud);
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
            kafka.finalizarSolicitud(idSolicitud);
        }
    }

    private long obtenerTimeoutServicio(int duracionSegundos) {
        long porDefecto = Math.max(TIMEOUT_MINIMO_SERVICIO_SEGUNDOS,
                (long) duracionSegundos + SEGUNDOS_EXTRA_SERVICIO);
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
        Object consumoServicio = evento.get("serviceVolumeLitres");
        Object duracion = evento.get("elapsedSeconds");
        if (caudal != null || volumen != null || consumoServicio != null || duracion != null) {
            System.out.print("  Telemetría");
            if (caudal != null) {
                System.out.print(" | Caudal: " + decimal(caudal) + " L/min");
            }
            if (volumen != null) {
                System.out.print(" | Volumen acumulado: " + decimal(volumen) + " L");
            }
            if (consumoServicio != null) {
                System.out.print(" | Consumo del servicio: " + decimal(consumoServicio) + " L");
            }
            if (duracion != null) {
                System.out.print(" | Tiempo: " + texto(duracion) + " s");
            }
            System.out.println();
        }
    }

    private void mostrarWsDisponibles() {
        String idSolicitud;
        try {
            idSolicitud = kafka.solicitarListadoWsDisponibles();
        } catch (Exception e) {
            System.err.println("No se pudo solicitar el listado a CENTRAL: " + e.getMessage());
            return;
        }

        try {
            System.out.println("Consultando las estaciones disponibles...");
            long limite = System.nanoTime() + Duration.ofSeconds(TIMEOUT_LISTADO_SEGUNDOS).toNanos();
            while (System.nanoTime() < limite) {
                long restante = limite - System.nanoTime();
                Map<String, Object> evento = kafka.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                avisarCentral();
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
            System.err.println("CENTRAL no respondió al listado en " + TIMEOUT_LISTADO_SEGUNDOS + " segundos.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Consulta de WS interrumpida.");
        } catch (IOException | IllegalStateException e) {
            System.err.println("No se pudo recibir el listado: " + e.getMessage());
        } finally {
            kafka.finalizarSolicitud(idSolicitud);
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
            Object valorAvailable = estacion.get("available");
            boolean disponible = Boolean.TRUE.equals(valorAvailable)
                    || "ACTIVADA".equalsIgnoreCase(estado)
                    || "AVAILABLE".equalsIgnoreCase(estado);
            // La respuesta de esta consulta debe incluir únicamente estaciones disponibles.
            if (valorAvailable != null && !Boolean.TRUE.equals(valorAvailable)) {
                continue;
            }
            if (valorAvailable == null && estado != null && !disponible) {
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
            } catch (NumberFormatException ignorada) {
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
            kafka.cerrar();
        }
    }

    public static void main(String[] args) {
        String ipBroker;
        int puertoBroker;
        int idOperario;
        try {
            ipBroker = (args.length >= 1 ? args[0] : env("WM_KAFKA_HOST", "localhost")).trim();
            puertoBroker = Integer.parseInt((args.length >= 2 ? args[1] : env("WM_KAFKA_PORT", "9092")).trim());
            idOperario = Integer.parseInt((args.length >= 3 ? args[2] : env("WM_FO_ID", "1")).trim());
        } catch (NumberFormatException e) {
            System.err.println("El puerto del broker y el ID del operario deben ser números enteros.");
            System.err.println("Uso: java WM_FO [IP_BROKER] [PUERTO_BROKER] [ID_OPERARIO]");
            System.err.println("También se admiten WM_KAFKA_HOST, WM_KAFKA_PORT y WM_FO_ID.");
            return;
        }

        if (ipBroker.isEmpty() || puertoBroker < 1 || puertoBroker > 65535 || idOperario <= 0) {
            System.err.println("Argumentos inválidos. La IP no puede estar vacía, el puerto debe estar entre 1 y 65535 y el ID debe ser positivo.");
            return;
        }

        final String anfitrionBroker = ipBroker;
        final int puertoBrokerFinal = puertoBroker;
        final int operario = idOperario;
        if (UiSupport.guiEnabled()) {
            final boolean[] mostrada = {false};
            try {
                SwingUtilities.invokeAndWait(() -> {
                    FOFrame ventana = new FOFrame(anfitrionBroker, puertoBrokerFinal, operario);
                    ventana.setVisible(true);
                    mostrada[0] = true;
                });
            } catch (Throwable t) {
                System.err.println("No se pudo iniciar la interfaz gráfica de WM_FO: "
                        + t.getMessage() + ". Se usará la consola.");
            }
            if (mostrada[0]) return;
        }

        try (WM_FO operador = new WM_FO(ipBroker, puertoBroker, idOperario)) {
            operador.iniciar();
        } catch (Exception e) {
            System.err.println("WM_FO se ha cerrado por un error: " + e.getMessage());
        }
    }

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private enum Resultado {
        EXITO,
        FRACASO,
        DESCONOCIDO
    }

    private static final class Servicio {
        private final int idWs;
        private final int duracionSegundos;
        private final int linea;

        private Servicio(int idWs, int duracionSegundos, int linea) {
            this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
            this.linea = linea;
        }
    }
}
