import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;

/**
 * WM_WS_E - Motor (engine) de una estación WS.
 *
 * Se conecta al monitor WM_WS_M, que le entrega su identidad. A partir de ahí:
 *  - Responde cada segundo a la comprobación de salud (POLL) con su estado y telemetría.
 *  - Ejecuta las órdenes que le llegan del monitor (que las recibe de WM_Central o del
 *    menú local): iniciar/parar riego, bloquear y activar.
 *  - Acumula el agua suministrada y la persiste en disco para no perderla nunca.
 *  - Permite simular una avería (KO) o un motor que no responde con una tecla, para
 *    demostrar la resiliencia del sistema.
 *
 * Parámetros (argumentos o variables de entorno):
 *   ip-monitor    WM_MONITOR_HOST
 *   puerto-monitor WM_MONITOR_PORT
 *   [dir-estado]  WM_WS_STATE_DIR (por defecto "data")
 */
public final class WM_WS_E {

    private static final long TICK_MS = 100L;
    private static final long MONITOR_STALE_MS = 5_000L;
    private static final long RECONNECT_MS = 2_000L;

    private final String ipMonitor;
    private final int puertoMonitor;
    private final String directorioEstado;

    private final Object lock = new Object();
    private volatile boolean ejecutando = true;

    private int wsId = -1;
    private String ubicacion = "";
    private double caudalNominal = 0;

    private volatile boolean monitorConectado;
    private volatile long ultimoPoll;
    private volatile boolean ko;
    private volatile boolean sinRespuesta;
    private volatile Socket socketActual;

    private boolean bloqueada;
    private Riego activo;
    private Pendiente pendiente;
    private double totalLitros;

    private boolean estadoCargado;
    private long ultimoGuardado;

    private WM_WS_E(String ipMonitor, int puertoMonitor, String directorioEstado) {
        this.ipMonitor = ipMonitor;
        this.puertoMonitor = puertoMonitor;
        this.directorioEstado = directorioEstado;
    }

    public static void main(String[] args) {
        String ip;
        int puerto;
        String dir;
        try {
            if (args.length >= 2) {
                ip = args[0].trim();
                puerto = Integer.parseInt(args[1].trim());
                dir = args.length >= 3 ? args[2].trim() : env("WM_WS_STATE_DIR", "data");
            } else {
                ip = env("WM_MONITOR_HOST", "localhost").trim();
                puerto = Integer.parseInt(env("WM_MONITOR_PORT", "6000").trim());
                dir = env("WM_WS_STATE_DIR", "data");
            }
        } catch (Exception e) {
            System.err.println("Uso: java WM_WS_E <ipMonitor> <puertoMonitor> [dirEstado]");
            System.err.println("También se admiten WM_MONITOR_HOST, WM_MONITOR_PORT y WM_WS_STATE_DIR.");
            System.exit(2);
            return;
        }
        if (ip.isEmpty() || puerto < 1 || puerto > 65535) {
            System.err.println("Parámetros del motor no válidos.");
            System.exit(2);
            return;
        }

        WM_WS_E motor = new WM_WS_E(ip, puerto, dir);
        Runtime.getRuntime().addShutdownHook(new Thread(motor::detener, "wm-ws-engine-shutdown"));
        motor.iniciar();
    }

    private void iniciar() {
        System.out.println("=================================================");
        System.out.println(" WM_WS_E - MOTOR DE ESTACIÓN");
        System.out.println("=================================================");
        System.out.println("Monitor .........: " + ipMonitor + ":" + puertoMonitor);
        System.out.println("Estado en disco .: " + directorioEstado);
        System.out.println("Teclas: K=avería KO | S=motor sin respuesta | E=estado | Q=salir");
        System.out.println("=================================================");

        iniciarConsola();
        iniciarReloj();

        while (ejecutando) {
            try {
                conectarYAtender();
            } catch (Exception e) {
                if (ejecutando) {
                    System.err.println("[MONITOR] Imposible conectar con el monitor: " + mensajeCorto(e));
                }
            } finally {
                perderMonitor("Monitor desconectado; la estación queda AVERIADA.");
            }
            if (ejecutando) dormir(RECONNECT_MS);
        }
        detener();
    }

    /** Abre la conexión con el monitor y procesa sus tramas hasta que se pierda. */
    private void conectarYAtender() throws IOException {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(ipMonitor, puertoMonitor), 3_000);
            s.setKeepAlive(true);
            s.setSoTimeout(2_500);
            InputStream in = new BufferedInputStream(s.getInputStream());
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            String identificacion = WireProtocol.readFrame(in);
            if (identificacion == null || !identificacion.startsWith("ID#")) {
                throw new IOException("El monitor no envió la identificación esperada");
            }
            String[] datos = identificacion.split("#", -1);
            if (datos.length < 4) throw new IOException("Identificación incompleta del monitor");
            int idRecibido;
            try {
                idRecibido = Integer.parseInt(datos[1].trim());
                caudalNominal = Double.parseDouble(datos[3].trim().replace(',', '.'));
            } catch (Exception e) {
                throw new IOException("Identificación con valores no válidos");
            }
            wsId = idRecibido;
            ubicacion = URLDecoder.decode(datos[2], StandardCharsets.UTF_8.name());
            WireProtocol.sendControl(out, WireProtocol.ACK);

            if (!estadoCargado) cargarEstado();
            synchronized (lock) {
                monitorConectado = true;
                ultimoPoll = System.currentTimeMillis();
            }
            socketActual = s;
            System.out.println("[MONITOR] Conectado. WS " + wsId + " (" + ubicacion + "), caudal "
                    + fmt(caudalNominal) + " L/min. Agua acumulada: " + fmt(totalLitros) + " L.");
            imprimirEstado();

            leerTramas(in, out);
        } finally {
            socketActual = null;
            try { s.close(); } catch (IOException ignored) { }
        }
    }

    private void leerTramas(InputStream in, OutputStream out) throws IOException {
        while (ejecutando) {
            String trama;
            try {
                trama = WireProtocol.readFrame(in);
            } catch (SocketTimeoutException timeout) {
                // Sin tráfico en este intervalo; el reloj comprueba la salud del enlace.
                continue;
            }

            if (trama.startsWith("POLL#")) {
                String[] p = trama.split("#", -1);
                boolean centralOnline = p.length > 1 && "1".equals(p[1]);
                String ackCentral = p.length > 2 ? p[2].trim() : "";
                synchronized (lock) {
                    ultimoPoll = System.currentTimeMillis();
                    monitorConectado = true;
                    if (!ackCentral.isEmpty() && pendiente != null && ackCentral.equals(pendiente.requestId)) {
                        System.out.println("[CENTRAL] Confirmado el estado final del servicio " + ackCentral + ".");
                        pendiente = null;
                        guardar();
                    }
                }
                if (sinRespuesta) continue; // motor "colgado": no envía estado
                if (ko) {
                    WireProtocol.sendFrame(out, "KO");
                } else {
                    WireProtocol.sendFrame(out, construirEstado());
                }
            } else if (trama.startsWith("CMD#")) {
                WireProtocol.sendControl(out, WireProtocol.ACK);
                ejecutarComando(trama.substring(4));
            } else {
                WireProtocol.sendControl(out, WireProtocol.NACK);
            }
        }
    }

    /** Texto de estado según el protocolo acordado con el monitor. */
    private String construirEstado() {
        synchronized (lock) {
            String estado;
            String req = "";
            int operario = 0;
            long segundos = 0;
            double caudal = 0;

            if (activo != null) {
                estado = "REGANDO";
                req = activo.requestId;
                operario = activo.operario;
                segundos = activo.elapsedSeconds();
                caudal = caudalNominal;
            } else {
                estado = bloqueada ? "FUERA_SERVICIO" : "ACTIVADA";
                if (pendiente != null) {
                    req = pendiente.requestId;
                    operario = pendiente.operario;
                    segundos = pendiente.segundos;
                }
            }
            return "STATUS#" + estado + "#" + fmt(caudal) + "#" + fmt(totalLitros)
                    + "#" + segundos + "#" + (req == null ? "" : req) + "#" + operario;
        }
    }

    private void ejecutarComando(String comando) {
        String[] p = comando.split("\\|", -1);
        String tipo = p.length > 0 ? p[0].trim() : "";
        String requestId = p.length > 1 ? p[1].trim() : "";
        int operario = p.length > 2 ? entero(p[2], 0) : 0;
        int duracion = p.length > 3 ? entero(p[3], 0) : 0;

        synchronized (lock) {
            switch (tipo) {
                case "START_IRRIGATION":
                    iniciarRiego(requestId, operario, duracion);
                    break;
                case "STOP_IRRIGATION":
                    if (activo != null) {
                        System.out.println("[CENTRAL] Orden de parada recibida.");
                        finalizar("Parada solicitada");
                    } else {
                        System.out.println("[CENTRAL] Orden de parada sin riego en curso.");
                    }
                    break;
                case "BLOCK_WS":
                    if (activo != null) finalizar("Bloqueo de la estación");
                    bloqueada = true;
                    System.out.println("[CENTRAL] Estación BLOQUEADA (Out of Service).");
                    guardar();
                    break;
                case "ACTIVATE_WS":
                    bloqueada = false;
                    System.out.println("[CENTRAL] Estación ACTIVADA (Available).");
                    guardar();
                    break;
                default:
                    System.out.println("[CENTRAL] Orden desconocida: " + tipo);
            }
        }
        imprimirEstado();
    }

    private void iniciarRiego(String requestId, int operario, int duracion) {
        if (ko || sinRespuesta) {
            System.out.println("[RIEGO] Rechazado: la estación está averiada.");
            return;
        }
        if (!monitorConectado) {
            System.out.println("[RIEGO] Rechazado: no hay enlace con el monitor (Averiada).");
            return;
        }
        if (bloqueada) {
            System.out.println("[RIEGO] Rechazado: la estación está bloqueada (Out of Service).");
            return;
        }
        if (activo != null) {
            System.out.println("[RIEGO] Rechazado: ya hay un riego en curso.");
            return;
        }
        if (duracion < 1 || duracion > 86400) {
            System.out.println("[RIEGO] Rechazado: duración no válida.");
            return;
        }
        if (requestId.isEmpty()) requestId = "LOCAL-" + java.util.UUID.randomUUID();
        activo = new Riego(requestId, operario, duracion);
        System.out.println("[RIEGO] INICIADO " + requestId + " | Operario: " + operario
                + " | Duración: " + duracion + " s | Caudal: " + fmt(caudalNominal) + " L/min");
        guardar();
    }

    /** Finaliza el riego en curso y deja su resultado pendiente de confirmar por CENTRAL. */
    private void finalizar(String motivo) {
        if (activo == null) return;
        Riego r = activo;
        long segundos = r.elapsedSeconds();
        pendiente = new Pendiente(r.requestId, r.operario, segundos, r.servicioLitros);
        activo = null;
        System.out.println("[RIEGO] FINALIZADO (" + motivo + ") " + r.requestId
                + " | Servicio: " + fmt(r.servicioLitros) + " L en " + segundos
                + " s | Total acumulado: " + fmt(totalLitros) + " L");
        guardar();
    }

    // ---------------------------------------------------------------- Reloj

    private void iniciarReloj() {
        Thread reloj = new Thread(() -> {
            long anterior = System.currentTimeMillis();
            while (ejecutando) {
                long ahora = System.currentTimeMillis();
                long dt = ahora - anterior;
                anterior = ahora;

                boolean cerrarEnlace = false;
                synchronized (lock) {
                    if (activo != null && monitorConectado && !ko && !sinRespuesta) {
                        double incremento = caudalNominal / 60.0 * (dt / 1000.0);
                        activo.servicioLitros += incremento;
                        totalLitros += incremento;
                        if (activo.elapsedSeconds() >= activo.duracion) {
                            finalizar("Duración completada");
                        }
                    }
                    if (monitorConectado && ahora - ultimoPoll > MONITOR_STALE_MS) {
                        cerrarEnlace = true;
                    }
                    if (ahora - ultimoGuardado >= 1_000L && (activo != null || pendiente != null)) {
                        guardar();
                    }
                }
                if (cerrarEnlace) {
                    System.out.println("[MONITOR] Sin comprobaciones de salud; se cierra el enlace averiado.");
                    perderMonitor("Monitor sin respuesta; la estación queda AVERIADA.");
                    Socket s = socketActual;
                    if (s != null) try { s.close(); } catch (IOException ignored) { }
                }
                dormir(TICK_MS);
            }
        }, "wm-ws-engine-clock");
        reloj.setDaemon(true);
        reloj.start();
    }

    private void perderMonitor(String motivo) {
        boolean teniaRiego;
        synchronized (lock) {
            teniaRiego = activo != null;
            if (monitorConectado || teniaRiego) {
                monitorConectado = false;
                if (activo != null) finalizar(motivo);
            }
            monitorConectado = false;
        }
        System.err.println("[ESTADO] " + motivo + (teniaRiego ? " El riego en curso finaliza." : ""));
        imprimirEstado();
    }

    // ---------------------------------------------------------------- Consola

    private void iniciarConsola() {
        Thread consola = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (ejecutando) {
                System.out.println();
                System.out.println("--- WM_WS_E (motor) ---");
                System.out.println("K. Simular avería KO (ON/OFF) | S. Motor sin respuesta (ON/OFF)");
                System.out.println("E. Ver estado                 | Q. Salir");
                System.out.print("Opción: ");
                if (!in.hasNextLine()) return;
                String opcion = in.nextLine().trim().toUpperCase(Locale.ROOT);
                switch (opcion) {
                    case "K": alternarKo(); break;
                    case "S": alternarSinRespuesta(); break;
                    case "E": imprimirEstado(); break;
                    case "Q":
                        ejecutando = false;
                        return;
                    default: System.out.println("Opción no válida.");
                }
            }
        }, "wm-ws-engine-console");
        consola.setDaemon(true);
        consola.start();
    }

    private void alternarKo() {
        synchronized (lock) {
            ko = !ko;
            if (ko) {
                sinRespuesta = false;
                System.out.println(">>> AVERÍA SIMULADA (KO): el motor responderá KO al monitor.");
                if (activo != null) finalizar("Avería simulada (KO)");
            } else {
                System.out.println(">>> AVERÍA RESUELTA: el motor vuelve a responder con su estado.");
            }
            guardar();
        }
        imprimirEstado();
    }

    private void alternarSinRespuesta() {
        synchronized (lock) {
            sinRespuesta = !sinRespuesta;
            if (sinRespuesta) {
                ko = false;
                System.out.println(">>> MOTOR SIN RESPUESTA: el monitor detectará la avería por timeout.");
                if (activo != null) finalizar("Motor sin respuesta");
            } else {
                System.out.println(">>> MOTOR RECUPERADO: vuelve a responder al monitor.");
            }
            guardar();
        }
        imprimirEstado();
    }

    private void imprimirEstado() {
        synchronized (lock) {
            String estado;
            if (ko) estado = "AVERÍA KO (ROJO)";
            else if (sinRespuesta) estado = "SIN RESPUESTA (ROJO)";
            else if (!monitorConectado) estado = "AVERIADA / MONITOR DESCONECTADO";
            else if (activo != null) estado = "Watering (REGANDO)";
            else if (bloqueada) estado = "Out of Service (NARANJA)";
            else estado = "Available (VERDE)";
            System.out.println("[" + hora() + "] WS " + (wsId > 0 ? wsId : "?") + " | " + estado
                    + " | Agua total: " + fmt(totalLitros) + " L"
                    + (activo != null ? " | Servicio: " + fmt(activo.servicioLitros) + " L"
                    + " | Operario: " + activo.operario : "")
                    + (pendiente != null ? " | Pendiente de reportar: " + pendiente.requestId : ""));
        }
    }

    // ---------------------------------------------------------------- Persistencia

    private Path rutaEstado() {
        if (wsId <= 0) return null;
        return Paths.get(directorioEstado, "ws_" + wsId + ".state");
    }

    /** Carga el volumen acumulado y el último riego no confirmado tras un reinicio. */
    private void cargarEstado() {
        estadoCargado = true;
        Path ruta = rutaEstado();
        if (ruta == null || !Files.isRegularFile(ruta)) return;
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(ruta)) {
            p.load(in);
            totalLitros = decimal(p.getProperty("total", "0"), 0);
            String activoRequest = p.getProperty("activeRequest", "").trim();
            if (!activoRequest.isEmpty()) {
                // El motor cayó durante un riego: se conserva lo ya regado y se reporta
                // como resultado final al recuperar la conexión.
                long segundos = (long) decimal(p.getProperty("activeElapsed", "0"), 0);
                double servicio = decimal(p.getProperty("activeService", "0"), 0);
                try {
                    long inicio = Long.parseLong(p.getProperty("activeStart", "0"));
                    if (inicio > 0) {
                        long transcurrido = (System.currentTimeMillis() - inicio) / 1000L;
                        long duracion = (long) decimal(p.getProperty("activeDuration", "0"), 0);
                        if (duracion > 0) transcurrido = Math.min(transcurrido, duracion);
                        segundos = Math.max(segundos, transcurrido);
                    }
                } catch (Exception ignored) { }
                pendiente = new Pendiente(activoRequest,
                        (int) decimal(p.getProperty("activeOperator", "0"), 0),
                        segundos, servicio);
                System.out.println("[ESTADO] Recuperado riego interrumpido " + activoRequest
                        + " (" + fmt(servicio) + " L); se reportará su estado final.");
            } else {
                String pendRequest = p.getProperty("pendRequest", "").trim();
                if (!pendRequest.isEmpty()) {
                    pendiente = new Pendiente(pendRequest,
                            (int) decimal(p.getProperty("pendOperator", "0"), 0),
                            (long) decimal(p.getProperty("pendElapsed", "0"), 0),
                            decimal(p.getProperty("pendService", "0"), 0));
                }
            }
            System.out.println("[ESTADO] Volumen acumulado recuperado: " + fmt(totalLitros) + " L.");
            guardar();
        } catch (Exception e) {
            System.err.println("[ESTADO] No se pudo leer " + ruta + ": " + mensajeCorto(e));
        }
    }

    /** Guarda el estado en disco. Se invoca con el monitor de {@link #lock} tomado. */
    private void guardar() {
        Path ruta = rutaEstado();
        if (ruta == null) return;
        Properties p = new Properties();
        p.setProperty("total", Double.toString(totalLitros));
        if (activo != null) {
            p.setProperty("activeRequest", activo.requestId);
            p.setProperty("activeOperator", Integer.toString(activo.operario));
            p.setProperty("activeDuration", Long.toString(activo.duracion));
            p.setProperty("activeStart", Long.toString(activo.inicioMillis));
            p.setProperty("activeElapsed", Long.toString(activo.elapsedSeconds()));
            p.setProperty("activeService", Double.toString(activo.servicioLitros));
        }
        if (pendiente != null) {
            p.setProperty("pendRequest", pendiente.requestId);
            p.setProperty("pendOperator", Integer.toString(pendiente.operario));
            p.setProperty("pendElapsed", Long.toString(pendiente.segundos));
            p.setProperty("pendService", Double.toString(pendiente.servicio));
        }
        try {
            Path padre = ruta.getParent();
            if (padre != null) Files.createDirectories(padre);
            try (OutputStream out = Files.newOutputStream(ruta)) {
                p.store(out, "WM_WS_E - estado persistente de la estación " + wsId);
            }
            ultimoGuardado = System.currentTimeMillis();
        } catch (Exception e) {
            System.err.println("[ESTADO] No se pudo guardar " + ruta + ": " + mensajeCorto(e));
        }
    }

    private void detener() {
        ejecutando = false;
        synchronized (lock) {
            if (activo != null) finalizar("Cierre del motor");
            guardar();
        }
        Socket s = socketActual;
        if (s != null) try { s.close(); } catch (IOException ignored) { }
    }

    // ---------------------------------------------------------------- Utilidades

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private static int entero(String valor, int porDefecto) {
        try { return Integer.parseInt(valor.trim()); } catch (Exception e) { return porDefecto; }
    }

    private static double decimal(String valor, double porDefecto) {
        try {
            double d = Double.parseDouble(valor.trim());
            return Double.isFinite(d) ? d : porDefecto;
        } catch (Exception e) {
            return porDefecto;
        }
    }

    private static String fmt(double v) { return String.format(Locale.ROOT, "%.2f", v); }

    private static String hora() { return java.time.LocalTime.now().withNano(0).toString(); }

    private static String mensajeCorto(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.trim().isEmpty()) m = t.getClass().getSimpleName();
        return m;
    }

    private static void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static final class Riego {
        final String requestId;
        final int operario;
        final long duracion;
        final long inicioMillis = System.currentTimeMillis();
        double servicioLitros;

        Riego(String requestId, int operario, long duracion) {
            this.requestId = requestId;
            this.operario = operario;
            this.duracion = duracion;
        }

        long elapsedSeconds() {
            return Math.max(0, (System.currentTimeMillis() - inicioMillis) / 1000L);
        }
    }

    private static final class Pendiente {
        final String requestId;
        final int operario;
        final long segundos;
        final double servicio;

        Pendiente(String requestId, int operario, long segundos, double servicio) {
            this.requestId = requestId;
            this.operario = operario;
            this.segundos = segundos;
            this.servicio = servicio;
        }
    }
}
