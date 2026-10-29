import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * WM_WS_M - Monitor de una estación WS.
 *
 * Responsabilidades:
 *  - Autenticarse contra WM_Central mediante ENQ + HELLO y aceptar/rechazar el registro.
 *  - Mantener una sesión persistente con WM_Central y enviar cada segundo el estado
 *    y la telemetría de la estación (protocolo STX/DATA/ETX/LRC).
 *  - Escuchar a WM_WS_E (motor) en un puerto propio y comprobar su salud cada segundo.
 *    Si el motor no responde o responde KO, la estación pasa a FUGA (Leak, rojo).
 *  - Recibir de WM_Central las órdenes (iniciar/parar/bloquear/activar) y reenviarlas
 *    al motor.
 *  - Ofrecer un menú local para controlar la estación (riego manual, bloqueo, etc.).
 *
 * Parámetros (argumentos o variables de entorno):
 *   puerto-motor  WM_MONITOR_PORT
 *   ip-central    WM_CENTRAL_HOST
 *   puerto-central WM_CENTRAL_PORT
 *   id-ws         WM_WS_ID
 *   ubicacion     WM_WS_LOCATION
 *   caudal L/min  WM_WS_FLOW
 *   [intervalo]   WM_STATUS_INTERVAL_MS  (por defecto 1000)
 */
public final class WM_WS_M {

    private static final int CENTRAL_CONNECT_TIMEOUT_MS = 5_000;
    private static final int CENTRAL_READ_TIMEOUT_MS = 8_000;
    private static final int ENGINE_CONNECT_TIMEOUT_MS = 3_000;
    private static final long ENGINE_STALE_MS = 3_500L;
    private static final long ENGINE_POLL_MS = 700L;

    private final int puertoMotor;
    private final String ipCentral;
    private final int puertoCentral;
    private final int wsId;
    private final String ubicacion;
    private final double caudalNominal;
    private final long intervaloEstadoMs;

    private final EngineLink motor;

    private volatile boolean ejecutando = true;
    private volatile boolean centralOnline;
    private volatile String ultimoAckCentral = "";

    private Socket socketCentral;
    private InputStream entradaCentral;
    private OutputStream salidaCentral;

    private volatile String ultimaLineaEstado = "";
    private volatile String ultimoEstadoCentral = "";

    private WM_WS_M(int puertoMotor, String ipCentral, int puertoCentral, int wsId,
                    String ubicacion, double caudalNominal, long intervaloEstadoMs) {
        this.puertoMotor = puertoMotor;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.wsId = wsId;
        this.ubicacion = ubicacion;
        this.caudalNominal = caudalNominal;
        this.intervaloEstadoMs = Math.max(250L, intervaloEstadoMs);
        this.motor = new EngineLink(this, puertoMotor);
    }

    public static void main(String[] args) {
        Parametros p;
        try {
            p = Parametros.cargar(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error de configuración: " + e.getMessage());
            System.err.println("Uso: java WM_WS_M <puertoMotor> <ipCentral> <puertoCentral> <idWS> <ubicacion> <caudalLMin> [intervaloMs]");
            System.err.println("También se admiten WM_MONITOR_PORT, WM_CENTRAL_HOST, WM_CENTRAL_PORT,");
            System.err.println("WM_WS_ID, WM_WS_LOCATION, WM_WS_FLOW y WM_STATUS_INTERVAL_MS.");
            System.exit(2);
            return;
        }

        WM_WS_M monitor = new WM_WS_M(p.puertoMotor, p.ipCentral, p.puertoCentral, p.wsId,
                p.ubicacion, p.caudal, p.intervaloMs);
        Runtime.getRuntime().addShutdownHook(new Thread(monitor::detener, "wm-ws-monitor-shutdown"));
        monitor.iniciar();
    }

    private void iniciar() {
        System.out.println("=================================================");
        System.out.println(" WM_WS_M - MONITOR DE ESTACIÓN");
        System.out.println("=================================================");
        System.out.println("WS ID ...........: " + wsId);
        System.out.println("Ubicación .......: " + ubicacion);
        System.out.println("Caudal nominal ..: " + fmt(caudalNominal) + " L/min");
        System.out.println("Motor escuchando : puerto " + puertoMotor);
        System.out.println("WM_Central ......: " + ipCentral + ":" + puertoCentral);
        System.out.println("Intervalo estado : " + intervaloEstadoMs + " ms");
        System.out.println("=================================================");

        motor.start();
        iniciarConsola();

        long siguiente = System.currentTimeMillis();
        while (ejecutando) {
            try {
                if (socketCentral == null || socketCentral.isClosed()) {
                    if (!conectarYAutenticar()) {
                        // Credenciales rechazadas: no tiene sentido reintentar en bucle.
                        if (ejecutando) dormir(3_000L);
                        continue;
                    }
                }
                enviarEstadoCentral();
            } catch (Exception e) {
                desconectarCentral("Imposible conectar con WM_Central: " + mensajeCorto(e));
            }

            imprimirEstado();
            siguiente += intervaloEstadoMs;
            long espera = siguiente - System.currentTimeMillis();
            if (espera < 0) {
                siguiente = System.currentTimeMillis();
                espera = 0;
            }
            dormir(espera);
        }
        detener();
    }

    // ---------------------------------------------------------------- Central

    private boolean conectarYAutenticar() {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(ipCentral, puertoCentral), CENTRAL_CONNECT_TIMEOUT_MS);
            s.setKeepAlive(true);
            s.setSoTimeout(CENTRAL_READ_TIMEOUT_MS);
            InputStream in = new BufferedInputStream(s.getInputStream());
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            WireProtocol.sendControl(out, WireProtocol.ENQ);
            int respuesta = leerControl(in, CENTRAL_READ_TIMEOUT_MS);
            if (respuesta != WireProtocol.ACK) {
                throw new IOException("WM_Central no aceptó la petición de sesión (código " + respuesta + ")");
            }

            String ubicacionCodificada = URLEncoder.encode(ubicacion, StandardCharsets.UTF_8.name());
            WireProtocol.sendFrame(out, "HELLO#" + wsId + "#" + ubicacionCodificada + "#" + fmt(caudalNominal));
            int ack = leerControl(in, CENTRAL_READ_TIMEOUT_MS);
            if (ack != WireProtocol.ACK) throw new IOException("WM_Central no confirmó el HELLO (código " + ack + ")");

            String respuestaHello = WireProtocol.readFrame(in);
            WireProtocol.sendControl(out, WireProtocol.ACK);
            if (respuestaHello == null || !respuestaHello.startsWith("HELLO_OK#OK")) {
                System.err.println("REGISTRO DENEGADO por WM_Central: " + respuestaHello);
                System.err.println("La WS " + wsId + " no está autorizada. Revise WM_WS_ALLOWED_IDS en la central.");
                s.close();
                ejecutando = false;
                return false;
            }

            socketCentral = s;
            entradaCentral = in;
            salidaCentral = out;
            centralOnline = true;
            ultimoEstadoCentral = "";
            System.out.println("[CENTRAL] Sesión autenticada: " + respuestaHello);
            return true;
        } catch (Exception e) {
            try { s.close(); } catch (IOException ignored) { }
            desconectarCentral("Imposible conectar con WM_Central: " + mensajeCorto(e));
            return false;
        }
    }

    private void enviarEstadoCentral() throws IOException {
        EstadoMotor estado = motor.estadoParaCentral();
        String requestId = estado.requestId == null ? "" : estado.requestId;
        String trama = "STATUS#" + estado.estado + "#" + fmt(estado.caudal) + "#" + fmt(estado.volumen)
                + "#" + estado.segundos + "#" + requestId + "#" + estado.operario;
        WireProtocol.sendFrame(salidaCentral, trama);

        int ack = leerControl(entradaCentral, CENTRAL_READ_TIMEOUT_MS);
        if (ack != WireProtocol.ACK) {
            throw new IOException("WM_Central no confirmó la trama de estado (código " + ack + ")");
        }
        String respuesta = WireProtocol.readFrame(entradaCentral);
        WireProtocol.sendControl(salidaCentral, WireProtocol.ACK);
        centralOnline = true;

        if (respuesta != null && respuesta.startsWith("ACK#STATUS#OK")) {
            // Sólo se confirma al motor el informe FINAL de un servicio. Si se confirmara
            // también mientras la estación está REGANDO, el motor borraría su informe
            // final antes de que CENTRAL llegue a observar la transición a disponible y
            // el riego se cerraría como fallido por timeout.
            if (!requestId.isEmpty() && !"REGANDO".equals(estado.estado)) {
                ultimoAckCentral = requestId;
            }
        }
        motor.confirmarConCentral(ultimoAckCentral, centralOnline);
        procesarComandosCentral(respuesta);
    }

    /**
     * WM_Central adjunta las órdenes pendientes a la respuesta de cada STATUS:
     *   ACK#STATUS#OK#CMD#TIPO|requestId|operario|duracion#CMD#TIPO...
     * Se reenvían al motor sin bloquear el envío de estados.
     */
    private void procesarComandosCentral(String respuesta) {
        if (respuesta == null) return;
        String[] partes = respuesta.split("#", -1);
        for (int i = 0; i < partes.length - 1; i++) {
            if ("CMD".equals(partes[i])) {
                String comando = partes[i + 1];
                if (!comando.trim().isEmpty()) {
                    motor.encolarComando(comando);
                    System.out.println("[CENTRAL] Orden recibida: " + comando);
                }
            }
        }
    }

    private void desconectarCentral(String motivo) {
        if (centralOnline || socketCentral != null) {
            System.err.println(motor.estaDisponible()
                    ? "[CENTRAL] " + motivo + ". La WS continúa su riego en curso."
                    : "[CENTRAL] " + motivo);
        }
        centralOnline = false;
        motor.confirmarConCentral("", false);
        cerrarSocketCentral();
        if (!"SIN CONEXION".equals(ultimoEstadoCentral)) {
            ultimoEstadoCentral = "SIN CONEXION";
            ultimaLineaEstado = "";
        }
    }

    private void cerrarSocketCentral() {
        Socket s = socketCentral;
        socketCentral = null;
        entradaCentral = null;
        salidaCentral = null;
        if (s != null) try { s.close(); } catch (IOException ignored) { }
    }

    // ---------------------------------------------------------------- Consola

    private void iniciarConsola() {
        Thread consola = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (ejecutando) {
                try {
                    System.out.println();
                    System.out.println("--- MENÚ WM_WS (monitor) ---");
                    System.out.println("1. Iniciar riego local | 2. Bloquear WS | 3. Activar WS");
                    System.out.println("4. Parar riego        | 5. Ver estado | 0. Salir");
                    System.out.print("Opción: ");
                    if (!in.hasNextLine()) return;
                    String opcion = in.nextLine().trim();
                    switch (opcion) {
                        case "1": iniciarRiegoLocal(in); break;
                        case "2": enviarComandoLocal("BLOCK_WS", 0); break;
                        case "3": enviarComandoLocal("ACTIVATE_WS", 0); break;
                        case "4": enviarComandoLocal("STOP_IRRIGATION", 0); break;
                        case "5": imprimirEstadoForzado(); break;
                        case "0":
                            System.out.println("Cerrando monitor...");
                            ejecutando = false;
                            return;
                        default: System.out.println("Opción no válida.");
                    }
                } catch (Exception e) {
                    System.err.println("Consola del monitor: " + mensajeCorto(e));
                }
            }
        }, "wm-ws-monitor-console");
        consola.setDaemon(true);
        consola.start();
    }

    private void iniciarRiegoLocal(Scanner in) {
        if (!centralOnline) {
            System.out.println("No se puede iniciar un riego: Imposible conectar con WM_Central.");
            return;
        }
        if (!motor.estaDisponible()) {
            System.out.println("No se puede iniciar un riego: la estación está Averiada (motor no disponible).");
            return;
        }
        System.out.print("Duración del riego en segundos (1-86400): ");
        if (!in.hasNextLine()) return;
        int duracion;
        try {
            duracion = Integer.parseInt(in.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Duración no válida.");
            return;
        }
        if (duracion < 1 || duracion > 86400) {
            System.out.println("La duración debe estar entre 1 y 86400 segundos.");
            return;
        }
        String requestId = "LOCAL-" + UUID.randomUUID();
        motor.encolarComando("START_IRRIGATION|" + requestId + "|0|" + duracion);
        System.out.println("Solicitud de riego local enviada al motor.");
    }

    private void enviarComandoLocal(String tipo, int operario) {
        if (!motor.estaConectado()) {
            System.out.println("No se puede enviar la orden: el motor no está conectado.");
            return;
        }
        motor.encolarComando(tipo + "|LOCAL-" + UUID.randomUUID() + "|" + operario + "|0");
        System.out.println("Orden " + tipo + " enviada al motor.");
    }

    // ---------------------------------------------------------------- Panel

    private void imprimirEstado() {
        EstadoMotor e = motor.estadoParaCentral();
        String motorTxt = motor.estaConectado()
                ? (motor.sinRespuesta() ? "SIN RESPUESTA" : "OK")
                : "DESCONECTADO";
        String linea = String.format(Locale.ROOT,
                "WS %d [%s] | CENTRAL: %s | MOTOR: %s | ESTADO: %s | Caudal: %s L/min | Agua: %s L%s",
                wsId, ubicacion, centralOnline ? "CONECTADA" : "SIN CONEXIÓN", motorTxt,
                etiquetaEstado(e.estado), fmt(e.caudal), fmt(e.volumen),
                e.operario > 0 ? " | Operario: " + e.operario : "");
        if (!linea.equals(ultimaLineaEstado)) {
            ultimaLineaEstado = linea;
            System.out.println("[" + hora() + "] " + linea);
        }
    }

    private void imprimirEstadoForzado() {
        ultimaLineaEstado = "";
        imprimirEstado();
    }

    private static String etiquetaEstado(String estado) {
        if (estado == null) return "?";
        switch (estado) {
            case "ACTIVADA": return "Available (VERDE)";
            case "REGANDO": return "Watering (VERDE)";
            case "FUGA": return "Leak / Avería (ROJO)";
            case "FUERA_SERVICIO": return "Out of Service (NARANJA)";
            case "DESCONECTADA": return "Disconnected (GRIS)";
            default: return estado;
        }
    }

    private void detener() {
        ejecutando = false;
        motor.detener();
        cerrarSocketCentral();
    }

    // ---------------------------------------------------------------- Utilidades

    private static int leerControl(InputStream in, int timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int b = in.read();
            if (b < 0) throw new IOException("La conexión se ha cerrado");
            if (b == WireProtocol.ACK || b == WireProtocol.NACK || b == WireProtocol.ENQ || b == WireProtocol.EOT) {
                return b;
            }
            // Ignora cualquier byte residual que no sea de control.
        }
        throw new SocketTimeoutException("Tiempo de espera agotado leyendo un byte de control");
    }

    private static String mensajeCorto(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.trim().isEmpty()) m = t.getClass().getSimpleName();
        return m;
    }

    private static String fmt(double v) { return String.format(Locale.ROOT, "%.2f", v); }

    private static String hora() {
        return java.time.LocalTime.now().withNano(0).toString();
    }

    private static void dormir(long ms) {
        if (ms <= 0) return;
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ---------------------------------------------------------------- Motor

    /** Estado agregado que se envía a WM_Central. */
    static final class EstadoMotor {
        String estado = "FUGA";
        double caudal;
        double volumen;
        long segundos;
        String requestId = "";
        int operario;
    }

    /**
     * Enlace con WM_WS_E. Mantiene una única conexión, le hace POLL cada segundo,
     * recibe su estado y le reenvía las órdenes de WM_Central o del menú local.
     */
    static final class EngineLink extends Thread {
        private final WM_WS_M monitor;
        private final int puerto;
        private final BlockingQueue<String> comandos = new LinkedBlockingQueue<>();

        private volatile boolean activo = true;
        private volatile boolean conectado;
        private volatile boolean sinRespuesta;
        private volatile long ultimoMensaje;
        private volatile String ultimoAckCentral = "";
        private volatile boolean centralOnline;

        private volatile String estado = "FUGA";
        private volatile double caudal;
        private volatile double volumen;
        private volatile long segundos;
        private volatile String requestId = "";
        private volatile int operario;

        private volatile Socket socketActual;
        private ServerSocket servidor;

        EngineLink(WM_WS_M monitor, int puerto) {
            super("wm-ws-engine-link");
            this.monitor = monitor;
            this.puerto = puerto;
            setDaemon(true);
        }

        void encolarComando(String comando) {
            if (comando != null && !comando.trim().isEmpty()) comandos.offer(comando);
        }

        void confirmarConCentral(String requestId, boolean online) {
            this.ultimoAckCentral = requestId == null ? "" : requestId;
            this.centralOnline = online;
        }

        boolean estaConectado() { return conectado; }
        boolean sinRespuesta() { return sinRespuesta || (conectado && System.currentTimeMillis() - ultimoMensaje > ENGINE_STALE_MS); }
        boolean estaDisponible() { return conectado && !sinRespuesta() && "ACTIVADA".equals(estado); }

        EstadoMotor estadoParaCentral() {
            EstadoMotor e = new EstadoMotor();
            boolean motorCaido = !conectado || sinRespuesta() || "FUGA".equals(estado) ||
                    System.currentTimeMillis() - ultimoMensaje > ENGINE_STALE_MS;
            if (motorCaido) {
                // La telemetría se conserva para poder informar del volumen ya regado.
                e.estado = "FUGA";
                e.caudal = 0;
                e.volumen = volumen;
                e.segundos = segundos;
                e.requestId = requestId;
                e.operario = operario;
            } else {
                e.estado = estado;
                e.caudal = caudal;
                e.volumen = volumen;
                e.segundos = segundos;
                e.requestId = requestId;
                e.operario = operario;
            }
            return e;
        }

        @Override
        public void run() {
            try {
                servidor = new ServerSocket();
                servidor.setReuseAddress(true);
                servidor.bind(new java.net.InetSocketAddress(puerto));
            } catch (IOException e) {
                System.err.println("[MOTOR] No se pudo abrir el puerto " + puerto + ": " + e.getMessage());
                activo = false;
                return;
            }
            System.out.println("[MOTOR] Esperando conexión de WM_WS_E en el puerto " + puerto + "...");

            while (activo) {
                try {
                    Socket s = servidor.accept();
                    atenderMotor(s);
                } catch (IOException e) {
                    if (activo) {
                        System.err.println("[MOTOR] Error de enlace: " + mensajeCorto(e) + ". Se reintenta...");
                        dormir(700);
                    }
                }
            }
            cerrarServidor();
        }

        private void atenderMotor(Socket s) {
            socketActual = s;
            try {
                s.setKeepAlive(true);
                s.setSoTimeout((int) Math.max(ENGINE_STALE_MS, ENGINE_POLL_MS + 1_500));
                InputStream in = new BufferedInputStream(s.getInputStream());
                OutputStream out = new BufferedOutputStream(s.getOutputStream());

                String ubicacion = URLEncoder.encode(monitor.ubicacion, StandardCharsets.UTF_8.name());
                WireProtocol.sendFrame(out, "ID#" + monitor.wsId + "#" + ubicacion + "#" + fmt(monitor.caudalNominal));
                int ack = in.read();
                if (ack != WireProtocol.ACK) throw new IOException("El motor no confirmó la identificación (código " + ack + ")");
                System.out.println("[MOTOR] WM_WS_E identificado correctamente.");

                conectado = true;
                sinRespuesta = false;
                ultimoMensaje = System.currentTimeMillis();

                while (activo && !s.isClosed()) {
                    // 1) Órdenes pendientes (central o menú local).
                    String comando = comandos.poll(ENGINE_POLL_MS, TimeUnit.MILLISECONDS);
                    if (comando != null) {
                        WireProtocol.sendFrame(out, "CMD#" + comando);
                        int c = in.read();
                        if (c == WireProtocol.NACK) {
                            comandos.offer(comando);
                        } else if (c != WireProtocol.ACK) {
                            throw new IOException("El motor no confirmó la orden (código " + c + ")");
                        }
                    }

                    // 2) Comprobación de salud + estado. POLL/STATUS es una
                    // petición-respuesta: el LRC protege la trama y no se envía ACK
                    // adicional (evita desincronizar el flujo de bytes).
                    WireProtocol.sendFrame(out, "POLL#" + (centralOnline ? "1" : "0") + "#" + nz(ultimoAckCentral));

                    try {
                        String respuesta = WireProtocol.readFrame(in);
                        if ("KO".equals(respuesta)) {
                            marcarCaida();
                        } else {
                            procesarStatus(respuesta);
                        }
                    } catch (SocketTimeoutException timeout) {
                        // El motor no ha respondido al POLL: se mantiene el enlace, pero
                        // desde este momento la estación se considera averiada.
                        marcarCaida();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (activo) System.err.println("[MOTOR] Enlace perdido: " + mensajeCorto(e));
            } finally {
                conectado = false;
                sinRespuesta = true;
                socketActual = null;
                try { s.close(); } catch (IOException ignored) { }
            }
        }

        private void procesarStatus(String respuesta) {
            if (respuesta == null || !respuesta.startsWith("STATUS#")) return;
            String[] p = respuesta.split("#", -1);
            if (p.length < 7) return;
            String nuevoEstado = p[1].trim().toUpperCase(Locale.ROOT);
            if (!"ACTIVADA".equals(nuevoEstado) && !"REGANDO".equals(nuevoEstado) &&
                    !"FUERA_SERVICIO".equals(nuevoEstado) && !"FUGA".equals(nuevoEstado)) {
                return;
            }
            estado = nuevoEstado;
            caudal = numero(p[2], caudal);
            volumen = numero(p[3], volumen);
            segundos = (long) numero(p[4], segundos);
            requestId = p[5] == null ? "" : p[5];
            operario = (int) numero(p[6], operario);
            sinRespuesta = false;
            ultimoMensaje = System.currentTimeMillis();
        }

        private void marcarCaida() {
            sinRespuesta = true;
            estado = "FUGA";
            caudal = 0;
        }

        void detener() {
            activo = false;
            Socket s = socketActual;
            if (s != null) try { s.close(); } catch (IOException ignored) { }
            cerrarServidor();
            interrupt();
        }

        private void cerrarServidor() {
            ServerSocket ss = servidor;
            servidor = null;
            if (ss != null) try { ss.close(); } catch (IOException ignored) { }
        }

        private static String nz(String s) { return s == null ? "" : s; }

        private static double numero(String s, double fallback) {
            try {
                double v = Double.parseDouble(s.trim());
                return Double.isFinite(v) && v >= 0 ? v : fallback;
            } catch (Exception e) {
                return fallback;
            }
        }
    }

    // ---------------------------------------------------------------- Parámetros

    private static final class Parametros {
        int puertoMotor;
        String ipCentral;
        int puertoCentral;
        int wsId;
        String ubicacion;
        double caudal;
        long intervaloMs = 1_000L;

        static Parametros cargar(String[] args) {
            Parametros p = new Parametros();
            if (args.length >= 6) {
                p.puertoMotor = entero(args[0], "puerto del motor");
                p.ipCentral = args[1].trim();
                p.puertoCentral = entero(args[2], "puerto de WM_Central");
                p.wsId = entero(args[3], "ID de la WS");
                p.ubicacion = args[4].trim();
                p.caudal = decimal(args[5], "caudal");
                if (args.length >= 7) p.intervaloMs = entero(args[6], "intervalo de estado");
            } else {
                p.puertoMotor = entero(env("WM_MONITOR_PORT", "6000"), "WM_MONITOR_PORT");
                p.ipCentral = env("WM_CENTRAL_HOST", "localhost").trim();
                p.puertoCentral = entero(env("WM_CENTRAL_PORT", "5000"), "WM_CENTRAL_PORT");
                p.wsId = entero(env("WM_WS_ID", "1"), "WM_WS_ID");
                p.ubicacion = env("WM_WS_LOCATION", "Jardín principal").trim();
                p.caudal = decimal(env("WM_WS_FLOW", "10.0"), "WM_WS_FLOW");
                p.intervaloMs = entero(env("WM_STATUS_INTERVAL_MS", "1000"), "WM_STATUS_INTERVAL_MS");
            }
            if (p.puertoMotor < 1 || p.puertoMotor > 65535 || p.puertoCentral < 1 || p.puertoCentral > 65535) {
                throw new IllegalArgumentException("Los puertos deben estar entre 1 y 65535.");
            }
            if (p.ipCentral.isEmpty()) throw new IllegalArgumentException("La IP de WM_Central no puede estar vacía.");
            if (p.wsId <= 0) throw new IllegalArgumentException("El ID de la WS debe ser positivo.");
            if (p.ubicacion.isEmpty()) throw new IllegalArgumentException("La ubicación no puede estar vacía.");
            if (!Double.isFinite(p.caudal) || p.caudal <= 0) throw new IllegalArgumentException("El caudal debe ser mayor que cero.");
            if (p.intervaloMs < 250) throw new IllegalArgumentException("El intervalo de estado debe ser >= 250 ms.");
            return p;
        }

        private static String env(String nombre, String porDefecto) {
            String v = System.getenv(nombre);
            return v == null || v.trim().isEmpty() ? porDefecto : v;
        }

        private static int entero(String valor, String nombre) {
            try { return Integer.parseInt(valor.trim()); }
            catch (Exception e) { throw new IllegalArgumentException(nombre + " debe ser un entero."); }
        }

        private static double decimal(String valor, String nombre) {
            try {
                double v = Double.parseDouble(valor.trim().replace(',', '.'));
                if (!Double.isFinite(v)) throw new NumberFormatException();
                return v;
            } catch (Exception e) {
                throw new IllegalArgumentException(nombre + " debe ser un número.");
            }
        }
    }
}
