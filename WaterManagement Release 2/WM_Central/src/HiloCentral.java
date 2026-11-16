import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Atiende una conexión persistente de monitor WS. Un hilo por estación conectada. */
public final class HiloCentral extends Thread {
    private final Socket socket;
    private final CentralService central;
    private volatile int idWs = -1;
    private volatile boolean registrada;
    private volatile boolean cerrada;
    private InputStream entrada;
    private OutputStream salida;

    public HiloCentral(Socket socket, CentralService central) {
        super("wm-central-ws-socket-" + socket.getRemoteSocketAddress());
        this.socket = socket;
        this.central = central;
        setDaemon(true);
    }

    @Override
    public void run() {
        try {
            socket.setKeepAlive(true);
            socket.setSoTimeout(6_000); // el monitor envía estado cada segundo
            entrada = new BufferedInputStream(socket.getInputStream());
            salida = new BufferedOutputStream(socket.getOutputStream());

            int enq = entrada.read();
            if (enq != WireProtocol.ENQ) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                throw new IOException("Se esperaba ENQ al establecer la sesión");
            }
            WireProtocol.enviarControl(salida, WireProtocol.ACK);

            String saludo = recibirTramaCliente();
            if (saludo == null) throw new EOFException("La WS cerró el socket durante el saludo");
            String respuestaSaludo = procesarSaludo(saludo);
            enviarRespuestaYEsperarAck(respuestaSaludo);
            if (!registrada) throw new IOException("La WS no pudo registrarse/autenticarse: " + respuestaSaludo);

            System.out.println("WS " + idWs + " autenticada; sesión persistente establecida.");
            while (!cerrada && !socket.isClosed()) {
                String mensaje = recibirTramaCliente();
                if (mensaje == null) break; // EOT del monitor
                String respuesta = procesarMensaje(mensaje);
                List<String> comandos = central.tomarComandos(idWs);
                String respuestaFinal = adjuntarComandos(respuesta, comandos);
                try {
                    enviarRespuestaYEsperarAck(respuestaFinal);
                } catch (IOException e) {
                    // Las órdenes no llegaron a confirmarse: se reencolan para la
                    // siguiente sesión en lugar de perderse.
                    central.reencolarComandos(idWs, comandos);
                    throw e;
                }
            }
        } catch (SocketTimeoutException e) {
            if (!cerrada) System.err.println("Timeout de comunicación con WS " + idWs + "; se marca desconectada.");
        } catch (IOException e) {
            if (!cerrada) System.err.println("Conexión de WS " + idWs + " finalizada: " + e.getMessage());
        } catch (RuntimeException e) {
            if (!cerrada) System.err.println("Error atendiendo WS " + idWs + ": " + e.getMessage());
        } finally {
            if (registrada) central.desregistrarEstacion(idWs, this);
            cerrarSocket();
        }
    }

    /** Lee una petición, notifica ACK/NACK de su trama y devuelve DATA. */
    private String recibirTramaCliente() throws IOException {
        while (!cerrada && !socket.isClosed()) {
            int primero = entrada.read();
            if (primero < 0) throw new EOFException("La WS cerró el socket");
            if (primero == WireProtocol.EOT) return null;
            if (primero != WireProtocol.STX) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                System.err.println("WS " + idWs + ": se esperaba STX/EOT; recibido " + primero);
                continue;
            }
            try {
                String datos = WireProtocol.leerTramaTrasStx(entrada);
                WireProtocol.enviarControl(salida, WireProtocol.ACK);
                return datos;
            } catch (WireProtocol.BadLrcException e) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                System.err.println("WS " + idWs + ": " + e.getMessage() + "; esperando retransmisión.");
            }
        }
        return null;
    }

    private void enviarRespuestaYEsperarAck(String respuesta) throws IOException {
        for (int intento = 1; intento <= 3; intento++) {
            WireProtocol.enviarTrama(salida, respuesta);
            int confirmacion = entrada.read();
            if (confirmacion == WireProtocol.ACK) return;
            if (confirmacion == WireProtocol.NACK) {
                System.err.println("WS " + idWs + ": no validó la respuesta de CENTRAL; reintentando (" + intento + "/3).");
                continue;
            }
            if (confirmacion < 0) throw new EOFException("La WS cerró antes de confirmar la respuesta");
            if (confirmacion == WireProtocol.EOT) throw new EOFException("La WS terminó la sesión antes de confirmar la respuesta");
            throw new IOException("Se esperaba ACK/NACK de la respuesta y llegó " + confirmacion);
        }
        throw new IOException("La WS rechazó tres veces una respuesta de CENTRAL");
    }

    private static String adjuntarComandos(String respuesta, List<String> comandos) {
        if (comandos == null || comandos.isEmpty()) return respuesta;
        StringBuilder sb = new StringBuilder(respuesta == null ? "" : respuesta);
        for (String comando : comandos) sb.append("#CMD#").append(comando);
        return sb.toString();
    }

    private String procesarSaludo(String mensaje) {
        String[] h = mensaje.split("#", -1);
        if (h.length != 4 || !"HELLO".equals(h[0])) {
            return "HELLO_OK#KO#Se esperaba HELLO#id#ubicacion_codificada#caudal";
        }
        idWs = entero(h[1], -1);
        String ubicacion;
        try { ubicacion = URLDecoder.decode(h[2], StandardCharsets.UTF_8.name()); }
        catch (Exception e) { ubicacion = h[2]; }
        double caudalNominal = decimal(h[3], -1);
        if (idWs <= 0 || ubicacion.trim().isEmpty() || caudalNominal <= 0) {
            return "HELLO_OK#KO#ID, ubicación o caudal inválidos";
        }
        if (!central.puedeRegistrar(idWs)) {
            return "HELLO_OK#KO#WS no registrada en CENTRAL";
        }
        if (!central.registrarEstacion(idWs, ubicacion, caudalNominal, this)) {
            return "HELLO_OK#KO#No se pudo registrar la estación en SQLite";
        }
        registrada = true;
        return "HELLO_OK#OK#WS_" + idWs + "_REGISTRADA";
    }

    private String procesarMensaje(String mensaje) {
        String[] p = mensaje.split("#", -1);
        if (p.length == 7 && "STATUS".equals(p[0])) {
            String estado = p[1];
            double caudal = decimal(p[2], -1);
            double volumen = decimal(p[3], -1);
            long transcurrido = enteroLargo(p[4], -1);
            String idSolicitud = p[5];
            int idOperario = entero(p[6], 0);
            if (caudal < 0 || volumen < 0 || transcurrido < 0) return "ACK#STATUS#KO#TELEMETRIA_INVALIDA";
            boolean aceptado = central.alRecibirEstado(idWs, estado, caudal, volumen, transcurrido, idSolicitud, idOperario, this);
            return aceptado ? "ACK#STATUS#OK" : "ACK#STATUS#KO#ESTADO_INVALIDO_O_SESION_ANTIGUA";
        }
        if (p.length == 1 && "PING".equals(p[0])) return "PONG";
        return "ACK#KO#OPERACION_DESCONOCIDA";
    }

    public boolean sesionAbierta() { return !cerrada && socket.isConnected() && !socket.isClosed(); }
    public String direccionRemota() { SocketAddress direccion = socket.getRemoteSocketAddress(); return String.valueOf(direccion); }

    public void cerrarDesdeReemplazo() {
        cerrada = true;
        cerrarSocket();
    }

    private void cerrarSocket() {
        cerrada = true;
        try { socket.close(); } catch (IOException ignorada) { }
    }

    private static int entero(String x, int porDefecto) {
        try { return Integer.parseInt(x.trim()); } catch (Exception e) { return porDefecto; }
    }
    private static long enteroLargo(String x, long porDefecto) {
        try { return Long.parseLong(x.trim()); } catch (Exception e) { return porDefecto; }
    }
    private static double decimal(String x, double porDefecto) {
        try { double valor = Double.parseDouble(x.trim()); return Double.isFinite(valor) ? valor : porDefecto; }
        catch (Exception e) { return porDefecto; }
    }
}
