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

/** Atiende una conexión persistente de monitor WS. Un hilo por estación conectada. */
public final class HiloCentral extends Thread {
    private final Socket socket;
    private final CentralService central;
    private volatile int wsId = -1;
    private volatile boolean registered;
    private volatile boolean closed;
    private InputStream in;
    private OutputStream out;

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
            socket.setSoTimeout(10_000); // se espera heartbeat/estado al menos cada pocos segundos
            in = new BufferedInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());

            int enq = in.read();
            if (enq != WireProtocol.ENQ) {
                WireProtocol.sendControl(out, WireProtocol.NACK);
                throw new IOException("Se esperaba ENQ al establecer la sesión");
            }
            WireProtocol.sendControl(out, WireProtocol.ACK);

            String hello = receiveClientFrame();
            if (hello == null) throw new EOFException("La WS cerró el socket durante el saludo");
            String helloAnswer = processHello(hello);
            sendAnswerAndWaitAck(helloAnswer);
            if (!registered) throw new IOException("La WS no pudo registrarse/autenticarse: " + helloAnswer);

            System.out.println("WS " + wsId + " autenticada; sesión persistente establecida.");
            while (!closed && !socket.isClosed()) {
                String message = receiveClientFrame();
                if (message == null) break; // EOT del monitor
                String answer = processMessage(message);
                sendAnswerAndWaitAck(answer);
            }
        } catch (SocketTimeoutException e) {
            if (!closed) System.err.println("Timeout de comunicación con WS " + wsId + "; se marca desconectada.");
        } catch (IOException e) {
            if (!closed) System.err.println("Conexión de WS " + wsId + " finalizada: " + e.getMessage());
        } catch (RuntimeException e) {
            if (!closed) System.err.println("Error atendiendo WS " + wsId + ": " + e.getMessage());
        } finally {
            if (registered) central.unregisterStation(wsId, this);
            closeSocket();
        }
    }

    /** Lee una petición, notifica ACK/NACK de su trama y devuelve DATA. */
    private String receiveClientFrame() throws IOException {
        while (!closed && !socket.isClosed()) {
            int first = in.read();
            if (first < 0) throw new EOFException("La WS cerró el socket");
            if (first == WireProtocol.EOT) return null;
            if (first != WireProtocol.STX) {
                WireProtocol.sendControl(out, WireProtocol.NACK);
                System.err.println("WS " + wsId + ": se esperaba STX/EOT; recibido " + first);
                continue;
            }
            try {
                String data = WireProtocol.readFrameAfterStx(in);
                WireProtocol.sendControl(out, WireProtocol.ACK);
                return data;
            } catch (WireProtocol.BadLrcException e) {
                WireProtocol.sendControl(out, WireProtocol.NACK);
                System.err.println("WS " + wsId + ": " + e.getMessage() + "; esperando retransmisión.");
            }
        }
        return null;
    }

    private void sendAnswerAndWaitAck(String answer) throws IOException {
        for (int attempt = 1; attempt <= 3; attempt++) {
            WireProtocol.sendFrame(out, answer);
            int confirmation = in.read();
            if (confirmation == WireProtocol.ACK) return;
            if (confirmation == WireProtocol.NACK) {
                System.err.println("WS " + wsId + ": no validó la respuesta de CENTRAL; reintentando (" + attempt + "/3).");
                continue;
            }
            if (confirmation < 0) throw new EOFException("La WS cerró antes de confirmar la respuesta");
            if (confirmation == WireProtocol.EOT) throw new EOFException("La WS terminó la sesión antes de confirmar la respuesta");
            throw new IOException("Se esperaba ACK/NACK de la respuesta y llegó " + confirmation);
        }
        throw new IOException("La WS rechazó tres veces una respuesta de CENTRAL");
    }

    private String processHello(String message) {
        String[] h = message.split("#", -1);
        if (h.length != 4 || !"HELLO".equals(h[0])) {
            return "HELLO_OK#KO#Se esperaba HELLO#id#ubicacion_codificada#caudal";
        }
        wsId = parseInt(h[1], -1);
        String location;
        try { location = URLDecoder.decode(h[2], StandardCharsets.UTF_8.name()); }
        catch (Exception e) { location = h[2]; }
        double nominalFlow = parseDouble(h[3], -1);
        if (wsId <= 0 || location.trim().isEmpty() || nominalFlow <= 0) {
            return "HELLO_OK#KO#ID, ubicación o caudal inválidos";
        }
        if (!central.registerStation(wsId, location, nominalFlow, this)) {
            return "HELLO_OK#KO#No se pudo registrar la estación en SQLite";
        }
        registered = true;
        return "HELLO_OK#OK#WS_" + wsId + "_REGISTRADA";
    }

    private String processMessage(String message) {
        String[] p = message.split("#", -1);
        if (p.length == 7 && "STATUS".equals(p[0])) {
            String state = p[1];
            double flow = parseDouble(p[2], -1);
            double volume = parseDouble(p[3], -1);
            long elapsed = parseLong(p[4], -1);
            String requestId = p[5];
            int operatorId = parseInt(p[6], 0);
            if (flow < 0 || volume < 0 || elapsed < 0) return "ACK#STATUS#KO#TELEMETRIA_INVALIDA";
            boolean accepted = central.onStationStatus(wsId, state, flow, volume, elapsed, requestId, operatorId, this);
            return accepted ? "ACK#STATUS#OK" : "ACK#STATUS#KO#ESTADO_INVALIDO_O_SESION_ANTIGUA";
        }
        if (p.length == 1 && "PING".equals(p[0])) return "PONG";
        return "ACK#KO#OPERACION_DESCONOCIDA";
    }

    public boolean isSessionOpen() { return !closed && socket.isConnected() && !socket.isClosed(); }
    public String remoteAddress() { SocketAddress address = socket.getRemoteSocketAddress(); return String.valueOf(address); }

    public void closeFromReplacement() {
        closed = true;
        closeSocket();
    }

    private void closeSocket() {
        closed = true;
        try { socket.close(); } catch (IOException ignored) { }
    }

    private static int parseInt(String x, int fallback) {
        try { return Integer.parseInt(x.trim()); } catch (Exception e) { return fallback; }
    }
    private static long parseLong(String x, long fallback) {
        try { return Long.parseLong(x.trim()); } catch (Exception e) { return fallback; }
    }
    private static double parseDouble(String x, double fallback) {
        try { double value = Double.parseDouble(x.trim()); return Double.isFinite(value) ? value : fallback; }
        catch (Exception e) { return fallback; }
    }
}
