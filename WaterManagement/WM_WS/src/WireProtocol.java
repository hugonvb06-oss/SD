import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Protocolo de tramas compartido por WM_Central, WM_WS_M y WM_WS_E.
 *
 * Formato de trama: STX + DATA (UTF-8) + ETX + LRC.
 * LRC = XOR de todos los bytes de DATA.
 * Los bytes de control ENQ/ACK/NACK/EOT viajan sueltos (no forman trama).
 *
 * Al recibir una trama se responde con ACK si el LRC es correcto o NACK si no
 * lo es, lo que permite al emisor retransmitirla.
 */
public final class WireProtocol {
    public static final int ENQ = 0x05;
    public static final int ACK = 0x06;
    public static final int NACK = 0x15;
    public static final int STX = 0x02;
    public static final int ETX = 0x03;
    public static final int EOT = 0x04;

    private WireProtocol() { }

    public static void sendControl(OutputStream out, int control) throws IOException {
        if (control != ENQ && control != ACK && control != NACK && control != EOT) {
            throw new IllegalArgumentException("Byte de control no válido: " + control);
        }
        synchronized (out) {
            out.write(control);
            out.flush();
        }
    }

    public static void sendFrame(OutputStream out, String data) throws IOException {
        if (data == null || data.indexOf((char) STX) >= 0 || data.indexOf((char) ETX) >= 0) {
            throw new IllegalArgumentException("Mensaje nulo o con caracteres STX/ETX reservados");
        }
        byte[] payload = data.getBytes(StandardCharsets.UTF_8);
        int lrc = xor(payload);
        synchronized (out) {
            out.write(STX);
            out.write(payload);
            out.write(ETX);
            out.write(lrc);
            out.flush();
        }
    }

    /** Lee una trama completa; el primer byte (STX) todavía está por leer. */
    public static String readFrame(InputStream in) throws IOException {
        int first = in.read();
        if (first < 0) throw new EOFException("Socket cerrado mientras se esperaba STX");
        if (first != STX) throw new IOException("Se esperaba STX; recibido " + first);
        return readFrameAfterStx(in);
    }

    /** Lee DATA, ETX y LRC; el llamador ya ha consumido STX. */
    public static String readFrameAfterStx(InputStream in) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int value;
        while ((value = in.read()) != ETX) {
            if (value < 0) throw new EOFException("Socket cerrado antes de ETX");
            data.write(value);
            if (data.size() > 1024 * 1024) throw new IOException("Trama mayor que 1 MiB; se rechaza");
        }
        int receivedLrc = in.read();
        if (receivedLrc < 0) throw new EOFException("Socket cerrado antes de LRC");
        byte[] payload = data.toByteArray();
        int calculated = xor(payload);
        if ((receivedLrc & 0xFF) != calculated) {
            throw new BadLrcException("LRC incorrecto: recibido=" + receivedLrc + ", calculado=" + calculated);
        }
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static int xor(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) value ^= (b & 0xFF);
        return value & 0xFF;
    }

    public static final class BadLrcException extends IOException {
        public BadLrcException(String message) { super(message); }
    }
}
