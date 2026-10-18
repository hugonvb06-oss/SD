import java.io.*;
import java.net.Socket;

public class HiloCentral extends Thread {

    private Socket socket;
    private BaseDatos db;

    // Caracteres de control
    private static final byte ENQ  = 0x05;
    private static final byte ACK  = 0x06;
    private static final byte NACK = 0x15;

    private static final byte STX  = 0x02;
    private static final byte ETX  = 0x03;
    private static final byte EOT  = 0x04;

    public HiloCentral(Socket s_maquina, BaseDatos base) {
        socket = s_maquina;
        db = base;
    }

    @Override
    public void run() {

        try {

            BufferedInputStream entrada = new BufferedInputStream(socket.getInputStream());
            BufferedOutputStream salida = new BufferedOutputStream(socket.getOutputStream());

            // ------------------------------------------------
            // 1. Esperamos ENQ
            // ------------------------------------------------

            int mensaje = entrada.read();
            if (mensaje != ENQ) {
                salida.write(NACK);
                salida.flush();
                System.out.println("Se esperaba ENQ");
                socket.close();
                return;
            }
            System.out.println("ENQ recibido");

            // ------------------------------------------------
            // 2. Respondemos ACK
            // ------------------------------------------------

            salida.write(ACK);
            salida.flush();
            System.out.println("ACK enviado");

            // ------------------------------------------------
            // 3. Recibimos la trama
            // ------------------------------------------------

            int caracter = entrada.read();

            if (caracter != STX) {
                salida.write(NACK);
                salida.flush();

                System.out.println("Error: no se recibió STX");
                socket.close();
                return;
            }

            // ------------------------------------------------
            // 4. Leemos DATA hasta encontrar ETX
            // ------------------------------------------------

            ByteArrayOutputStream datos = new ByteArrayOutputStream();

            while ((caracter = entrada.read()) != ETX) {
                if (caracter == -1) {
                    throw new IOException("Conexion cerrada");
                }
                datos.write(caracter);
            }

            String mensajeDatos = new String(datos.toByteArray(), "UTF-8");

            System.out.println(
                    "Mensaje recibido: " + mensajeDatos
            );

            // ------------------------------------------------
            // 5. Leemos LRC
            // ------------------------------------------------

            int lrcRecibido = entrada.read();
            if (lrcRecibido == -1) {
                throw new IOException("No se recibió LRC");
            }
            byte lrcCalculado = calcularLRC(mensajeDatos);

            // ------------------------------------------------
            // 6. Comprobamos LRC
            // ------------------------------------------------

            if ((byte) lrcRecibido != lrcCalculado) {
                salida.write(NACK);
                salida.flush();
                System.out.println("LRC incorrecto");
                socket.close();
                return;
            }

            System.out.println("LRC correcto");

            // ------------------------------------------------
            // 7. ACK de la trama
            // ------------------------------------------------

            salida.write(ACK);
            salida.flush();

            // ------------------------------------------------
            // 8. Procesamos el mensaje
            // ------------------------------------------------

            procesarMensaje(mensajeDatos, salida);
            socket.close();

        } catch (IOException e) {
            System.out.println("Error en HiloCentral: " + e.getMessage());
        }
    }


    // ========================================================
    // Procesar mensaje recibido
    // ========================================================

    private void procesarMensaje(String mensaje, BufferedOutputStream salida) throws IOException {
        String[] campos = mensaje.split("#");

        if (campos.length == 0) {
            enviarStatus(salida, false, "Mensaje vacio");
            return;
        }

        // ========================================================
        // REGISTRO#ID#UBICACION
        // ========================================================

        if (campos[0].equals("REGISTRO")) {
            if (campos.length != 3) {
                enviarStatus(salida, false, "Formato de registro incorrecto");
                return;
            }

            int id;
            try {
                id = Integer.parseInt(campos[1]);
            } catch (NumberFormatException e) {
                enviarStatus(salida, false, "ID incorrecto");
                return;
            }

            String ubicacion = campos[2];
            System.out.println("Registro de WS recibido:");
            System.out.println("ID: " + id);
            System.out.println("Ubicacion: " + ubicacion);

            boolean registrado = db.registrarWS(id, ubicacion);
            if (registrado) {
                enviarStatus(salida, true, "Estacion registrada correctamente");
            } else {
                enviarStatus(salida, false, "No se pudo registrar la estacion");
            }

        }

        // ========================================================
        // AUTENTICACION#ID
        // ========================================================

        else if (campos[0].equals("AUTENTICACION")) {
            if (campos.length != 2) {
                enviarStatus(salida, false, "Formato de autenticacion incorrecto");
                return;
            }

            int id;
            try {
                id = Integer.parseInt(campos[1]);
            } catch (NumberFormatException e) {
                enviarStatus(salida, false, "ID incorrecto");
                return;
            }

            System.out.println("Solicitud de autenticacion recibida");
            System.out.println("ID de WS: " + id);

            // Comprobamos si la WS esta registrada en la BD
            boolean existe = db.buscarWS(id);
            if (existe) {
                // La WS ha conectado con CENTRAL,
                // por lo que deja de estar DESCONECTADA.
                boolean actualizada = db.actualizarEstadoWS(id, "ACTIVA");
                if (actualizada) {
                    enviarStatus(salida, true, "WS autenticada correctamente");
                } else {
                    enviarStatus(salida, false, "No se pudo actualizar el estado de la WS");
                }

            } else {
                enviarStatus(salida, false, "WS no registrada");
            }
        }

        // ========================================================
        // OPERACION DESCONOCIDA
        // ========================================================

        else {
            enviarStatus(salida, false, "Operacion desconocida");

        }
    }


    // ========================================================
    // Enviar STATUS
    // ========================================================

    private void enviarStatus(BufferedOutputStream salida,boolean correcto,String mensaje) throws IOException {
        String respuesta;
        if (correcto) {
            respuesta ="STATUS#OK#" + mensaje;
        } else {
            respuesta = "STATUS#KO#" + mensaje;
        }

        enviarTrama(salida, respuesta);
    }


    // ========================================================
    // Construir y enviar trama
    // ========================================================

    private void enviarTrama(BufferedOutputStream salida,String datos) throws IOException {
        byte lrc = calcularLRC(datos);
        salida.write(STX);
        salida.write(datos.getBytes("UTF-8"));
        salida.write(ETX);
        salida.write(lrc);
        salida.flush();
    }


    // ========================================================
    // Calcular LRC
    // ========================================================

    private byte calcularLRC(String mensaje) {
        byte lrc = 0;
        byte[] datos = mensaje.getBytes();
        for (byte b : datos) {
            lrc ^= b;
        }
        return lrc;
    }
}