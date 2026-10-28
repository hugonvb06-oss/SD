
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ServidorCentral {

    public static void main(String[] args) {

        // CENTRAL necesita 3 argumentos:
        // 1. Puerto del servidor de sockets
        // 2. IP del broker Kafka
        // 3. Puerto del broker Kafka

        if (args.length < 3) {
            System.out.println(
                "Uso: java ServidorCentral <puerto> <ip-broker> <puerto-broker>"
            );
            System.out.println(
                "Ejemplo: java ServidorCentral 5000 localhost 9092"
            );
            return;
        }

        int puerto;
        String ipBroker = args[1].trim();
        int puertoBroker;

        try {
            puerto = Integer.parseInt(args[0]);
            puertoBroker = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("El puerto debe ser un numero entero.");
            return;
        }

        if (puerto < 1 || puerto > 65535
                || puertoBroker < 1 || puertoBroker > 65535
                || ipBroker.isEmpty()) {
            System.out.println("Los argumentos introducidos no son validos.");
            return;
        }

        BaseDatos db = new BaseDatos(
            "../../database/watermanagement.db"
        );

        try {
            db.desconectarTodasWS();
            db.mostrarWS();

            try (ServerSocket servidor = new ServerSocket(puerto)) {

                System.out.println("Servidor Central iniciado");
                System.out.println("Puerto de sockets: " + puerto);
                System.out.println("Broker Kafka: " + ipBroker + ":" + puertoBroker);
                System.out.println("Esperando conexiones...");

                while (true) {
                    Socket socket = servidor.accept();

                    System.out.println(
                        "Cliente conectado: " + socket.getInetAddress()
                    );

                    HiloCentral hilo = new HiloCentral(socket, db);
                    hilo.start();
                }
            }

        } catch (IOException e) {
            System.out.println(
                "Error en el ServidorCentral: " + e.getMessage()
            );
        } finally {
            db.cerrar();
        }
    }
}
