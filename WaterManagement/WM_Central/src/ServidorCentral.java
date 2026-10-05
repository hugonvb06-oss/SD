import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ServidorCentral {

    public static void main(String[] args) {

        int puerto = 5000;

        try {
            BaseDatos db =new BaseDatos("../../database/watermanagement.db");
            ServerSocket servidor = new ServerSocket(puerto);

            System.out.println("Servidor Central iniciado");
            System.out.println("Esperando conexiones...");
            while (true) {
                Socket socket = servidor.accept();
                System.out.println("Cliente conectado");
                HiloCentral hilo = new HiloCentral(socket, db);

                hilo.start();
            }
        } catch (IOException e) {
            System.out.println("Error en el ServidorCentral: "+ e.getMessage());
        }
    }
}