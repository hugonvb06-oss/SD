import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class WM_FO {

    private String ipBroker;
    private int puertoBroker;
    private int idOperador;
    private Scanner teclado;

    public WM_FO(String ipBroker, int puertoBroker, int idOperador) {
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperador = idOperador;

        teclado = new Scanner(System.in);
    }

    public void iniciar() {
        System.out.println("================================");
        System.out.println("       WATER MANAGEMENT FO");
        System.out.println("================================");
        System.out.println("Operador: " + idOperador);
        System.out.println("Broker: " + ipBroker + ":" + puertoBroker);

        boolean salir = false;
        while (!salir) {
            mostrarMenu();
            int opcion = teclado.nextInt();
            teclado.nextLine();
            
            switch (opcion) {
                case 1:
                    solicitarRiego();
                    break;
                case 2:
                    leerFichero();
                    break;
                case 3:
                    salir = true;
                    System.out.println("Saliendo de WM_FO...");
                    break;
                default:
                    System.out.println("Opción incorrecta");
            }
        }
    }

    private void mostrarMenu() {
        System.out.println();
        System.out.println("----------- MENU -----------");
        System.out.println("1. Solicitar riego");
        System.out.println("2. Leer fichero de servicios");
        System.out.println("3. Salir");
        System.out.print("Seleccione una opción: ");
    }

    private void solicitarRiego() {

        System.out.println();
        System.out.println("===== SOLICITAR RIEGO =====");

        System.out.print("ID de la WS: ");
        int idWS = teclado.nextInt();

        System.out.print("Tiempo de riego (segundos): ");
        int tiempo = teclado.nextInt();

        System.out.println();
        System.out.println("Solicitud creada:");
        System.out.println("Operador: " + idOperador);
        System.out.println("WS: " + idWS);
        System.out.println("Tiempo: " + tiempo + " segundos");

        enviarSolicitud(idWS, tiempo);
    }

    private void enviarSolicitud(int idWS, int tiempo) {

        /*
         * ESTA PARTE SE CONECTARÁ CON KAFKA
         *
         * De momento solamente mostramos
         * qué solicitud enviaríamos a CENTRAL.
         */

        String mensaje ="RIEGO#" + idOperador + "#" + idWS + "#" +tiempo;
        System.out.println();
        System.out.println("Solicitud preparada:");
        System.out.println(mensaje);
        System.out.println("Esperando respuesta de CENTRAL...");
    }

    private void leerFichero() {
        System.out.print("Ruta del fichero: ");
        String ruta = teclado.nextLine();

        try {
            BufferedReader lector = new BufferedReader(new FileReader(ruta));
            String linea;
            while ((linea = lector.readLine()) != null) {
                if (linea.trim().isEmpty()) {
                    continue;
                }
                System.out.println();
                System.out.println("Servicio leído:");
                System.out.println(linea);
                procesarServicio(linea);

                /*
                 * La práctica indica que, después de que termine
                 * el servicio anterior, se esperan 4 segundos
                 * antes de solicitar el siguiente.
                 *
                 * De momento lo simulamos.
                 */

                System.out.println("Esperando 4 segundos...");
                Thread.sleep(4000);
            }

            lector.close();

        } catch (IOException e) {
            System.out.println("Error leyendo el fichero: "+ e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("La espera fue interrumpida.");
        }
    }

    private void procesarServicio(String servicio) {

        /*
         * Aquí interpretaremos posteriormente
         * el formato que decidamos para el fichero.
         *
         * De momento simplemente mostramos
         * el servicio.
         */

        System.out.println("Enviando servicio a CENTRAL: "+ servicio);
    }

    public static void main(String[] args) {

        /*
         * WM_FO debe recibir:
         *
         * args[0] -> IP del Broker
         * args[1] -> Puerto del Broker
         * args[2] -> ID del operador
         */

        if (args.length < 3) {
            System.out.println("Uso: java WM_FO <IP_BROKER> <PUERTO_BROKER> <ID_OPERADOR>");
            return;
        }

        String ipBroker = args[0];
        int puertoBroker;
        int idOperador;

        try {
            puertoBroker = Integer.parseInt(args[1]);
            idOperador = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("El puerto y el ID del operador deben ser números.");
            return;
        }
        WM_FO operador = new WM_FO(ipBroker, puertoBroker,idOperador);
        operador.iniciar();
    }
}