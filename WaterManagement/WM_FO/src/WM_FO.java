import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class WM_FO {

    private final String ipBroker;
    private final int puertoBroker;
    private final int idOperador;

    private final Scanner teclado;

    public WM_FO(String ipBroker, int puertoBroker, int idOperador) {
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperador = idOperador;
        teclado = new Scanner(System.in);
    }

    // ========================================================
    // Inicio de la aplicación
    // ========================================================

    public void iniciar() {
        System.out.println("================================");
        System.out.println("          WM_FO");
        System.out.println("================================");
        System.out.println("ID Operador: " + idOperador);
        System.out.println("Broker: " + ipBroker + ":" + puertoBroker);

        boolean salir = false;
        while (!salir) {
            mostrarMenu();
            String opcion = teclado.nextLine();
            switch (opcion) {
                case "1":
                    solicitarServicioManual();
                    break;
                case "2":
                    solicitarServiciosDesdeFichero();
                    break;
                case "3":
                    salir = true;
                    System.out.println("Saliendo de WM_FO...");
                    break;
                default:
                    System.out.println("Opcion no valida.");
            }
        }
    }

    // ========================================================
    // Menu
    // ========================================================

    private void mostrarMenu() {
        System.out.println();
        System.out.println("----------- MENU -----------");
        System.out.println("1. Solicitar suministro");
        System.out.println("2. Leer servicios desde fichero");
        System.out.println("3. Salir");
        System.out.print("Seleccione una opcion: ");
    }

    // ========================================================
    // Solicitud manual
    // ========================================================

    private void solicitarServicioManual() {
        System.out.println();
        System.out.println("===== SOLICITUD DE SUMINISTRO =====");
        System.out.print("Introduzca los datos de la solicitud: ");
        String servicio = teclado.nextLine();

        if (servicio.trim().isEmpty()) {
            System.out.println("La solicitud no puede estar vacia.");
            return;
        }

        System.out.println();
        System.out.println("Solicitud creada por el operador " + idOperador);

        boolean finalizado = enviarServicio(servicio);
        if (finalizado) {
            System.out.println("El servicio ha terminado correctamente.");
        } else {
            System.out.println("El servicio ha terminado con fracaso.");
        }
    }

    // ========================================================
    // Lectura del fichero
    // ========================================================

    private void solicitarServiciosDesdeFichero() {
        System.out.print("Ruta del fichero: ");
        String ruta = teclado.nextLine();
        try (BufferedReader lector = new BufferedReader(new FileReader(ruta))) {
            String servicio;
            while ((servicio = lector.readLine()) != null) {

                servicio = servicio.trim();
                // Ignoramos lineas vacias
                if (servicio.isEmpty()) {
                    continue;
                }
                System.out.println();
                System.out.println("================================");
                System.out.println("Nuevo servicio del fichero");
                System.out.println("================================");

                /*
                 * La practica indica:
                 *
                 * 1. Enviar el primer servicio.
                 * 2. Esperar a que termine con exito o fracaso.
                 * 3. Esperar 4 segundos.
                 * 4. Solicitar el siguiente.
                 */

                boolean finalizado = enviarServicio(servicio);
                if (finalizado) {
                    System.out.println("Servicio terminado correctamente.");
                } else {
                    System.out.println("Servicio terminado con fracaso.");
                }

                System.out.println("Esperando 4 segundos antes del siguiente...");
                Thread.sleep(4000);
            }

        } catch (IOException e) {
            System.out.println("Error al leer el fichero: "+ e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("La espera fue interrumpida.");
        }
    }

    // ========================================================
    // Envio del servicio
    // ========================================================

    private boolean enviarServicio(String servicio) {

        /*
         * ESTE METODO SERA EL PUNTO DE INTEGRACION CON KAFKA.
         *
         * El PDF exige que WM_FO envie el servicio a CENTRAL,
         * pero no especifica en el enunciado el nombre del
         * topic ni el formato exacto del mensaje.
         *
         * Por eso NO inventamos aqui un formato como:
         *
         * RIEGO#1#2#30
         *
         * hasta que definamos nuestro protocolo Kafka.
         */

        System.out.println();
        System.out.println("Operador: " + idOperador);
        System.out.println("Servicio: " + servicio);

        /*
         * TODO:
         * Publicar el servicio en Kafka.
         */

        boolean enviado = true;

        if (!enviado) {
            System.out.println("No se pudo enviar el servicio a CENTRAL.");
            return false;
        }

        /*
         * TODO:
         *
         * Esperar la respuesta/evento de CENTRAL indicando
         * que el servicio ha terminado:
         *
         * - exito
         * - fracaso
         *
         * Cuando Kafka este implementado, este metodo no debe
         * terminar hasta recibir esa conclusion.
         */

        boolean resultado = esperarConclusionServicio();
        return resultado;
    }

    // ========================================================
    // Esperar conclusion
    // ========================================================

    private boolean esperarConclusionServicio() {

        /*
         * TODAVIA PROVISIONAL.
         *
         * Aqui ira el Consumer de Kafka que esperara el
         * resultado de CENTRAL.
         */

        System.out.println("Esperando conclusion del servicio...");
        return true;
    }

    // ========================================================
    // Main
    // ========================================================

    public static void main(String[] args) {

        /*
         * La practica establece que WM_FO recibe como minimo:
         *
         * args[0] -> IP del Broker
         * args[1] -> Puerto del Broker
         * args[2] -> ID del operador
         */

        if (args.length < 3) {
            System.out.println("Uso:");
            System.out.println("java WM_FO <IP_BROKER> <PUERTO_BROKER> <ID_OPERADOR>");
            return;
        }

        String ipBroker = args[0];
        int puertoBroker;
        int idOperador;

        try {
            puertoBroker =Integer.parseInt(args[1]);
            idOperador =Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("El puerto del broker y el ID del operador "+ "deben ser numeros.");
            return;
        }
        WM_FO operador = new WM_FO(ipBroker, puertoBroker, idOperador);
        operador.iniciar();
    }
}