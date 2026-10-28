import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/** Punto de entrada de WM_Central. */
public final class ServidorCentral {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java ServidorCentral <puerto-socket> <ip-broker> <puerto-broker> [ruta-sqlite]");
            System.out.println("Ejemplo: java ServidorCentral 5000 localhost 9092 ../database/watermanagement.db");
            return;
        }
        int socketPort, kafkaPort;
        String kafkaHost = args[1].trim();
        try { socketPort = Integer.parseInt(args[0]); kafkaPort = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.err.println("Los puertos deben ser enteros."); return; }
        if (socketPort < 1 || socketPort > 65535 || kafkaPort < 1 || kafkaPort > 65535 || kafkaHost.isEmpty()) {
            System.err.println("Argumentos no válidos."); return;
        }

        String dbPath = args.length >= 4 ? args[3] : findDatabasePath();
        //BaseDatos db = null;
        String rutaBD = System.getenv().getOrDefault(
            "WM_DB_PATH",
            "../../database/watermanagement.db"
        );
        BaseDatos db = new BaseDatos(rutaBD);
        
        CentralService central = null;
        try {
            db = new BaseDatos(dbPath);
            // Tras reiniciar CENTRAL, ninguna WS se considera disponible hasta recibir un nuevo estado.
            db.desconectarTodasWS();
            central = new CentralService(db, kafkaHost, kafkaPort);
            central.start();
            CentralService service = central;
            BaseDatos dbForHook = db;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                service.close();
                dbForHook.close();
            }, "wm-central-shutdown"));

            System.out.println("\n========== WM_CENTRAL ==========");
            System.out.println("Socket WS: puerto " + socketPort);
            System.out.println("Kafka bootstrap: " + kafkaHost + ":" + kafkaPort);
            System.out.println("SQLite: " + dbPath);
            printStations(db.listarWS());
            startAdminConsole(central);

            try (ServerSocket server = new ServerSocket()) {
                server.setReuseAddress(true);
                server.bind(new InetSocketAddress(socketPort));
                System.out.println("CENTRAL iniciado. Esperando conexiones de monitores WS...");
                while (true) {
                    Socket socket = server.accept();
                    socket.setKeepAlive(true);
                    HiloCentral hilo = new HiloCentral(socket, central);
                    hilo.start();
                }
            }
        } catch (IOException e) {
            System.err.println("Error del servidor CENTRAL: " + e.getMessage());
        } catch (RuntimeException e) {
            System.err.println("No se pudo iniciar CENTRAL: " + e.getMessage());
        } finally {
            if (central != null) central.close();
            if (db != null) db.close();
        }
    }

    private static String findDatabasePath() {
        // Prioriza el directorio database del proyecto cuando se ejecuta desde la raíz.
        // Si se ejecuta desde WM_Central/src o WM_Central, usa el database del padre.
        String[] candidates = {"database/watermanagement.db", "../database/watermanagement.db", "../../database/watermanagement.db"};
        for (String candidate : candidates) {
            if (Files.isRegularFile(Paths.get(candidate))) return candidate;
        }
        for (String candidate : candidates) {
            Path parent = Paths.get(candidate).toAbsolutePath().normalize().getParent();
            if (parent != null && Files.isDirectory(parent)) return candidate;
        }
        return candidates[0];
    }

    private static void startAdminConsole(CentralService central) {
        Thread admin = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (central.isRunning()) {
                try {
                    System.out.println("\n--- ADMIN CENTRAL (0 vuelve a mostrar el menú; no apaga CENTRAL) ---");
                    System.out.println("1. Mostrar WS | 2. Registrar operario | 3. Mostrar operarios");
                    System.out.println("4. Iniciar riego | 5. Bloquear WS (0=todas) | 6. Activar WS (0=todas)");
                    System.out.print("Opción: ");
                    if (!in.hasNextLine()) return;
                    String option = in.nextLine().trim();
                    switch (option) {
                        case "1": printStations(central.listarWS()); break;
                        case "2":
                            int id = positiveInt(in, "ID de operario: ");
                            System.out.print("Nombre: "); String name = in.nextLine().trim();
                            System.out.println(central.registrarOperario(id, name) ? "Operario registrado." : "No se pudo registrar (ID duplicado o datos inválidos).");
                            break;
                        case "3":
                            List<Map<String,Object>> operators = central.listarOperarios();
                            if (operators.isEmpty()) System.out.println("No hay operarios registrados.");
                            for (Map<String,Object> op : operators) System.out.println("Operario " + op.get("operatorId") + " | " + op.get("name"));
                            break;
                        case "4":
                            int wsId = nonNegativeInt(in, "ID de WS (0=todas conectadas y disponibles): ");
                            int seconds = positiveInt(in, "Duración en segundos (1-86400): ");
                            if (seconds > 86400) System.out.println("La duración máxima es 86400 segundos.");
                            else central.adminRiego(wsId, seconds);
                            break;
                        case "5": central.adminOrden("BLOCK_WS", nonNegativeInt(in, "ID de WS (0=todas conectadas): ")); break;
                        case "6": central.adminOrden("ACTIVATE_WS", nonNegativeInt(in, "ID de WS (0=todas conectadas): ")); break;
                        case "0": System.out.println("CENTRAL sigue activo."); break;
                        default: System.out.println("Opción no válida.");
                    }
                } catch (Exception e) { System.err.println("Menú administrativo: " + e.getMessage()); }
            }
        }, "wm-central-admin-console");
        admin.setDaemon(true);
        admin.start();
    }

    private static int positiveInt(Scanner in, String prompt) {
        while (true) { System.out.print(prompt); String s=in.nextLine().trim(); try { int n=Integer.parseInt(s); if(n>0)return n; } catch(Exception ignored) { }
            System.out.println("Introduce un entero positivo."); }
    }
    private static int nonNegativeInt(Scanner in, String prompt) {
        while (true) { System.out.print(prompt); String s=in.nextLine().trim(); try { int n=Integer.parseInt(s); if(n>=0)return n; } catch(Exception ignored) { }
            System.out.println("Introduce un entero mayor o igual que cero."); }
    }
    private static void printStations(List<Map<String,Object>> stations) {
        System.out.println("\n--- ESTACIONES WS ---");
        if (stations.isEmpty()) { System.out.println("No hay WS registradas."); return; }
        for (Map<String,Object> s : stations) {
            String state = String.valueOf(s.get("state"));
            System.out.println("WS " + s.get("wsId") + " | " + s.get("location") +
                    " | Estado: " + coloredState(state) + " | Agua: " + s.get("volumeLitres") + " L");
        }
    }

    private static String coloredState(String state) {
        // ANSI color en terminales compatibles (Windows Terminal y terminal integrada de VS Code).
        switch (state.toUpperCase(java.util.Locale.ROOT)) {
            case "ACTIVADA": return "\u001B[32mVERDE / ACTIVADA\u001B[0m";
            case "REGANDO": return "\u001B[1;32mVERDE PARPADEANTE / REGANDO\u001B[0m";
            case "FUGA": return "\u001B[31mROJO / FUGA\u001B[0m";
            case "FUERA_SERVICIO": return "\u001B[38;5;208mNARANJA / FUERA DE SERVICIO\u001B[0m";
            case "DESCONECTADA": return "\u001B[90mGRIS / DESCONECTADA\u001B[0m";
            default: return state;
        }
    }
}
