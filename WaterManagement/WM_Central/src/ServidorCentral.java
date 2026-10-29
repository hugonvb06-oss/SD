import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Punto de entrada de WM_Central.
 *
 * Parámetros (argumentos o variables de entorno):
 *   puerto socket   argumento 1 / WM_CENTRAL_PORT   (por defecto 5000)
 *   IP del broker   argumento 2 / WM_KAFKA_HOST     (por defecto localhost)
 *   puerto broker   argumento 3 / WM_KAFKA_PORT     (por defecto 9092)
 *   ruta SQLite     argumento 4 / WM_DB_PATH        (por defecto database/watermanagement.db)
 */
public final class ServidorCentral {

    public static void main(String[] args) {
        int socketPort;
        int kafkaPort;
        String kafkaHost;
        String dbPath;

        try {
            socketPort = args.length >= 1 ? Integer.parseInt(args[0].trim()) : envInt("WM_CENTRAL_PORT", 5000);
            kafkaHost = args.length >= 2 ? args[1].trim() : env("WM_KAFKA_HOST", "localhost");
            kafkaPort = args.length >= 3 ? Integer.parseInt(args[2].trim()) : envInt("WM_KAFKA_PORT", 9092);
            dbPath = args.length >= 4 ? args[3].trim() : env("WM_DB_PATH", findDatabasePath());
        } catch (NumberFormatException e) {
            System.err.println("Los puertos deben ser números enteros.");
            System.err.println("Uso: java ServidorCentral [puertoSocket] [ipBroker] [puertoBroker] [rutaSQLite]");
            System.err.println("También se admiten WM_CENTRAL_PORT, WM_KAFKA_HOST, WM_KAFKA_PORT y WM_DB_PATH.");
            return;
        }

        if (socketPort < 1 || socketPort > 65535 || kafkaPort < 1 || kafkaPort > 65535 || kafkaHost.isEmpty()) {
            System.err.println("Argumentos no válidos: revise los puertos y la IP del broker.");
            return;
        }

        BaseDatos db = null;
        CentralService central = null;
        try {
            db = new BaseDatos(dbPath);
            // Tras reiniciar CENTRAL ninguna WS se considera disponible hasta que su
            // monitor vuelva a conectar y envíe su primer estado.
            db.desconectarTodasWS();
            central = new CentralService(db, kafkaHost, kafkaPort);
            central.start();

            final CentralService servicio = central;
            final BaseDatos baseDatos = db;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                servicio.close();
                baseDatos.close();
            }, "wm-central-shutdown"));

            System.out.println("\n==================== WM_CENTRAL ====================");
            System.out.println("Socket para monitores WS : puerto " + socketPort);
            System.out.println("Kafka bootstrap          : " + kafkaHost + ":" + kafkaPort);
            System.out.println("SQLite                   : " + dbPath);
            System.out.println("WS autorizadas           : " + env("WM_WS_ALLOWED_IDS", "(todas las válidas)"));
            System.out.println("====================================================");
            central.mostrarPanel(true);
            iniciarPanel(central);
            startAdminConsole(central);

            try (ServerSocket server = new ServerSocket()) {
                server.setReuseAddress(true);
                server.bind(new InetSocketAddress(socketPort));
                System.out.println("CENTRAL iniciado. Esperando conexiones de monitores WS...");
                while (true) {
                    Socket socket = server.accept();
                    socket.setKeepAlive(true);
                    try {
                        new HiloCentral(socket, central).start();
                    } catch (RuntimeException e) {
                        System.err.println("No se pudo atender una conexión WS: " + e.getMessage());
                        try { socket.close(); } catch (IOException ignored) { }
                    }
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

    private static void iniciarPanel(CentralService central) {
        Thread panel = new Thread(() -> {
            while (central.isRunning()) {
                try {
                    central.panelSiCambia();
                    Thread.sleep(1_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    System.err.println("Panel de CENTRAL: " + e.getMessage());
                }
            }
        }, "wm-central-panel");
        panel.setDaemon(true);
        panel.start();
    }

    private static void startAdminConsole(CentralService central) {
        Thread admin = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (central.isRunning()) {
                try {
                    System.out.println("\n--- ADMIN CENTRAL (0 vuelve a mostrar el menú; no apaga CENTRAL) ---");
                    System.out.println("1. Mostrar panel   | 2. Registrar operario | 3. Mostrar operarios");
                    System.out.println("4. Iniciar riego   | 5. Bloquear WS (0=todas) | 6. Activar WS (0=todas)");
                    System.out.println("7. Parar riego (0=todas) | 0. Mostrar menú de nuevo");
                    System.out.print("Opción: ");
                    if (!in.hasNextLine()) return;
                    String option = in.nextLine().trim();
                    switch (option) {
                        case "1": central.mostrarPanel(true); break;
                        case "2":
                            int id = positiveInt(in, "ID de operario: ");
                            System.out.print("Nombre: ");
                            if (!in.hasNextLine()) return;
                            String name = in.nextLine().trim();
                            System.out.println(central.registrarOperario(id, name)
                                    ? "Operario registrado."
                                    : "No se pudo registrar (ID duplicado o datos inválidos).");
                            break;
                        case "3":
                            List<Map<String,Object>> operators = central.listarOperarios();
                            if (operators.isEmpty()) System.out.println("No hay operarios registrados.");
                            for (Map<String,Object> op : operators) {
                                System.out.println("Operario " + op.get("operatorId") + " | " + op.get("name"));
                            }
                            break;
                        case "4":
                            int wsId = nonNegativeInt(in, "ID de WS (0=todas conectadas y disponibles): ");
                            int seconds = positiveInt(in, "Duración en segundos (1-86400): ");
                            if (seconds > 86400) System.out.println("La duración máxima es 86400 segundos.");
                            else central.adminRiego(wsId, seconds);
                            break;
                        case "5":
                            central.adminOrden("BLOCK_WS", nonNegativeInt(in, "ID de WS (0=todas conectadas): "));
                            break;
                        case "6":
                            central.adminOrden("ACTIVATE_WS", nonNegativeInt(in, "ID de WS (0=todas conectadas): "));
                            break;
                        case "7":
                            central.adminOrden("STOP_IRRIGATION", nonNegativeInt(in, "ID de WS (0=todas conectadas): "));
                            break;
                        case "0": System.out.println("CENTRAL sigue activo."); break;
                        default: System.out.println("Opción no válida.");
                    }
                } catch (Exception e) {
                    System.err.println("Menú administrativo: " + e.getMessage());
                }
            }
        }, "wm-central-admin-console");
        admin.setDaemon(true);
        admin.start();
    }

    private static int positiveInt(Scanner in, String prompt) {
        while (true) {
            System.out.print(prompt);
            if (!in.hasNextLine()) return 1;
            String s = in.nextLine().trim();
            try {
                int n = Integer.parseInt(s);
                if (n > 0) return n;
            } catch (NumberFormatException ignored) { }
            System.out.println("Introduce un entero positivo.");
        }
    }

    private static int nonNegativeInt(Scanner in, String prompt) {
        while (true) {
            System.out.print(prompt);
            if (!in.hasNextLine()) return 0;
            String s = in.nextLine().trim();
            try {
                int n = Integer.parseInt(s);
                if (n >= 0) return n;
            } catch (NumberFormatException ignored) { }
            System.out.println("Introduce un entero mayor o igual que cero.");
        }
    }

    private static String findDatabasePath() {
        String[] candidates = {
                "database/watermanagement.db",
                "../database/watermanagement.db",
                "../../database/watermanagement.db"
        };
        for (String candidate : candidates) {
            if (Files.isRegularFile(Paths.get(candidate))) return candidate;
        }
        for (String candidate : candidates) {
            Path parent = Paths.get(candidate).toAbsolutePath().normalize().getParent();
            if (parent != null && Files.isDirectory(parent)) return candidate;
        }
        return candidates[0];
    }

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private static int envInt(String nombre, int porDefecto) {
        try { return Integer.parseInt(env(nombre, Integer.toString(porDefecto))); }
        catch (NumberFormatException e) { return porDefecto; }
    }
}
