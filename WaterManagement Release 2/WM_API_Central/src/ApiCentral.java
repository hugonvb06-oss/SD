import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * API_Central (Release 2).
 *
 * Expone una API REST sobre la base de datos SQLite compartida con WM_Central para que
 * el Front (y otros módulos) consulten el estado del sistema, y para que WM_W notifique
 * alertas climáticas o su cancelación.
 *
 * Endpoints:
 *   GET  /api/estaciones   -> WS registradas (id, ubicación, estado, litros, última conexión)
 *   GET  /api/operarios    -> operarios (FO) registrados
 *   GET  /api/riegos       -> riegos en curso
 *   GET  /api/clima        -> alertas climáticas vigentes (tabla AlertaClima)
 *   GET  /api/resumen      -> agregado de todo lo anterior (para el dashboard)
 *   PUT  /api/alerta       -> WM_W notifica una alerta (o su cancelación) para una ciudad
 *
 * La alerta se guarda en SQLite (visible para el Front) y, de forma opcional, se publica
 * en el topic Kafka "wm.central.alertas" para que WM_Central ordene la parada de la WS.
 *
 * Parámetros (argumentos o variables de entorno):
 *   puerto  WM_API_PORT    (por defecto 8080)
 *   BD      WM_DB_PATH     (por defecto database/watermanagement.db)
 */
public final class ApiCentral {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TIPO_MAPA =
            new TypeReference<Map<String, Object>>() { };

    private final Connection conexion;
    private final HttpServer servidor;
    private final KafkaProducer<String, String> productorAlertas;

    public static void main(String[] args) throws Exception {
        int puerto = args.length >= 1 ? Integer.parseInt(args[0].trim())
                : envInt("WM_API_PORT", 8080);
        String rutaBd = args.length >= 2 ? args[1].trim() : buscarRutaBaseDatos();

        ApiCentral api = new ApiCentral(rutaBd, puerto);
        api.iniciar();
        System.out.println("API_Central escuchando en http://0.0.0.0:" + puerto);
        System.out.println("Base de datos compartida : " + rutaBd);
        System.out.println("Endpoints: GET /api/estaciones, /api/operarios, /api/riegos, /api/clima, /api/resumen | PUT /api/alerta");
    }

    public ApiCentral(String rutaBd, int puerto) throws Exception {
        conexion = DriverManager.getConnection("jdbc:sqlite:" + rutaBd);
        try (Statement s = conexion.createStatement()) {
            s.execute("PRAGMA busy_timeout=5000");
            s.execute("CREATE TABLE IF NOT EXISTS AlertaClima ("
                    + "ciudad TEXT PRIMARY KEY,"
                    + "alerta INTEGER NOT NULL DEFAULT 0,"
                    + "motivo TEXT,"
                    + "temperatura REAL,"
                    + "lluvia INTEGER NOT NULL DEFAULT 0,"
                    + "actualizado TEXT)");
        }

        servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        servidor.createContext("/api/estaciones", this::estaciones);
        servidor.createContext("/api/operarios", this::operarios);
        servidor.createContext("/api/riegos", this::riegos);
        servidor.createContext("/api/clima", this::clima);
        servidor.createContext("/api/resumen", this::resumen);
        servidor.createContext("/api/alerta", this::alerta);
        servidor.setExecutor(Executors.newFixedThreadPool(8));

        // Productor Kafka para alertas: opcional y no bloqueante para el arranque.
        this.productorAlertas = alertasKafkaHabilitadas() ? crearProductorAlertas() : null;
    }

    public void iniciar() {
        servidor.start();
    }

    // ------------------------------------------------------------------ Endpoints

    private void estaciones(HttpExchange ex) throws IOException {
        manejar(ex, "GET", () -> responder(ex, 200, leerEstaciones()));
    }

    private void operarios(HttpExchange ex) throws IOException {
        manejar(ex, "GET", () -> responder(ex, 200, leerOperarios()));
    }

    private void riegos(HttpExchange ex) throws IOException {
        manejar(ex, "GET", () -> responder(ex, 200, leerRiegos()));
    }

    private void clima(HttpExchange ex) throws IOException {
        manejar(ex, "GET", () -> responder(ex, 200, leerClima()));
    }

    private void resumen(HttpExchange ex) throws IOException {
        manejar(ex, "GET", () -> {
            Map<String, Object> resumen = new LinkedHashMap<>();
            resumen.put("estaciones", leerEstaciones());
            resumen.put("operarios", leerOperarios());
            resumen.put("riegos", leerRiegos());
            resumen.put("clima", leerClima());
            responder(ex, 200, resumen);
        });
    }

    private void alerta(HttpExchange ex) throws IOException {
        manejar(ex, "PUT", () -> {
            String cuerpo = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, Object> datos = JSON.readValue(cuerpo, TIPO_MAPA);
            String ciudad = texto(datos.get("ciudad"));
            if (ciudad == null || ciudad.trim().isEmpty()) {
                responder(ex, 400, Map.of("error", "Falta el campo 'ciudad'."));
                return;
            }
            boolean alerta = Boolean.TRUE.equals(datos.get("alerta"));
            String motivo = texto(datos.get("motivo"));
            double temperatura = numero(datos.get("temperatura"), Double.NaN);
            boolean lluvia = Boolean.TRUE.equals(datos.get("lluvia"));

            guardarAlerta(ciudad.trim(), alerta, motivo, temperatura, lluvia);
            publicarAlertaKafka(ciudad.trim(), alerta, motivo, temperatura, lluvia);

            Map<String, Object> ack = new LinkedHashMap<>();
            ack.put("ok", true);
            ack.put("ciudad", ciudad.trim());
            ack.put("alerta", alerta);
            ack.put("mensaje", alerta ? "Alerta registrada; WM_Central parará la WS."
                    : "Alerta cancelada; WM_Central restaurará la WS.");
            responder(ex, 200, ack);
        });
    }

    // ------------------------------------------------------------------ SQLite

    private synchronized List<Map<String, Object>> leerEstaciones() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Statement s = conexion.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id_ws, ubicacion, estado, cant_agua, ultima_conexion FROM WS ORDER BY id_ws")) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("idWs", rs.getInt("id_ws"));
                m.put("ubicacion", rs.getString("ubicacion"));
                m.put("estado", rs.getString("estado"));
                m.put("litros", rs.getDouble("cant_agua"));
                m.put("ultimaConexion", rs.getString("ultima_conexion"));
                items.add(m);
            }
        } catch (Exception e) {
            System.err.println("API_Central: error leyendo WS: " + e.getMessage());
        }
        return items;
    }

    private synchronized List<Map<String, Object>> leerOperarios() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Statement s = conexion.createStatement();
             ResultSet rs = s.executeQuery("SELECT id_operario, nombre FROM Operario ORDER BY id_operario")) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("idOperario", rs.getInt("id_operario"));
                m.put("nombre", rs.getString("nombre"));
                items.add(m);
            }
        } catch (Exception e) {
            System.err.println("API_Central: error leyendo operarios: " + e.getMessage());
        }
        return items;
    }

    private synchronized List<Map<String, Object>> leerRiegos() {
        List<Map<String, Object>> items = new ArrayList<>();
        String sql = "SELECT request_id, operator_id, ws_id, duration_seconds, status, created_at, updated_at "
                + "FROM SolicitudRiego "
                + "WHERE UPPER(status) NOT IN ('COMPLETED','REJECTED','FAILED','CANCELLED') "
                + "ORDER BY updated_at DESC LIMIT 100";
        try (Statement s = conexion.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("requestId", rs.getString("request_id"));
                m.put("idOperario", rs.getInt("operator_id"));
                m.put("idWs", rs.getInt("ws_id"));
                m.put("duracionSegundos", rs.getInt("duration_seconds"));
                m.put("estado", rs.getString("status"));
                m.put("creado", rs.getString("created_at"));
                items.add(m);
            }
        } catch (Exception e) {
            System.err.println("API_Central: error leyendo riegos: " + e.getMessage());
        }
        return items;
    }

    private synchronized List<Map<String, Object>> leerClima() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Statement s = conexion.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT ciudad, alerta, motivo, temperatura, lluvia, actualizado FROM AlertaClima ORDER BY ciudad")) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ciudad", rs.getString("ciudad"));
                m.put("alerta", rs.getInt("alerta") != 0);
                m.put("motivo", rs.getString("motivo"));
                m.put("temperatura", rs.getDouble("temperatura"));
                m.put("lluvia", rs.getInt("lluvia") != 0);
                m.put("actualizado", rs.getString("actualizado"));
                items.add(m);
            }
        } catch (Exception e) {
            System.err.println("API_Central: error leyendo clima: " + e.getMessage());
        }
        return items;
    }

    private synchronized void guardarAlerta(String ciudad, boolean alerta, String motivo,
                                            double temperatura, boolean lluvia) {
        String sql = "INSERT INTO AlertaClima (ciudad, alerta, motivo, temperatura, lluvia, actualizado) "
                + "VALUES (?,?,?,?,?,?) "
                + "ON CONFLICT(ciudad) DO UPDATE SET alerta=excluded.alerta, motivo=excluded.motivo, "
                + "temperatura=excluded.temperatura, lluvia=excluded.lluvia, actualizado=excluded.actualizado";
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, ciudad);
            ps.setInt(2, alerta ? 1 : 0);
            ps.setString(3, motivo == null ? "" : motivo);
            ps.setDouble(4, Double.isFinite(temperatura) ? temperatura : 0.0);
            ps.setInt(5, lluvia ? 1 : 0);
            ps.setString(6, Instant.now().toString());
            ps.executeUpdate();
        } catch (Exception e) {
            System.err.println("API_Central: error guardando alerta: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ Kafka (opcional)

    private static boolean alertasKafkaHabilitadas() {
        String configurado = System.getenv("WM_ALERT_KAFKA");
        return configurado != null && Boolean.parseBoolean(configurado.trim());
    }

    private KafkaProducer<String, String> crearProductorAlertas() {
        String host = env("WM_KAFKA_HOST", "localhost").trim();
        int puerto = envInt("WM_KAFKA_PORT", 9092);
        String bootstrap = host.contains(":") && !host.startsWith("[")
                ? "[" + host + "]:" + puerto : host + ":" + puerto;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        return new KafkaProducer<>(p);
    }

    private void publicarAlertaKafka(String ciudad, boolean alerta, String motivo,
                                     double temperatura, boolean lluvia) {
        if (productorAlertas == null) return;
        try {
            Map<String, Object> carga = new LinkedHashMap<>();
            carga.put("type", alerta ? "ALERTA_CLIMA" : "CANCELAR_ALERTA");
            carga.put("ciudad", ciudad);
            carga.put("motivo", motivo == null ? "" : motivo);
            carga.put("temperatura", temperatura);
            carga.put("lluvia", lluvia);
            carga.put("createdAt", Instant.now().toString());
            String textoJson = JSON.writeValueAsString(carga);
            productorAlertas.send(new ProducerRecord<>("wm.central.alertas", ciudad, textoJson),
                    (metadatos, excepcion) -> {
                        if (excepcion != null) {
                            System.err.println("API_Central: no se pudo publicar la alerta en Kafka: "
                                    + excepcion.getMessage());
                        }
                    });
        } catch (Exception e) {
            System.err.println("API_Central: error publicando alerta en Kafka: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ HTTP (apoyo)

    private interface Accion {
        void ejecutar() throws IOException;
    }

    private void manejar(HttpExchange ex, String metodo, Accion accion) throws IOException {
        cors(ex);
        if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(204, -1);
            ex.close();
            return;
        }
        if (!metodo.equalsIgnoreCase(ex.getRequestMethod())) {
            responder(ex, 405, Map.of("error", "Método no permitido. Use " + metodo + "."));
            return;
        }
        try {
            accion.ejecutar();
        } catch (Exception e) {
            System.err.println("API_Central: error en " + ex.getRequestURI() + ": " + e.getMessage());
            try {
                responder(ex, 500, Map.of("error", e.getMessage() == null ? "Error interno" : e.getMessage()));
            } catch (IOException ignorada) { }
        }
    }

    private void cors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private void responder(HttpExchange ex, int codigo, Object objeto) throws IOException {
        byte[] bytes = objeto == null ? new byte[0] : JSON.writeValueAsBytes(objeto);
        cors(ex);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(codigo, bytes.length);
        if (bytes.length > 0) {
            try (OutputStream out = ex.getResponseBody()) {
                out.write(bytes);
            }
        }
        ex.close();
    }

    // ------------------------------------------------------------------ Utilidades

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static double numero(Object valor, double porDefecto) {
        if (valor instanceof Number) return ((Number) valor).doubleValue();
        if (valor != null) {
            try { return Double.parseDouble(String.valueOf(valor)); } catch (Exception e) { }
        }
        return porDefecto;
    }

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private static int envInt(String nombre, int porDefecto) {
        try { return Integer.parseInt(env(nombre, Integer.toString(porDefecto))); }
        catch (NumberFormatException e) { return porDefecto; }
    }

    private static String buscarRutaBaseDatos() {
        String[] candidatas = {
                "database/watermanagement.db",
                "../database/watermanagement.db",
                "../../database/watermanagement.db"
        };
        for (String candidata : candidatas) {
            if (Files.isRegularFile(Paths.get(candidata))) return candidata;
        }
        for (String candidata : candidatas) {
            Path padre = Paths.get(candidata).toAbsolutePath().normalize().getParent();
            if (padre != null && Files.isDirectory(padre)) return candidata;
        }
        return candidatas[0];
    }
}
