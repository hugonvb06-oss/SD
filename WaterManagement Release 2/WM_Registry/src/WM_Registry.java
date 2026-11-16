import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * WM_Registry: API REST segura para el alta, baja y autenticacion de Watering Stations.
 *
 * Endpoints:
 *   PUT    /api/registry/ws/{id}                Alta/renovacion de credenciales
 *   GET    /api/registry/ws/{id}                Consulta del registro
 *   POST   /api/registry/ws/{id}/authenticate   Verifica las credenciales
 *   DELETE /api/registry/ws/{id}                Baja logica de la WS
 *
 * La tabla WS se comparte con WM_Central. El token se devuelve una sola vez al dar de
 * alta la estacion; en SQLite solo se guarda su SHA-256, nunca el token en claro.
 */
public final class WM_Registry {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() { };
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int MAX_BODY_BYTES = 8192;

    private final RegistryStore store;
    private final HttpsServer server;

    private WM_Registry(String host, int port, String dbPath, String keyStorePath, char[] keyStorePassword)
            throws Exception {
        this.store = new RegistryStore(dbPath);
        SSLContext sslContext = crearContextoSSL(keyStorePath, keyStorePassword);
        this.server = HttpsServer.create(new InetSocketAddress(host, port), 0);
        this.server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(com.sun.net.httpserver.HttpsParameters params) {
                SSLParameters ssl = getSSLContext().getDefaultSSLParameters();
                ssl.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
                params.setSSLParameters(ssl);
            }
        });
        this.server.createContext("/api/registry/", new RegistryHandler(store));
        this.server.setExecutor(Executors.newFixedThreadPool(8));
    }

    public static void main(String[] args) {
        String host = env("WM_REGISTRY_HOST", "0.0.0.0");
        int port = envInt("WM_REGISTRY_PORT", 8443);
        String dbPath = env("WM_DB_PATH", "database/watermanagement.db");
        String keyStorePath = env("WM_REGISTRY_KEYSTORE", "secrets/registry.p12");
        String password = env("WM_REGISTRY_KEYSTORE_PASSWORD", "");

        if (password.isEmpty()) {
            System.err.println("Falta WM_REGISTRY_KEYSTORE_PASSWORD. No se inicia Registry sin HTTPS configurado.");
            System.exit(2);
        }

        try {
            WM_Registry registry = new WM_Registry(host, port, dbPath, keyStorePath, password.toCharArray());
            Runtime.getRuntime().addShutdownHook(new Thread(registry::stop, "wm-registry-shutdown"));
            registry.start();
            System.out.println("WM_Registry escuchando por HTTPS en " + host + ":" + port);
            System.out.println("SQLite compartida: " + dbPath);
        } catch (Exception e) {
            System.err.println("No se pudo iniciar WM_Registry: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private void start() { server.start(); }

    private void stop() {
        server.stop(1);
        store.close();
    }

    private static SSLContext crearContextoSSL(String ruta, char[] password) throws Exception {
        Path path = Paths.get(ruta);
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("No existe el keystore HTTPS: " + path.toAbsolutePath());
        }
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, new SecureRandom());
        return context;
    }

    private static final class RegistryHandler implements HttpHandler {
        private final RegistryStore store;

        RegistryHandler(RegistryStore store) { this.store = store; }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String method = exchange.getRequestMethod().toUpperCase();
                String[] parts = exchange.getRequestURI().getPath().split("/");
                // /api/registry/ws/{id} o /api/registry/ws/{id}/authenticate
                if (parts.length < 5 || !"api".equals(parts[1]) || !"registry".equals(parts[2])
                        || !"ws".equals(parts[3])) {
                    send(exchange, 404, error("Ruta no encontrada"));
                    return;
                }

                int wsId;
                try { wsId = Integer.parseInt(parts[4]); }
                catch (NumberFormatException e) { send(exchange, 400, error("El ID de WS debe ser un entero")); return; }
                if (wsId <= 0) { send(exchange, 400, error("El ID de WS debe ser positivo")); return; }

                boolean authenticateRoute = parts.length == 6 && "authenticate".equals(parts[5]);
                if (parts.length > 6 || (parts.length == 6 && !authenticateRoute)) {
                    send(exchange, 404, error("Ruta no encontrada"));
                    return;
                }

                if (authenticateRoute) {
                    if (!"POST".equals(method)) { methodNotAllowed(exchange, "POST"); return; }
                    Map<String, Object> body = readJson(exchange);
                    String credential = stringValue(body.get("credential"));
                    if (credential == null || credential.isEmpty()) {
                        send(exchange, 400, error("Falta el campo credential"));
                        return;
                    }
                    boolean valid = store.autenticar(wsId, credential);
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("authenticated", valid);
                    response.put("wsId", wsId);
                    send(exchange, valid ? 200 : 401, response);
                    return;
                }

                if ("PUT".equals(method)) {
                    Map<String, Object> body = readJson(exchange);
                    String location = stringValue(body.get("location"));
                    if (location == null || location.trim().isEmpty() || location.length() > 200) {
                        send(exchange, 400, error("location es obligatorio y debe tener como maximo 200 caracteres"));
                        return;
                    }
                    String credential = generarCredencial();
                    store.registrar(wsId, location.trim(), hash(credential));
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("registered", true);
                    response.put("wsId", wsId);
                    response.put("location", location.trim());
                    response.put("credential", credential);
                    response.put("message", "Guarde esta credencial: solo se devuelve durante el alta");
                    send(exchange, 201, response);
                    return;
                }

                if ("GET".equals(method)) {
                    Map<String, Object> ws = store.consultar(wsId);
                    if (ws == null) { send(exchange, 404, error("WS no registrada")); return; }
                    send(exchange, 200, ws);
                    return;
                }

                if ("DELETE".equals(method)) {
                    boolean removed = store.darDeBaja(wsId);
                    if (!removed) { send(exchange, 404, error("WS no encontrada")); return; }
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("registered", false);
                    response.put("wsId", wsId);
                    response.put("message", "WS dada de baja");
                    send(exchange, 200, response);
                    return;
                }

                methodNotAllowed(exchange, "GET, PUT, DELETE");
            } catch (IllegalArgumentException e) {
                send(exchange, 400, error(e.getMessage()));
            } catch (Exception e) {
                System.err.println("Error en WM_Registry: " + e.getMessage());
                send(exchange, 500, error("Error interno del Registry"));
            } finally {
                exchange.close();
            }
        }

        private static Map<String, Object> readJson(HttpExchange exchange) throws IOException {
            byte[] bytes = readLimited(exchange.getRequestBody(), MAX_BODY_BYTES);
            if (bytes.length == 0) throw new IllegalArgumentException("El cuerpo JSON es obligatorio");
            try { return JSON.readValue(bytes, MAP_TYPE); }
            catch (Exception e) { throw new IllegalArgumentException("JSON no valido"); }
        }

        private static byte[] readLimited(InputStream in, int max) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (out.size() + read > max) throw new IllegalArgumentException("Cuerpo demasiado grande");
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }

        private static String stringValue(Object value) {
            return value == null ? null : String.valueOf(value);
        }

        private static void methodNotAllowed(HttpExchange exchange, String allowed) throws IOException {
            exchange.getResponseHeaders().set("Allow", allowed);
            send(exchange, 405, error("Metodo no permitido"));
        }

        private static Map<String, Object> error(String message) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("error", message == null ? "Error" : message);
            return response;
        }

        private static void send(HttpExchange exchange, int status, Object response) throws IOException {
            byte[] bytes = JSON.writeValueAsBytes(response);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    /** Acceso SQLite sincronizado porque se comparte una unica conexion entre peticiones. */
    private static final class RegistryStore {
        private final Connection connection;

        RegistryStore(String dbPath) throws SQLException, IOException {
            Path path = Paths.get(dbPath);
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            try (Statement st = connection.createStatement()) {
                st.execute("PRAGMA busy_timeout=5000");
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA foreign_keys=ON");
                st.execute("CREATE TABLE IF NOT EXISTS WS (id_ws INTEGER PRIMARY KEY, ubicacion TEXT NOT NULL, ultima_conexion TEXT, estado TEXT NOT NULL DEFAULT 'DESCONECTADA', cant_agua REAL NOT NULL DEFAULT 0, credencial_hash TEXT, registrada INTEGER NOT NULL DEFAULT 0)");
            }
            ensureColumn("credencial_hash", "TEXT");
            ensureColumn("registrada", "INTEGER NOT NULL DEFAULT 0");
        }

        private void ensureColumn(String name, String definition) throws SQLException {
            boolean found = false;
            try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery("PRAGMA table_info(WS)")) {
                while (rs.next()) if (name.equalsIgnoreCase(rs.getString("name"))) found = true;
            }
            if (!found) try (Statement st = connection.createStatement()) {
                st.execute("ALTER TABLE WS ADD COLUMN " + name + " " + definition);
            }
        }

        synchronized void registrar(int id, String location, String credentialHash) throws SQLException {
            String now = Instant.now().toString();
            String sql = "INSERT INTO WS(id_ws,ubicacion,ultima_conexion,estado,cant_agua,credencial_hash,registrada) "
                    + "VALUES(?,?,?,'DESCONECTADA',0,?,1) "
                    + "ON CONFLICT(id_ws) DO UPDATE SET ubicacion=excluded.ubicacion, "
                    + "ultima_conexion=excluded.ultima_conexion, credencial_hash=excluded.credencial_hash, "
                    + "registrada=1, estado='DESCONECTADA'";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setInt(1, id); ps.setString(2, location); ps.setString(3, now); ps.setString(4, credentialHash);
                ps.executeUpdate();
            }
        }

        synchronized boolean autenticar(int id, String credential) throws Exception {
            String expected = null;
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT credencial_hash FROM WS WHERE id_ws=? AND registrada=1")) {
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) { if (rs.next()) expected = rs.getString(1); }
            }
            if (expected == null) return false;
            byte[] a = expected.getBytes(StandardCharsets.UTF_8);
            byte[] b = hash(credential).getBytes(StandardCharsets.UTF_8);
            return MessageDigest.isEqual(a, b);
        }

        synchronized Map<String, Object> consultar(int id) throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT id_ws,ubicacion,estado,registrada,ultima_conexion FROM WS WHERE id_ws=?")) {
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("wsId", rs.getInt("id_ws"));
                    response.put("location", rs.getString("ubicacion"));
                    response.put("state", rs.getString("estado"));
                    response.put("registered", rs.getInt("registrada") == 1);
                    response.put("lastConnection", rs.getString("ultima_conexion"));
                    return response;
                }
            }
        }

        synchronized boolean darDeBaja(int id) throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE WS SET registrada=0, credencial_hash=NULL, estado='DESCONECTADA' WHERE id_ws=?")) {
                ps.setInt(1, id);
                return ps.executeUpdate() > 0;
            }
        }

        synchronized void close() {
            try { connection.close(); } catch (SQLException e) {
                System.err.println("Error cerrando SQLite de WM_Registry: " + e.getMessage());
            }
        }
    }

    private static String generarCredencial() {
        byte[] token = new byte[32];
        RANDOM.nextBytes(token);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(token);
    }

    private static String hash(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) hex.append(String.format("%02x", b & 0xff));
        return hex.toString();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    private static int envInt(String name, int fallback) {
        try { return Integer.parseInt(env(name, Integer.toString(fallback))); }
        catch (NumberFormatException e) { return fallback; }
    }
}
