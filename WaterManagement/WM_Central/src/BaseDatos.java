import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Capa SQLite compartida por los hilos; los métodos están sincronizados. */
public final class BaseDatos implements AutoCloseable {
    private final Connection conexion;

    public BaseDatos(String rutaBD) {
        try {
            conexion = DriverManager.getConnection("jdbc:sqlite:" + rutaBD);
            try (Statement s = conexion.createStatement()) {
                s.execute("PRAGMA busy_timeout=5000");
                s.execute("PRAGMA journal_mode=WAL");
                s.execute("PRAGMA foreign_keys=ON");
                s.execute("CREATE TABLE IF NOT EXISTS WS (id_ws INTEGER PRIMARY KEY, ubicacion TEXT NOT NULL, ultima_conexion TEXT, estado TEXT NOT NULL DEFAULT 'DESCONECTADA', cant_agua REAL NOT NULL DEFAULT 0)");
                s.execute("CREATE TABLE IF NOT EXISTS Operario (id_operario INTEGER PRIMARY KEY, nombre TEXT NOT NULL)");
                s.execute("CREATE TABLE IF NOT EXISTS SolicitudRiego (request_id TEXT PRIMARY KEY, operator_id INTEGER NOT NULL, ws_id INTEGER NOT NULL, duration_seconds INTEGER NOT NULL, status TEXT NOT NULL, terminal_event TEXT, terminal_sent INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)");
            }
            ensureColumn("WS", "ultima_conexion", "TEXT");
            ensureColumn("WS", "estado", "TEXT NOT NULL DEFAULT 'DESCONECTADA'");
            ensureColumn("WS", "cant_agua", "REAL NOT NULL DEFAULT 0");
            ensureColumn("SolicitudRiego", "terminal_event", "TEXT");
            ensureColumn("SolicitudRiego", "terminal_sent", "INTEGER NOT NULL DEFAULT 0");
            System.out.println("Conectado con la base de datos SQLite: " + rutaBD);
        } catch (SQLException e) {
            throw new IllegalStateException("No se pudo abrir o preparar la base de datos SQLite: " + e.getMessage(), e);
        }
    }

    private void ensureColumn(String table, String column, String definition) throws SQLException {
        boolean found = false;
        try (Statement s = conexion.createStatement(); ResultSet rs = s.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) if (column.equalsIgnoreCase(rs.getString("name"))) found = true;
        }
        if (!found) try (Statement s = conexion.createStatement()) {
            s.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
    }

    public synchronized boolean registrarOActualizarWS(int id, String ubicacion) {
        if (id <= 0 || ubicacion == null || ubicacion.trim().isEmpty()) return false;
        if (existeWS(id)) {
            try (PreparedStatement ps = conexion.prepareStatement("UPDATE WS SET ubicacion=?, ultima_conexion=? WHERE id_ws=?")) {
                ps.setString(1, ubicacion.trim()); ps.setString(2, Instant.now().toString()); ps.setInt(3, id);
                ps.executeUpdate(); return true;
            } catch (SQLException e) { System.err.println("Error actualizando WS: " + e.getMessage()); return false; }
        }
        return registrarWS(id, ubicacion);
    }

    public synchronized boolean registrarWS(int id, String ubicacion) {
        String sql = "INSERT INTO WS (id_ws, ubicacion, ultima_conexion, estado, cant_agua) VALUES (?, ?, ?, 'DESCONECTADA', 0)";
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setInt(1, id); ps.setString(2, ubicacion); ps.setString(3, Instant.now().toString());
            ps.executeUpdate(); return true;
        } catch (SQLException e) { System.err.println("Error registrando WS: " + e.getMessage()); return false; }
    }

    public synchronized boolean existeWS(int id) {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT 1 FROM WS WHERE id_ws=?")) {
            ps.setInt(1, id); try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { System.err.println("Error buscando WS: " + e.getMessage()); return false; }
    }

    public synchronized boolean buscarWS(int id) {
        Map<String, Object> ws = obtenerWS(id);
        if (ws == null) return false;
        System.out.println("WS encontrada: ID=" + ws.get("wsId") + " | Ubicación=" + ws.get("location") +
                " | Estado=" + ws.get("state") + " | Agua=" + ws.get("volumeLitres"));
        return true;
    }

    public synchronized Map<String, Object> obtenerWS(int id) {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT id_ws, ubicacion, ultima_conexion, estado, cant_agua FROM WS WHERE id_ws=?")) {
            ps.setInt(1, id); try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("wsId", rs.getInt("id_ws")); m.put("location", rs.getString("ubicacion"));
                m.put("lastConnection", rs.getString("ultima_conexion")); m.put("state", rs.getString("estado"));
                m.put("volumeLitres", rs.getDouble("cant_agua")); return m;
            }
        } catch (SQLException e) { System.err.println("Error consultando WS: " + e.getMessage()); return null; }
    }

    public synchronized List<Map<String, Object>> listarWS() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Statement s = conexion.createStatement(); ResultSet rs = s.executeQuery("SELECT id_ws, ubicacion, ultima_conexion, estado, cant_agua FROM WS ORDER BY id_ws")) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("wsId", rs.getInt("id_ws")); m.put("location", rs.getString("ubicacion"));
                m.put("lastConnection", rs.getString("ultima_conexion")); m.put("state", rs.getString("estado"));
                m.put("volumeLitres", rs.getDouble("cant_agua"));
                String st = rs.getString("estado"); m.put("available", "ACTIVADA".equalsIgnoreCase(st)); items.add(m);
            }
        } catch (SQLException e) { System.err.println("Error listando WS: " + e.getMessage()); }
        return items;
    }

    public synchronized List<Map<String, Object>> listarWSDisponibles() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (PreparedStatement ps = conexion.prepareStatement("SELECT id_ws, ubicacion, estado FROM WS WHERE UPPER(estado)='ACTIVADA' ORDER BY id_ws"); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("wsId", rs.getInt("id_ws")); m.put("location", rs.getString("ubicacion"));
                m.put("state", rs.getString("estado")); m.put("available", true); items.add(m);
            }
        } catch (SQLException e) { System.err.println("Error listando WS disponibles: " + e.getMessage()); }
        return items;
    }

    public synchronized boolean actualizarEstadoWS(int id, String estado) {
        try (PreparedStatement ps = conexion.prepareStatement("UPDATE WS SET estado=?, ultima_conexion=? WHERE id_ws=?")) {
            ps.setString(1, estado); ps.setString(2, Instant.now().toString()); ps.setInt(3, id); return ps.executeUpdate() > 0;
        } catch (SQLException e) { System.err.println("Error actualizando estado de WS: " + e.getMessage()); return false; }
    }

    public synchronized boolean actualizarAguaWS(int id, double cantidad) {
        try (PreparedStatement ps = conexion.prepareStatement("UPDATE WS SET cant_agua=MAX(cant_agua,?), ultima_conexion=? WHERE id_ws=?")) {
            ps.setDouble(1, Math.max(0, cantidad)); ps.setString(2, Instant.now().toString()); ps.setInt(3, id); return ps.executeUpdate() > 0;
        } catch (SQLException e) { System.err.println("Error actualizando volumen de WS: " + e.getMessage()); return false; }
    }

    public synchronized void desconectarTodasWS() {
        try (PreparedStatement ps = conexion.prepareStatement("UPDATE WS SET estado='DESCONECTADA'")) {
            ps.executeUpdate(); System.out.println("Las WS registradas quedan DESCONECTADAS hasta que se reconecten y reporten su salud.");
        } catch (SQLException e) { System.err.println("Error marcando WS desconectadas: " + e.getMessage()); }
    }

    public synchronized boolean registrarOperario(int id, String nombre) {
        if (id <= 0 || nombre == null || nombre.trim().isEmpty()) return false;
        try (PreparedStatement ps = conexion.prepareStatement("INSERT INTO Operario (id_operario, nombre) VALUES (?, ?)")) {
            ps.setInt(1, id); ps.setString(2, nombre.trim()); ps.executeUpdate(); return true;
        } catch (SQLException e) { System.err.println("Error registrando operario: " + e.getMessage()); return false; }
    }

    public synchronized boolean existeOperario(int id) {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT 1 FROM Operario WHERE id_operario=?")) {
            ps.setInt(1, id); try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { System.err.println("Error buscando operario: " + e.getMessage()); return false; }
    }

    public synchronized boolean buscarOperario(int id) {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT id_operario,nombre FROM Operario WHERE id_operario=?")) {
            ps.setInt(1, id); try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return false;
                System.out.println("Operario " + rs.getInt("id_operario") + " | Nombre: " + rs.getString("nombre")); return true;
            }
        } catch (SQLException e) { System.err.println("Error buscando operario: " + e.getMessage()); return false; }
    }

    public synchronized List<Map<String, Object>> listarOperarios() {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Statement s = conexion.createStatement(); ResultSet rs = s.executeQuery("SELECT id_operario,nombre FROM Operario ORDER BY id_operario")) {
            while (rs.next()) { Map<String,Object> m=new LinkedHashMap<>(); m.put("operatorId",rs.getInt("id_operario")); m.put("name",rs.getString("nombre")); items.add(m); }
        } catch (SQLException e) { System.err.println("Error listando operarios: " + e.getMessage()); }
        return items;
    }

    public synchronized boolean guardarSolicitudSiNueva(String requestId, int operatorId, int wsId, int durationSeconds) {
        if (requestId == null || requestId.trim().isEmpty() || operatorId < 0 || wsId <= 0 ||
                durationSeconds < 1 || durationSeconds > 86400) {
            return false;
        }
        String sql = "INSERT OR IGNORE INTO SolicitudRiego(request_id,operator_id,ws_id,duration_seconds,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)";
        String now = Instant.now().toString();
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, requestId); ps.setInt(2, operatorId); ps.setInt(3, wsId); ps.setInt(4, durationSeconds);
            ps.setString(5, "RECEIVED"); ps.setString(6, now); ps.setString(7, now);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) { System.err.println("Error guardando solicitud: " + e.getMessage()); return false; }
    }

    public synchronized Map<String, Object> obtenerSolicitud(String requestId) {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT * FROM SolicitudRiego WHERE request_id=?")) {
            ps.setString(1, requestId); try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                Map<String,Object> m=new LinkedHashMap<>(); m.put("requestId",rs.getString("request_id"));
                m.put("operatorId",rs.getInt("operator_id")); m.put("wsId",rs.getInt("ws_id"));
                m.put("durationSeconds",rs.getInt("duration_seconds"));
                m.put("status",rs.getString("status"));
                m.put("terminalEvent",rs.getString("terminal_event")); m.put("createdAt",rs.getString("created_at")); return m;
            }
        } catch (SQLException e) { System.err.println("Error consultando solicitud: " + e.getMessage()); return null; }
    }

    public synchronized void actualizarEstadoSolicitud(String requestId, String estado, String terminalEventJson) {
        try (PreparedStatement ps = conexion.prepareStatement("UPDATE SolicitudRiego SET status=?, terminal_event=COALESCE(?,terminal_event), updated_at=? WHERE request_id=?")) {
            ps.setString(1, estado); ps.setString(2, terminalEventJson); ps.setString(3, Instant.now().toString()); ps.setString(4, requestId); ps.executeUpdate();
        } catch (SQLException e) { System.err.println("Error actualizando solicitud: " + e.getMessage()); }
    }

    public synchronized List<Map<String,Object>> listarSolicitudesNoTerminales() {
        List<Map<String,Object>> items = new ArrayList<>();
        String sql = "SELECT request_id,operator_id,ws_id,duration_seconds,status,created_at,updated_at FROM SolicitudRiego " +
                "WHERE UPPER(status) NOT IN ('COMPLETED','REJECTED','FAILED','CANCELLED')";
        try (PreparedStatement ps = conexion.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("requestId", rs.getString("request_id"));
                m.put("operatorId", rs.getInt("operator_id"));
                m.put("wsId", rs.getInt("ws_id"));
                m.put("durationSeconds", rs.getInt("duration_seconds"));
                m.put("status", rs.getString("status"));
                m.put("createdAt", rs.getString("created_at"));
                m.put("updatedAt", rs.getString("updated_at"));
                items.add(m);
            }
        } catch (SQLException e) { System.err.println("Error consultando solicitudes pendientes: " + e.getMessage()); }
        return items;
    }

    public synchronized void marcarEventoTerminalEnviado(String requestId) {
        try (PreparedStatement ps = conexion.prepareStatement("UPDATE SolicitudRiego SET terminal_sent=1, updated_at=? WHERE request_id=?")) {
            ps.setString(1, Instant.now().toString()); ps.setString(2, requestId); ps.executeUpdate();
        } catch (SQLException e) { System.err.println("Error marcando evento terminal enviado: " + e.getMessage()); }
    }

    public synchronized List<Map<String,Object>> listarEventosTerminalesPendientes() {
        List<Map<String,Object>> items = new ArrayList<>();
        try (PreparedStatement ps = conexion.prepareStatement("SELECT request_id,operator_id,terminal_event FROM SolicitudRiego WHERE terminal_event IS NOT NULL AND terminal_sent=0");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("requestId", rs.getString("request_id"));
                m.put("operatorId", rs.getInt("operator_id"));
                m.put("terminalEvent", rs.getString("terminal_event"));
                items.add(m);
            }
        } catch (SQLException e) { System.err.println("Error listando eventos terminales pendientes: " + e.getMessage()); }
        return items;
    }

    @Override public synchronized void close() {
        try { if (conexion != null && !conexion.isClosed()) conexion.close(); }
        catch (SQLException e) { System.err.println("Error cerrando SQLite: " + e.getMessage()); }
    }
}
