# Anexo — Código fuente

Código fuente completo de todos los componentes, en el mismo orden en que

se describen en la memoria. Codificación UTF-8.


## WM_Central/src/BaseDatos.java

```java
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
            asegurarColumna("WS", "ultima_conexion", "TEXT");
            asegurarColumna("WS", "estado", "TEXT NOT NULL DEFAULT 'DESCONECTADA'");
            asegurarColumna("WS", "cant_agua", "REAL NOT NULL DEFAULT 0");
            asegurarColumna("SolicitudRiego", "terminal_event", "TEXT");
            asegurarColumna("SolicitudRiego", "terminal_sent", "INTEGER NOT NULL DEFAULT 0");
            System.out.println("Conectado con la base de datos SQLite: " + rutaBD);
        } catch (SQLException e) {
            throw new IllegalStateException("No se pudo abrir o preparar la base de datos SQLite: " + e.getMessage(), e);
        }
    }

    private void asegurarColumna(String tabla, String columna, String definicion) throws SQLException {
        boolean encontrada = false;
        try (Statement s = conexion.createStatement(); ResultSet rs = s.executeQuery("PRAGMA table_info(" + tabla + ")")) {
            while (rs.next()) if (columna.equalsIgnoreCase(rs.getString("name"))) encontrada = true;
        }
        if (!encontrada) try (Statement s = conexion.createStatement()) {
            s.execute("ALTER TABLE " + tabla + " ADD COLUMN " + columna + " " + definicion);
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

```


## WM_Central/src/CentralFrame.java

```java
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.awt.Rectangle;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Panel gráfico de WM_Central.
 *
 * Cada estación se muestra como una tarjeta con el color exigido:
 * Available (verde), Watering (verde parpadeante, con caudal, volumen acumulado y
 * operario), Leak (rojo), Out of Service (naranja) y Disconnected (gris).
 * El panel administrativo permite registrar operarios, iniciar riegos y enviar
 * órdenes de bloqueo, activación y parada sin usar la consola.
 */
final class CentralFrame extends JFrame {

    private static final Color VERDE = new Color(0x2E7D32);
    private static final Color RIEGO_A = new Color(0x1B5E20);
    private static final Color RIEGO_B = new Color(0x4CAF50);
    private static final Color ROJO = new Color(0xC62828);
    private static final Color NARANJA = new Color(0xEF6C00);
    private static final Color GRIS = new Color(0x616161);
    private static final Color DESCONOCIDO = new Color(0x424242);

    private static final Color FONDO = new Color(0x1E1E1E);
    private static final Color FONDO_PANEL = new Color(0x252526);
    private static final Color TEXTO = new Color(0xF2F2F2);
    private static final Color ATENUADO = new Color(0xBBBBBB);

    private final CentralService central;
    private final JPanel panelEstaciones = new JPanel();
    private final Map<Integer, TarjetaEstacion> tarjetas = new LinkedHashMap<>();
    private final JLabel etiquetaResumen = new JLabel(" ");
    private final JLabel etiquetaEstado = new JLabel("Listo.");
    private final JLabel etiquetaVacia = new JLabel("No hay estaciones WS registradas.", SwingConstants.CENTER);

    private final JTextField campoIdOperario = new JTextField(6);
    private final JTextField campoNombreOperario = new JTextField(14);
    private final JTextField campoWsRiego = new JTextField("0", 6);
    private final JTextField campoDuracionRiego = new JTextField("10", 6);
    private final JTextField campoWsControl = new JTextField("0", 6);

    private boolean parpadeo;
    private boolean vaciaMostrada;

    CentralFrame(CentralService central, int puertoSocket, String anfitrionKafka, int puertoKafka, String rutaBd) {
        super("WM_Central - Panel de estaciones");
        this.central = central;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                int opcion = JOptionPane.showConfirmDialog(CentralFrame.this,
                        "¿Cerrar WM_Central? Se detendrá el servicio de la central.",
                        "Cerrar WM_Central", JOptionPane.YES_NO_OPTION);
                if (opcion == JOptionPane.YES_OPTION) {
                    dispose();
                    System.exit(0);
                }
            }
        });

        getContentPane().setBackground(FONDO);
        setLayout(new BorderLayout());

        add(cabecera(puertoSocket, anfitrionKafka, puertoKafka, rutaBd), BorderLayout.NORTH);
        add(panelCentral(), BorderLayout.CENTER);
        add(panelAdmin(), BorderLayout.EAST);
        add(panelSur(), BorderLayout.SOUTH);

        etiquetaVacia.setForeground(ATENUADO);
        etiquetaVacia.setFont(new Font("SansSerif", Font.PLAIN, 15));

        Rectangle pantalla = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(1180, Math.max(900, pantalla.width - 40)),
                Math.min(760, Math.max(520, pantalla.height - 40)));
        setMinimumSize(new Dimension(860, 500));
        setLocationRelativeTo(null);

        refrescar();
        new Timer(1000, e -> refrescar()).start();
        new Timer(500, e -> {
            parpadeo = !parpadeo;
            for (TarjetaEstacion tarjeta : tarjetas.values()) tarjeta.pintarEstado(parpadeo);
        }).start();
    }

    // ------------------------------------------------------------------ Layout

    private JComponent cabecera(int puertoSocket, String anfitrionKafka, int puertoKafka, String rutaBd) {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel titulo = new JLabel("WM_Central");
        titulo.setFont(new Font("SansSerif", Font.BOLD, 24));
        titulo.setForeground(TEXTO);

        JLabel subtitulo = new JLabel("Socket " + puertoSocket + "  ·  Kafka " + anfitrionKafka + ":" + puertoKafka
                + "  ·  SQLite " + rutaBd);
        subtitulo.setFont(new Font("SansSerif", Font.PLAIN, 12));
        subtitulo.setForeground(ATENUADO);

        JPanel izquierda = new JPanel();
        izquierda.setOpaque(false);
        izquierda.setLayout(new BoxLayout(izquierda, BoxLayout.Y_AXIS));
        izquierda.add(titulo);
        izquierda.add(Box.createVerticalStrut(3));
        izquierda.add(subtitulo);

        etiquetaResumen.setFont(new Font("SansSerif", Font.BOLD, 14));
        etiquetaResumen.setForeground(TEXTO);
        etiquetaResumen.setHorizontalAlignment(SwingConstants.RIGHT);

        panel.add(izquierda, BorderLayout.WEST);
        panel.add(etiquetaResumen, BorderLayout.EAST);
        return panel;
    }

    private JComponent panelCentral() {
        panelEstaciones.setLayout(new GridLayout(0, 3, 14, 14));
        panelEstaciones.setBackground(FONDO);
        panelEstaciones.setBorder(new EmptyBorder(14, 14, 14, 14));

        JScrollPane desplazamiento = new JScrollPane(panelEstaciones);
        desplazamiento.setBorder(BorderFactory.createEmptyBorder());
        desplazamiento.getViewport().setBackground(FONDO);
        desplazamiento.getVerticalScrollBar().setUnitIncrement(18);
        return desplazamiento;
    }

    private JComponent panelAdmin() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(12, 12, 12, 12));
        panel.setPreferredSize(new Dimension(340, 0));

        JButton botonRegistrar = new JButton("Registrar operario");
        botonRegistrar.addActionListener(e -> registrarOperario());
        JButton botonVerOperarios = new JButton("Ver operarios");
        botonVerOperarios.addActionListener(e -> mostrarOperarios());

        JPanel formularioOperarios = formulario(
                etiqueta("ID:"), campoIdOperario,
                etiqueta("Nombre:"), campoNombreOperario);
        JPanel botonesOperarios = botones(botonRegistrar, botonVerOperarios);
        panel.add(seccion("Operarios", formularioOperarios, botonesOperarios));
        panel.add(Box.createVerticalStrut(10));

        JButton botonRegar = new JButton("Iniciar riego");
        botonRegar.addActionListener(e -> iniciarRiego());
        JPanel formularioRiego = formulario(
                etiqueta("WS (0 = todas):"), campoWsRiego,
                etiqueta("Duración (s):"), campoDuracionRiego);
        panel.add(seccion("Riego administrativo", formularioRiego, botones(botonRegar)));
        panel.add(Box.createVerticalStrut(10));

        JButton botonBloquear = new JButton("Bloquear");
        botonBloquear.addActionListener(e -> enviarControl("BLOCK_WS", "bloqueo"));
        JButton botonActivar = new JButton("Activar");
        botonActivar.addActionListener(e -> enviarControl("ACTIVATE_WS", "activación"));
        JButton botonParar = new JButton("Parar riego");
        botonParar.addActionListener(e -> enviarControl("STOP_IRRIGATION", "parada"));
        JPanel formularioControl = formulario(etiqueta("WS (0 = todas):"), campoWsControl);
        panel.add(seccion("Control de estaciones", formularioControl,
                botones(botonBloquear, botonActivar, botonParar)));
        panel.add(Box.createVerticalStrut(10));

        JButton botonActualizar = new JButton("Actualizar panel");
        botonActualizar.addActionListener(e -> {
            refrescarAhora();
            estado("Panel actualizado.");
        });
        panel.add(seccion("Vista", botones(botonActualizar)));

        panel.add(Box.createVerticalGlue());
        JLabel sugerencia = new JLabel("<html><div style='width:280px'>Las órdenes se entregan a la "
                + "estación en la siguiente comprobación de estado (1 s).</div></html>");
        sugerencia.setForeground(ATENUADO);
        sugerencia.setFont(new Font("SansSerif", Font.PLAIN, 11));
        sugerencia.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.add(sugerencia);
        return panel;
    }

    private JComponent panelSur() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(2, 10, 4, 10));
        panel.add(leyenda(), BorderLayout.NORTH);

        etiquetaEstado.setForeground(ATENUADO);
        etiquetaEstado.setFont(new Font("SansSerif", Font.PLAIN, 12));
        etiquetaEstado.setBorder(new EmptyBorder(0, 6, 4, 6));
        panel.add(etiquetaEstado, BorderLayout.SOUTH);
        return panel;
    }

    private JComponent leyenda() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 4));
        panel.setOpaque(false);
        panel.add(distintivo("Available", VERDE));
        panel.add(distintivo("Watering", RIEGO_A));
        panel.add(distintivo("Leak", ROJO));
        panel.add(distintivo("Out of Service", NARANJA));
        panel.add(distintivo("Disconnected", GRIS));
        return panel;
    }

    private JComponent distintivo(String texto, Color color) {
        JLabel etiqueta = new JLabel(" " + texto + " ");
        etiqueta.setOpaque(true);
        etiqueta.setBackground(color);
        etiqueta.setForeground(Color.WHITE);
        etiqueta.setFont(new Font("SansSerif", Font.BOLD, 11));
        etiqueta.setBorder(BorderFactory.createEmptyBorder(2, 6, 2, 6));
        return etiqueta;
    }

    private JPanel seccion(String titulo, JComponent... contenidos) {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(0x505050)), titulo,
                javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.TOP,
                new Font("SansSerif", Font.BOLD, 12), TEXTO));
        for (JComponent contenido : contenidos) {
            contenido.setAlignmentX(Component.LEFT_ALIGNMENT);
            panel.add(contenido);
            panel.add(Box.createVerticalStrut(5));
        }
        panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, panel.getPreferredSize().height));
        return panel;
    }

    private JPanel formulario(JComponent... pares) {
        JPanel panel = new JPanel(new GridLayout(0, 2, 6, 4));
        panel.setOpaque(false);
        for (JComponent componente : pares) panel.add(componente);
        return panel;
    }

    private JPanel botones(JButton... botones) {
        JPanel panel = new JPanel(new GridLayout(1, 0, 6, 0));
        panel.setOpaque(false);
        for (JButton boton : botones) panel.add(boton);
        return panel;
    }

    private JLabel etiqueta(String texto) {
        JLabel etiqueta = new JLabel(texto);
        etiqueta.setForeground(TEXTO);
        etiqueta.setFont(new Font("SansSerif", Font.PLAIN, 12));
        return etiqueta;
    }

    // ------------------------------------------------------------------ Refresco

    private void refrescarAhora() {
        refrescar();
    }

    private void refrescar() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refrescar);
            return;
        }

        List<CentralService.VistaEstacion> estaciones = central.vistaEstaciones();
        Set<Integer> vistos = new HashSet<>();
        int enLinea = 0;
        int regando = 0;

        for (CentralService.VistaEstacion estacion : estaciones) {
            vistos.add(estacion.idWs);
            if (estacion.enLinea) enLinea++;
            if ("REGANDO".equalsIgnoreCase(estacion.estado)) regando++;

            TarjetaEstacion tarjeta = tarjetas.get(estacion.idWs);
            if (tarjeta == null) {
                tarjeta = new TarjetaEstacion();
                tarjetas.put(estacion.idWs, tarjeta);
                panelEstaciones.add(tarjeta);
            }
            tarjeta.aplicar(estacion);
            tarjeta.pintarEstado(parpadeo);
        }

        for (Integer id : new ArrayList<>(tarjetas.keySet())) {
            if (!vistos.contains(id)) panelEstaciones.remove(tarjetas.remove(id));
        }

        if (estaciones.isEmpty()) {
            if (!vaciaMostrada) {
                panelEstaciones.add(etiquetaVacia);
                vaciaMostrada = true;
            }
        } else if (vaciaMostrada) {
            panelEstaciones.remove(etiquetaVacia);
            vaciaMostrada = false;
        }

        etiquetaResumen.setText(estaciones.size() + " estaciones  ·  " + enLinea + " en línea  ·  "
                + regando + " regando");
        panelEstaciones.revalidate();
        panelEstaciones.repaint();
    }

    // ------------------------------------------------------------------ Acciones

    private void registrarOperario() {
        int id = leerEntero(campoIdOperario, "ID de operario", true);
        if (id < 0) return;
        String nombre = campoNombreOperario.getText().trim();
        if (nombre.isEmpty()) {
            error("Introduce el nombre del operario.");
            return;
        }
        boolean creado = central.registrarOperario(id, nombre);
        if (creado) {
            campoNombreOperario.setText("");
            estado("Operario " + id + " registrado.");
        } else {
            error("No se pudo registrar el operario (¿ID duplicado?).");
        }
    }

    private void mostrarOperarios() {
        List<Map<String, Object>> operarios = central.listarOperarios();
        DefaultTableModel modelo = new DefaultTableModel(new Object[]{"ID", "Nombre"}, 0) {
            @Override public boolean isCellEditable(int fila, int columna) { return false; }
        };
        for (Map<String, Object> operario : operarios) {
            modelo.addRow(new Object[]{operario.get("operatorId"), operario.get("name")});
        }
        JTable tabla = new JTable(modelo);
        tabla.setRowHeight(24);
        JScrollPane desplazamiento = new JScrollPane(tabla);
        desplazamiento.setPreferredSize(new Dimension(340, Math.max(180, Math.min(420, 40 + operarios.size() * 26))));

        JDialog dialogo = new JDialog(this, "Operarios registrados", true);
        dialogo.setLayout(new BorderLayout());
        dialogo.add(desplazamiento, BorderLayout.CENTER);
        JButton cerrar = new JButton("Cerrar");
        cerrar.addActionListener(e -> dialogo.dispose());
        JPanel pie = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        pie.add(cerrar);
        dialogo.add(pie, BorderLayout.SOUTH);
        dialogo.pack();
        dialogo.setLocationRelativeTo(this);
        dialogo.setVisible(true);
    }

    private void iniciarRiego() {
        int idWs = leerEntero(campoWsRiego, "ID de WS", false);
        if (idWs < 0) return;
        int duracion = leerEntero(campoDuracionRiego, "Duración", true);
        if (duracion < 0) return;
        if (duracion > 86400) {
            error("La duración máxima es 86400 segundos.");
            return;
        }
        boolean encolado = central.adminRiego(idWs, duracion);
        if (encolado) estado("Riego encolado para " + (idWs == 0 ? "todas las estaciones conectadas" : "WS " + idWs)
                + " (" + duracion + " s).");
        else error("No se pudo iniciar el riego: la estación no está conectada y disponible.");
        refrescarAhora();
    }

    private void enviarControl(String tipo, String descripcion) {
        int idWs = leerEntero(campoWsControl, "ID de WS", false);
        if (idWs < 0) return;
        boolean encolado = central.adminOrden(tipo, idWs);
        if (encolado) estado("Orden de " + descripcion + " encolada para "
                + (idWs == 0 ? "todas las estaciones conectadas" : "WS " + idWs) + ".");
        else error("No hay estaciones conectadas con estado reciente para esa orden.");
        refrescarAhora();
    }

    private int leerEntero(JTextField campo, String nombre, boolean positivo) {
        try {
            int valor = Integer.parseInt(campo.getText().trim());
            if (positivo ? valor > 0 : valor >= 0) return valor;
        } catch (NumberFormatException ignorada) {
            // Se informa abajo con un mensaje uniforme.
        }
        error(nombre + ": introduce un entero " + (positivo ? "positivo." : "mayor o igual que cero."));
        return -1;
    }

    private void estado(String mensaje) {
        etiquetaEstado.setText(mensaje);
    }

    private void error(String mensaje) {
        etiquetaEstado.setText(mensaje);
        JOptionPane.showMessageDialog(this, mensaje, "WM_Central", JOptionPane.WARNING_MESSAGE);
    }

    // ------------------------------------------------------------------ Tarjeta

    private static final class TarjetaEstacion extends JPanel {
        private final JLabel etiquetaId = new JLabel();
        private final JLabel etiquetaUbicacion = new JLabel();
        private final JLabel etiquetaEstado = new JLabel();
        private final JLabel etiquetaVolumen = new JLabel();
        private final JLabel detalle1 = new JLabel(" ");
        private final JLabel detalle2 = new JLabel(" ");
        private final JLabel detalle3 = new JLabel(" ");
        private String estado = "DESCONECTADA";

        TarjetaEstacion() {
            super(new BorderLayout(0, 7));
            setOpaque(true);
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(new Color(0, 0, 0, 90), 1, true),
                    new EmptyBorder(14, 16, 14, 16)));

            etiquetaId.setFont(new Font("SansSerif", Font.BOLD, 16));
            etiquetaUbicacion.setFont(new Font("SansSerif", Font.PLAIN, 12));
            etiquetaEstado.setFont(new Font("SansSerif", Font.BOLD, 23));
            etiquetaVolumen.setFont(new Font("SansSerif", Font.BOLD, 13));
            Font fuenteDetalle = new Font("SansSerif", Font.PLAIN, 12);
            detalle1.setFont(fuenteDetalle);
            detalle2.setFont(fuenteDetalle);
            detalle3.setFont(fuenteDetalle);

            for (JLabel etiqueta : new JLabel[]{etiquetaId, etiquetaUbicacion, etiquetaEstado, etiquetaVolumen,
                    detalle1, detalle2, detalle3}) {
                etiqueta.setForeground(Color.WHITE);
            }
            etiquetaUbicacion.setHorizontalAlignment(SwingConstants.RIGHT);

            JPanel superior = new JPanel(new BorderLayout(8, 0));
            superior.setOpaque(false);
            superior.add(etiquetaId, BorderLayout.WEST);
            superior.add(etiquetaUbicacion, BorderLayout.CENTER);

            JPanel medio = new JPanel(new BorderLayout(0, 2));
            medio.setOpaque(false);
            medio.add(etiquetaEstado, BorderLayout.NORTH);
            medio.add(etiquetaVolumen, BorderLayout.SOUTH);

            JPanel detalles = new JPanel(new GridLayout(0, 1, 0, 1));
            detalles.setOpaque(false);
            detalles.add(detalle1);
            detalles.add(detalle2);
            detalles.add(detalle3);

            add(superior, BorderLayout.NORTH);
            add(medio, BorderLayout.CENTER);
            add(detalles, BorderLayout.SOUTH);
        }

        void aplicar(CentralService.VistaEstacion estacion) {
            etiquetaId.setText("WS " + estacion.idWs);
            etiquetaUbicacion.setText(estacion.ubicacion == null ? "" : estacion.ubicacion);
            etiquetaUbicacion.setToolTipText(estacion.ubicacion);
            estado = estacion.estado == null ? "DESCONECTADA" : estacion.estado.toUpperCase(Locale.ROOT);
            etiquetaEstado.setText(etiquetaEstado(estado));
            etiquetaVolumen.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", estacion.litros));

            switch (estado) {
                case "REGANDO":
                    detalle1.setText(String.format(Locale.ROOT, "Caudal: %.2f L/min", estacion.caudalLMin));
                    detalle2.setText(String.format(Locale.ROOT, "Servicio: %.2f L  ·  Tiempo: %d s",
                            estacion.litrosServicio, estacion.segundos));
                    detalle3.setText("Operario: " + (estacion.idOperario > 0 ? estacion.idOperario : "-"));
                    break;
                case "ACTIVADA":
                    detalle1.setText("Disponible para riego");
                    detalle2.setText(" ");
                    detalle3.setText(" ");
                    break;
                case "FUGA":
                    detalle1.setText("Fuga/avería detectada");
                    detalle2.setText("Riego detenido");
                    detalle3.setText(" ");
                    break;
                case "FUERA_SERVICIO":
                    detalle1.setText("Estación bloqueada");
                    detalle2.setText("No acepta nuevos riegos");
                    detalle3.setText(" ");
                    break;
                default:
                    detalle1.setText("Sin conexión con el monitor");
                    detalle2.setText(" ");
                    detalle3.setText(" ");
                    break;
            }
            if (estacion.controlPendiente != null) {
                detalle3.setText("Orden pendiente: " + estacion.controlPendiente);
            }
            setToolTipText(estacion.idSolicitud == null ? null : "requestId: " + estacion.idSolicitud);
        }

        void pintarEstado(boolean parpadeo) {
            switch (estado) {
                case "ACTIVADA": setBackground(VERDE); break;
                case "REGANDO": setBackground(parpadeo ? RIEGO_B : RIEGO_A); break;
                case "FUGA": setBackground(ROJO); break;
                case "FUERA_SERVICIO": setBackground(NARANJA); break;
                case "DESCONECTADA": setBackground(GRIS); break;
                default: setBackground(DESCONOCIDO); break;
            }
        }

        private static String etiquetaEstado(String estado) {
            switch (estado) {
                case "ACTIVADA": return "Available";
                case "REGANDO": return "Watering";
                case "FUGA": return "Leak";
                case "FUERA_SERVICIO": return "Out of Service";
                case "DESCONECTADA": return "Disconnected";
                default: return estado;
            }
        }
    }
}

```


## WM_Central/src/CentralService.java

```java
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Lógica de CENTRAL: sockets de monitorización, coordinación de riegos y Kafka. */
public final class CentralService implements AutoCloseable {
    public static final String TOPICO_SOLICITUDES = "wm.riego.solicitudes";
    public static final String TOPICO_RESPUESTAS = "wm.riego.respuestas";
    public static final String TOPICO_LATIDO = "wm.central.latido";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TIPO_MAPA = new TypeReference<Map<String, Object>>() { };
    private static final long TIMEOUT_ACK_INICIO_MS = 15_000;
    private static final long SEGUNDOS_EXTRA_TIMEOUT_SOLICITUD = 45;
    private static final long ANTIGUEDAD_MAXIMA_ESTADO_MS = 3_500;

    private final BaseDatos db;
    private final String servidoresKafka;
    private final KafkaProducer<String, String> productor;
    private final AtomicBoolean ejecutando = new AtomicBoolean(true);
    private final ConcurrentMap<Integer, HiloCentral> sesiones = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, InstantaneaEstacion> ultimosEstados = new ConcurrentHashMap<>();
    private volatile String firmaPanel = "";
    private final ConcurrentMap<Integer, RiegoActivo> activosPorWs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RiegoActivo> activosPorSolicitud = new ConcurrentHashMap<>();
    // Bloquea nuevos riegos mientras una orden administrativa espera confirmación de la WS.
    private final ConcurrentMap<Integer, ControlPendiente> controlesPendientes = new ConcurrentHashMap<>();
    // Órdenes pendientes de entregar a cada WS. Viajan dentro de la respuesta al STATUS
    // del monitor, de modo que no depende de Kafka y no se pierde ninguna transacción.
    private final ConcurrentMap<Integer, ConcurrentLinkedQueue<String>> comandosPendientes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService planificador = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "wm-central-supervision"); t.setDaemon(true); return t;
    });
    // Cola acotada para que una caída prolongada de Kafka no acumule eventos sin límite.
    // Si se llena, la notificación terminal permanece pendiente en SQLite y se reintentará.
    private final ExecutorService buzonSalida = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1000), r -> {
                Thread t = new Thread(r, "wm-central-kafka-buzon"); t.setDaemon(true); return t;
            }, new ThreadPoolExecutor.AbortPolicy());
    private final ConcurrentMap<String, Boolean> eventosTerminalesEnVuelo = new ConcurrentHashMap<>();
    private Thread hiloKafka;

    public CentralService(BaseDatos db, String ipBroker, int puertoBroker) {
        this.db = db;
        this.servidoresKafka = formatearBootstrap(ipBroker, puertoBroker);
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "wm-central-productor");
        productor = new KafkaProducer<>(p);
    }

    private static String formatearBootstrap(String anfitrion, int puerto) {
        String h = anfitrion.trim();
        if (h.contains(":") && !h.startsWith("[")) h = "[" + h + "]";
        return h + ":" + puerto;
    }

    public void iniciar() {
        hiloKafka = new Thread(this::bucleConsumoKafka, "wm-central-kafka-consumidor");
        hiloKafka.setDaemon(true);
        hiloKafka.start();
        planificador.scheduleAtFixedRate(this::comprobarTimeouts, 1, 1, TimeUnit.SECONDS);
        planificador.scheduleAtFixedRate(this::reintentarEventosTerminales, 5, 5, TimeUnit.SECONDS);
        planificador.scheduleAtFixedRate(this::reconciliarSolicitudesHuerfanas, 20, 5, TimeUnit.SECONDS);
        planificador.scheduleAtFixedRate(this::publicarLatido, 0, 2, TimeUnit.SECONDS);
    }

    /**
     * Permite o deniega el registro de una WS. Por defecto se acepta cualquier WS con
     * datos válidos; si se define WM_WS_ALLOWED_IDS (p. ej. "1,2,3") o
     * WM_WS_REQUIRE_KNOWN=true solo se admiten las autorizadas.
     */
    public boolean puedeRegistrar(int id) {
        String obliga = System.getenv("WM_WS_REQUIRE_KNOWN");
        if (obliga != null && Boolean.parseBoolean(obliga.trim()) && !db.existeWS(id)) return false;
        String permitidas = System.getenv("WM_WS_ALLOWED_IDS");
        if (permitidas == null || permitidas.trim().isEmpty()) return true;
        for (String token : permitidas.split("[,;\\s]+")) {
            try {
                if (Integer.parseInt(token.trim()) == id) return true;
            } catch (NumberFormatException ignorada) { }
        }
        return false;
    }

    /** Extrae las órdenes pendientes de una WS para adjuntarlas a su próxima respuesta. */
    public List<String> tomarComandos(int idWs) {
        ConcurrentLinkedQueue<String> cola = comandosPendientes.get(idWs);
        List<String> resultado = new ArrayList<>();
        if (cola == null) return resultado;
        String comando;
        while ((comando = cola.poll()) != null) resultado.add(comando);
        return resultado;
    }

    /** Devuelve a la cola las órdenes que no se pudieron entregar (fallo del socket). */
    public void reencolarComandos(int idWs, List<String> comandos) {
        if (comandos == null || comandos.isEmpty()) return;
        ConcurrentLinkedQueue<String> cola = comandosPendientes.computeIfAbsent(idWs, k -> new ConcurrentLinkedQueue<>());
        for (String comando : comandos) cola.offer(comando);
    }

    /** Descarta órdenes obsoletas al conectar/desconectar una WS. */
    public void limpiarComandos(int idWs) {
        ConcurrentLinkedQueue<String> cola = comandosPendientes.get(idWs);
        if (cola != null) cola.clear();
    }

    private void encolarComando(int idWs, String tipo, String idSolicitud, int idOperario, int duracionSegundos) {
        String comando = tipo + "|" + (idSolicitud == null ? "" : idSolicitud) + "|" + idOperario + "|" + duracionSegundos;
        comandosPendientes.computeIfAbsent(idWs, k -> new ConcurrentLinkedQueue<>()).offer(comando);
    }

    public boolean registrarEstacion(int id, String ubicacion, double caudalLMin, HiloCentral sesion) {
        if (id <= 0 || !Double.isFinite(caudalLMin) || caudalLMin < 0 || ubicacion == null || ubicacion.trim().isEmpty()) return false;
        if (!db.registrarOActualizarWS(id, ubicacion)) return false;
        limpiarComandos(id);
        HiloCentral anterior = sesiones.put(id, sesion);
        if (anterior != null && anterior != sesion) {
            RiegoActivo interrumpido = activosPorWs.get(id);
            if (interrumpido != null) {
                finalizarServicio(interrumpido, "FAILED", "La conexión del monitor se ha sustituido durante el servicio; resultado no seguro.");
                publicarParadaSeguridad(id, "STOP_IRRIGATION");
            }
            anterior.cerrarDesdeReemplazo();
        }
        // No reutilizar el último estado de una conexión anterior: hay que esperar
        // la primera trama STATUS de esta sesión antes de autorizar nuevos riegos.
        ultimosEstados.remove(id);
        db.actualizarEstadoWS(id, "DESCONECTADA");
        System.out.println("WS " + id + " conectada desde " + sesion.direccionRemota() + " (" + ubicacion + "); esperando primer estado válido.");
        return true;
    }

    public void desregistrarEstacion(int id, HiloCentral sesion) {
        boolean eliminada = sesiones.remove(id, sesion);
        if (!eliminada) return; // una conexión más nueva ya sustituyó esta sesión
        limpiarComandos(id);
        db.actualizarEstadoWS(id, "DESCONECTADA");
        InstantaneaEstacion anterior = ultimosEstados.get(id);
        if (anterior != null) ultimosEstados.put(id, anterior.conEstado("DESCONECTADA"));
        System.err.println("WS " + id + " desconectada; queda marcada DESCONECTADA.");
        RiegoActivo activo = activosPorWs.get(id);
        if (activo != null) {
            finalizarServicio(activo, "FAILED", "La estación se ha desconectado durante el servicio.");
            publicarParadaSeguridad(id, "STOP_IRRIGATION");
        }
    }

    public boolean alRecibirEstado(int idWs, String estadoBruto, double caudalLMin, double litros,
                                   long segundos, String idSolicitud, int idOperario, HiloCentral sesion) {
        // Solo se aceptan estados de la sesión actualmente asociada a esta WS.
        if (sesiones.get(idWs) != sesion || sesion == null || !sesion.sesionAbierta()) {
            System.err.println("Se ignora un estado recibido de una sesión WS antigua o cerrada: " + idWs);
            return false;
        }
        String estado = normalizarEstado(estadoBruto);
        if (estado == null || !Double.isFinite(caudalLMin) || caudalLMin < 0 ||
                !Double.isFinite(litros) || litros < 0 || segundos < 0) {
            System.err.println("Estado/telemetría inválidos recibidos de WS " + idWs + "; se ignoran.");
            return false;
        }
        InstantaneaEstacion anterior = ultimosEstados.get(idWs);
        // El agua suministrada es acumulativa y nunca puede disminuir. Un monitor
        // recién reiniciado aún no conoce el volumen del motor y reporta 0: se conserva
        // el último valor válido hasta que el motor vuelva a informar.
        double volumen = Math.max(0, litros);
        if (anterior != null && anterior.litros > volumen) volumen = anterior.litros;
        InstantaneaEstacion instantanea = new InstantaneaEstacion(idWs, estado, caudalLMin, volumen,
                segundos, vacioANulo(idSolicitud), idOperario, System.currentTimeMillis());
        ultimosEstados.put(idWs, instantanea);
        db.actualizarEstadoWS(idWs, estado);
        db.actualizarAguaWS(idWs, volumen);
        ControlPendiente pendiente = controlesPendientes.get(idWs);
        if (pendiente != null && pendiente.estadoEsperado.equals(estado)) {
            controlesPendientes.remove(idWs, pendiente);
            System.out.println("WS " + idWs + ": orden " + pendiente.tipoComando + " confirmada por estado " + estado + ".");
        }
        System.out.printf(Locale.ROOT,
                "[WS %d] Estado=%s | Caudal=%.2f L/min | Volumen=%.2f L | Tiempo=%d s%n",
                idWs, estado, caudalLMin, volumen, segundos);

        RiegoActivo activo = activosPorWs.get(idWs);
        // Recuperación tras reiniciar CENTRAL: si la estación informa el requestId persistido,
        // reconstruimos el seguimiento, pero nunca reenviamos a ciegas el comando de inicio.
        if (activo == null && instantanea.idSolicitud != null) {
            activo = recuperarActivo(instantanea);
        }
        if (activo != null) {
            synchronized (activo) {
                boolean mismoSolicitud = instantanea.idSolicitud != null
                        && activo.idSolicitud.equals(instantanea.idSolicitud) && activo.idOperario == idOperario;
                // Red de seguridad: si el motor ya no informa del requestId (porque lo
                // confirmó antes de que CENTRAL lo viera) pero el servicio había
                // comenzado y la estación vuelve a estar disponible, ha terminado.
                boolean finSinSolicitud = instantanea.idSolicitud == null && activo.iniciado;
                if (!activo.terminal.get() && (mismoSolicitud || finSinSolicitud)) {
                    // La telemetría se actualiza siempre, también cuando se informa de una
                    // fuga, para no perder el agua que ya se ha suministrado.
                    activo.ultimoCaudal = caudalLMin;
                    activo.ultimoVolumen = volumen;
                    activo.ultimoTiempo = segundos;
                    if ("REGANDO".equals(estado)) {
                        if (!activo.iniciado) {
                            activo.iniciado = true;
                            activo.iniciadoEnMillis = System.currentTimeMillis() - segundos * 1000L;
                            db.actualizarEstadoSolicitud(activo.idSolicitud, "STARTED", null);
                            publicarEvento(activo, "STARTED", "La WS ha confirmado que el riego está en curso.", false);
                        }
                        publicarEvento(activo, "PROGRESS", "Riego en curso.", false);
                    } else if (activo.iniciado && "ACTIVADA".equals(estado)) {
                        finalizarServicio(activo, "COMPLETED", "Riego finalizado.");
                    } else if (!activo.iniciado && "ACTIVADA".equals(estado) && segundos > 0) {
                        // La estación puede completar un riego corto entre dos informes y no llegar
                        // a publicar un estado REGANDO. Exigimos duración observada > 0 para no
                        // confundir un ACK de inicio sin ejecutar con una finalización correcta.
                        finalizarServicio(activo, "COMPLETED", "Riego finalizado; la transición intermedia no se observó.");
                    }
                }
            }
        }

        if ("FUGA".equals(estado)) {
            RiegoActivo enCurso = activosPorWs.get(idWs);
            if (enCurso != null) {
                finalizarServicio(enCurso, "FAILED", "La estación ha notificado una avería/fuga; el riego finaliza de inmediato.");
            }
        } else if ("DESCONECTADA".equals(estado)) {
            RiegoActivo enCurso = activosPorWs.get(idWs);
            if (enCurso != null) {
                finalizarServicio(enCurso, "FAILED", "La estación se ha desconectado durante el servicio.");
            }
        } else if ("FUERA_SERVICIO".equals(estado)) {
            RiegoActivo enCurso = activosPorWs.get(idWs);
            if (enCurso != null) finalizarServicio(enCurso, "CANCELLED", "La estación ha sido bloqueada.");
        }
        return true;
    }

    private RiegoActivo recuperarActivo(InstantaneaEstacion instantanea) {
        Map<String, Object> fila = db.obtenerSolicitud(instantanea.idSolicitud);
        if (fila == null || esTerminal(texto(fila.get("status")))) return null;
        int idOperario = entero(fila.get("operatorId"), instantanea.idOperario);
        int idWs = entero(fila.get("wsId"), instantanea.idWs);
        int duracion = entero(fila.get("durationSeconds"), 0);
        if (duracion <= 0) return null;
        RiegoActivo recuperado = new RiegoActivo(instantanea.idSolicitud, idOperario, idWs, duracion);
        recuperado.iniciado = true;
        recuperado.iniciadoEnMillis = System.currentTimeMillis() - instantanea.segundos * 1000L;
        recuperado.ultimoCaudal = instantanea.caudalLMin;
        recuperado.ultimoVolumen = instantanea.litros;
        recuperado.ultimoTiempo = instantanea.segundos;
        // Tras una recuperación no se conoce el volumen previo al servicio; se informa
        // del acumulado disponible en lugar de inventar una cifra.
        recuperado.volumenBase = 0;
        RiegoActivo ganador = activosPorWs.putIfAbsent(idWs, recuperado);
        if (ganador != null) return ganador;
        activosPorSolicitud.put(instantanea.idSolicitud, recuperado);
        db.actualizarEstadoSolicitud(instantanea.idSolicitud, "STARTED", null);
        System.out.println("Seguimiento de solicitud " + instantanea.idSolicitud + " recuperado tras reconexión.");
        return recuperado;
    }

    private static String normalizarEstado(String estado) {
        if (estado == null) return null;
        String s = estado.trim().toUpperCase(Locale.ROOT).replace(' ', '_').replace('-', '_');
        switch (s) {
            case "ACTIVADA": case "ACTIVA": case "IDLE": case "AVAILABLE": case "READY": case "VERDE":
                return "ACTIVADA";
            case "REGANDO": case "WATERING": case "RIEGO_ACTIVO": case "RIEGO":
                return "REGANDO";
            case "FUGA": case "FAULT": case "KO": case "AVERIA": case "AVERIA_DETECTADA": case "ROJO":
                return "FUGA";
            case "FUERA_SERVICIO": case "FUERA_DE_SERVICIO": case "OUT_OF_SERVICE": case "BLOQUEADA": case "BLOCKED": case "PARADA": case "NARANJA":
                return "FUERA_SERVICIO";
            case "DESCONECTADA": case "DISCONNECTED": case "GRIS":
                return "DESCONECTADA";
            default: return null;
        }
    }

    private void bucleConsumoKafka() {
        while (ejecutando.get()) {
            try {
                if (gestionTopicsHabilitada()) asegurarTopics();
                Properties p = new Properties();
                p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
                p.put(ConsumerConfig.GROUP_ID_CONFIG, "wm-central-solicitudes-v1");
                p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
                p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
                p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
                try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(p)) {
                    consumidor.subscribe(Collections.singletonList(TOPICO_SOLICITUDES));
                    System.out.println("CENTRAL escuchando solicitudes Kafka en " + TOPICO_SOLICITUDES);
                    while (ejecutando.get()) {
                        ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> registro : registros) {
                            final Map<String, Object> solicitud;
                            try {
                                solicitud = JSON.readValue(registro.value(), TIPO_MAPA);
                            } catch (Exception malFormada) {
                                // El JSON mal formado no se recuperará reintentándolo; se descarta
                                // para impedir que una entrada tóxica bloquee la partición.
                                System.err.println("Mensaje Kafka mal formado (se descarta): " + malFormada.getMessage());
                                continue;
                            }
                            // Si manejarSolicitud falla por un error transitorio, dejamos que la excepción
                            // salga del bucle: no se confirma el lote y se relee desde el último offset
                            // confirmado. requestId/SQLite evita ejecutar dos veces peticiones repetidas.
                            manejarSolicitud(solicitud);
                        }
                        if (!registros.isEmpty()) consumidor.commitSync(Duration.ofSeconds(10));
                    }
                }
            } catch (Exception e) {
                if (ejecutando.get()) {
                    System.err.println("Kafka de CENTRAL no disponible: " + e.getMessage() + ". Se reintentará en 2 s.");
                    dormir(2000);
                }
            }
        }
    }

    private boolean gestionTopicsHabilitada() {
        String configurado = System.getenv("WM_CENTRAL_MANAGE_TOPICS");
        return configurado == null || Boolean.parseBoolean(configurado.trim());
    }

    private void asegurarTopics() throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        try (AdminClient admin = AdminClient.create(p)) {
            Set<String> existentes = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            List<NewTopic> faltantes = new ArrayList<>();
            for (String tema : Arrays.asList(TOPICO_SOLICITUDES, TOPICO_RESPUESTAS, TOPICO_LATIDO)) {
                if (!existentes.contains(tema)) faltantes.add(new NewTopic(tema, 3, (short) 1));
            }
            if (!faltantes.isEmpty()) {
                try { admin.createTopics(faltantes).all().get(15, TimeUnit.SECONDS); }
                catch (ExecutionException e) {
                    if (!(causaRaiz(e) instanceof TopicExistsException)) throw e;
                }
            }
        }
    }

    private void manejarSolicitud(Map<String, Object> solicitud) throws Exception {
        String tipo = texto(solicitud.get("type"));
        String idSolicitud = texto(solicitud.get("requestId"));
        if (idSolicitud == null || idSolicitud.trim().isEmpty()) {
            System.err.println("Petición Kafka ignorada: falta requestId.");
            return;
        }

        int idOperario = entero(solicitud.get("operatorId"), -1);
        if (idOperario <= 0) {
            // No hay un operador válido al que devolver la respuesta. WM_FO ya valida
            // que el ID sea positivo; este caso indica un productor mal configurado.
            System.err.println("Petición " + idSolicitud + " ignorada: operatorId inválido.");
            return;
        }

        if ("LIST_AVAILABLE_WS".equals(tipo)) {
            if (!db.existeOperario(idOperario)) {
                publicarEventoGenerico(idSolicitud, idOperario, -1, "REJECTED",
                        "El operario no está registrado en CENTRAL.");
                return;
            }
            Map<String, Object> respuesta = new LinkedHashMap<>();
            respuesta.put("type", "AVAILABLE_WS_LIST");
            respuesta.put("requestId", idSolicitud);
            respuesta.put("operatorId", idOperario);
            respuesta.put("stations", listarWsDisponiblesEnLinea());
            respuesta.put("createdAt", Instant.now().toString());
            publicarAsincrono(TOPICO_RESPUESTAS, Integer.toString(idOperario), respuesta);
            return;
        }

        // Consulta del estado de una solicitud previa (recuperación de WM_FO tras
        // cerrarse durante un servicio). Devuelve el evento terminal si ya existe o el
        // estado actual en caso contrario, siempre que pertenezca al operario.
        if ("CONSULTAR_ESTADO".equals(tipo)) {
            if (!db.existeOperario(idOperario)) {
                publicarEventoGenerico(idSolicitud, idOperario, -1, "REJECTED",
                        "El operario no está registrado en CENTRAL.");
                return;
            }
            Map<String, Object> fila = db.obtenerSolicitud(idSolicitud);
            if (fila == null) {
                publicarEventoGenerico(idSolicitud, idOperario, -1, "REJECTED",
                        "No existe ninguna solicitud con ese requestId.");
                return;
            }
            int propietario = entero(fila.get("operatorId"), -1);
            int idWs = entero(fila.get("wsId"), -1);
            if (propietario != idOperario) {
                publicarEventoGenerico(idSolicitud, idOperario, idWs, "REJECTED",
                        "La solicitud no pertenece a este operario.");
                return;
            }
            String eventoTerminal = texto(fila.get("terminalEvent"));
            if (eventoTerminal != null && !eventoTerminal.isEmpty()) {
                publicarDirecto(TOPICO_RESPUESTAS, Integer.toString(idOperario), eventoTerminal);
                return;
            }
            RiegoActivo activo = activosPorSolicitud.get(idSolicitud);
            if (activo != null) {
                publicarEvento(activo, "PROGRESS", "El servicio sigue en curso.", false);
                return;
            }
            String estadoActual = texto(fila.get("status"));
            Map<String, Object> evento = crearEvento(idSolicitud, idOperario, idWs,
                    estadoActual == null ? "RECEIVED" : estadoActual,
                    "Estado actual de la solicitud.", 0, 0, 0);
            publicarAsincrono(TOPICO_RESPUESTAS, Integer.toString(idOperario), evento);
            return;
        }

        if (!"RIEGO_REQUEST".equals(tipo)) {
            publicarEventoGenerico(idSolicitud, idOperario, entero(solicitud.get("wsId"), -1),
                    "REJECTED", "Tipo de solicitud desconocido.");
            return;
        }

        int idWs = entero(solicitud.get("wsId"), -1);
        int duracion = entero(solicitud.get("durationSeconds"), -1);
        if (idWs <= 0 || duracion < 1 || duracion > 86400) {
            publicarEventoGenerico(idSolicitud, idOperario, idWs, "REJECTED",
                    "ID de WS o duración inválidos. La duración debe estar entre 1 y 86400 segundos.");
            return;
        }

        // Persistimos la petición antes de validarla por completo: si el operario no
        // existe o la WS no está disponible, el rechazo también queda registrado y
        // puede notificarse después de una caída temporal de Kafka.
        boolean nueva = db.guardarSolicitudSiNueva(idSolicitud, idOperario, idWs, duracion);
        if (!nueva) {
            Map<String, Object> anterior = db.obtenerSolicitud(idSolicitud);
            if (anterior == null) {
                publicarEventoGenerico(idSolicitud, idOperario, idWs, "FAILED",
                        "CENTRAL no pudo persistir la solicitud; no se ha enviado una orden de riego.");
                return;
            }

            int operarioAntiguo = entero(anterior.get("operatorId"), -1);
            int wsAntigua = entero(anterior.get("wsId"), -1);
            int duracionAntigua = entero(anterior.get("durationSeconds"), -1);
            if (operarioAntiguo != idOperario || wsAntigua != idWs || duracionAntigua != duracion) {
                Map<String, Object> conflicto = crearEvento(idSolicitud, idOperario, idWs, "REJECTED",
                        "El requestId ya se utilizó con parámetros diferentes; no se repetirá el riego.", 0, 0, 0);
                publicarAsincrono(TOPICO_RESPUESTAS, Integer.toString(idOperario), conflicto);
                return;
            }

            String terminal = texto(anterior.get("terminalEvent"));
            if (terminal != null && !terminal.isEmpty()) {
                if (publicarDirecto(TOPICO_RESPUESTAS, Integer.toString(idOperario), terminal)) {
                    db.marcarEventoTerminalEnviado(idSolicitud);
                }
                return;
            }

            RiegoActivo activoExistente = activosPorSolicitud.get(idSolicitud);
            if (activoExistente != null) {
                publicarEvento(activoExistente, activoExistente.iniciado ? "PROGRESS" : "RECEIVED",
                        "Solicitud repetida detectada; no se vuelve a activar la estación.", false);
                return;
            }

            InstantaneaEstacion actual = ultimosEstados.get(idWs);
            if (actual != null && idSolicitud.equals(actual.idSolicitud)) {
                RiegoActivo recuperado = recuperarActivo(actual);
                if (recuperado != null) {
                    publicarEvento(recuperado, recuperado.iniciado ? "PROGRESS" : "RECEIVED",
                            "Seguimiento recuperado; no se reenviará la orden de inicio.", false);
                    return;
                }
            }

            // No se reenvía una orden si el estado no puede recuperarse; el reconciliador
            // cerrará como FAILED una solicitud abandonada después del periodo de gracia.
            publicarEventoGenerico(idSolicitud, idOperario, idWs, "RECEIVED",
                    "Solicitud duplicada; CENTRAL está recuperando el estado y no repetirá el riego.");
            return;
        }

        if (!marcaTiempoFresca(solicitud.get("createdAt"))) {
            rechazar(idSolicitud, idOperario, idWs,
                    "La solicitud es demasiado antigua o no contiene una fecha createdAt válida; no se iniciará un riego atrasado.");
            return;
        }

        if (!db.existeOperario(idOperario)) {
            rechazar(idSolicitud, idOperario, idWs,
                    "El operario no está registrado en CENTRAL.");
            return;
        }

        publicarEventoGenerico(idSolicitud, idOperario, idWs, "RECEIVED", "Solicitud recibida por CENTRAL.");
        db.actualizarEstadoSolicitud(idSolicitud, "CHECKING", null);
        publicarEventoGenerico(idSolicitud, idOperario, idWs, "CHECKING",
                "Comprobando operario, estación y disponibilidad.");

        if (!db.existeWS(idWs)) {
            rechazar(idSolicitud, idOperario, idWs, "La WS no está registrada.");
            return;
        }

        HiloCentral sesion = sesiones.get(idWs);
        Map<String, Object> ws = db.obtenerWS(idWs);
        String estado = ws == null ? null : texto(ws.get("state"));
        InstantaneaEstacion estadoActual = ultimosEstados.get(idWs);
        ControlPendiente controlPendiente = controlesPendientes.get(idWs);
        if (controlPendiente != null) {
            rechazar(idSolicitud, idOperario, idWs,
                    "La WS tiene una orden administrativa pendiente de confirmación: " + controlPendiente.tipoComando + ".");
            return;
        }
        if (sesion == null || !sesion.sesionAbierta() || !estadoReciente(idWs) || estadoActual == null) {
            rechazar(idSolicitud, idOperario, idWs,
                    "La WS está desconectada o no responde a las comprobaciones periódicas.");
            return;
        }
        if (!"ACTIVADA".equalsIgnoreCase(estado) || !"ACTIVADA".equals(estadoActual.estado)) {
            rechazar(idSolicitud, idOperario, idWs,
                    "La WS no está disponible. Estado actual: " + estado);
            return;
        }

        RiegoActivo activo = new RiegoActivo(idSolicitud, idOperario, idWs, duracion);
        activo.volumenBase = estadoActual.litros;
        RiegoActivo previo = activosPorWs.putIfAbsent(idWs, activo);
        if (previo != null) {
            finalizarServicio(activo, "REJECTED", "La WS ya tiene un servicio en curso.");
            return;
        }
        activosPorSolicitud.put(idSolicitud, activo);
        db.actualizarEstadoSolicitud(idSolicitud, "AUTHORIZED", null);
        publicarEvento(activo, "AUTHORIZED", "La WS está disponible; se solicita el inicio del riego.", false);

        encolarComando(idWs, "START_IRRIGATION", idSolicitud, idOperario, duracion);
        activo.comandoEnviado = true;
        activo.comandoEnviadoEnMillis = System.currentTimeMillis();
        System.out.println("Orden de riego encolada para la WS " + idWs + ": requestId=" + idSolicitud);
    }

    private void rechazar(String idSolicitud, int idOperario, int idWs, String mensaje) {
        Map<String, Object> evento = crearEvento(idSolicitud, idOperario, idWs, "REJECTED", mensaje, 0, 0, 0);
        enviarTerminal(evento);
    }

    private void publicarEventoGenerico(String idSolicitud, int idOperario, int idWs, String estado, String mensaje) {
        // Para rechazos previos a crear el registro (p. ej. argumentos inválidos o consultas
        // de listado) no se sobrescribe una posible solicitud ya persistida con el mismo ID.
        Map<String, Object> evento = crearEvento(idSolicitud, idOperario, idWs, estado, mensaje, 0, 0, 0);
        publicarAsincrono(TOPICO_RESPUESTAS, Integer.toString(idOperario), evento);
    }

    private void publicarEvento(RiegoActivo activo, String estado, String mensaje, boolean terminal) {
        synchronized (activo) {
            // Una telemetría tardía no debe sobrescribir el estado persistido después del terminal.
            if (!terminal && activo.terminal.get()) return;
            Map<String, Object> evento = crearEvento(activo.idSolicitud, activo.idOperario, activo.idWs,
                    estado, mensaje, activo.ultimoCaudal, activo.ultimoVolumen, activo.ultimoTiempo);
            // La telemetría acumula litros desde el inicio de ESTE riego, no desde el arranque de la WS.
            evento.put("serviceVolumeLitres", Math.max(0.0, activo.ultimoVolumen - activo.volumenBase));
            if (terminal) enviarTerminal(evento);
            else {
                db.actualizarEstadoSolicitud(activo.idSolicitud, estado, null);
                publicarAsincrono(TOPICO_RESPUESTAS, Integer.toString(activo.idOperario), evento);
            }
        }
    }

    private Map<String, Object> crearEvento(String idSolicitud, int idOperario, int idWs, String estado,
                                            String mensaje, double caudal, double volumen, long transcurrido) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("type", "RIEGO_EVENT"); evento.put("requestId", idSolicitud);
        evento.put("operatorId", idOperario); evento.put("wsId", idWs); evento.put("status", estado);
        evento.put("message", mensaje); evento.put("flowLMin", caudal); evento.put("volumeLitres", volumen);
        evento.put("elapsedSeconds", transcurrido); evento.put("createdAt", Instant.now().toString()); return evento;
    }

    private void enviarTerminal(Map<String, Object> evento) {
        String idSolicitud = texto(evento.get("requestId"));
        String estado = texto(evento.get("status"));
        String textoJson;
        try { textoJson = JSON.writeValueAsString(evento); }
        catch (Exception e) { System.err.println("No se pudo serializar el evento terminal: " + e.getMessage()); return; }
        db.actualizarEstadoSolicitud(idSolicitud, estado, textoJson);
        encolarEventoTerminal(idSolicitud, String.valueOf(evento.get("operatorId")), textoJson);
    }

    private boolean publicarAsincrono(String tema, String clave, Map<String, Object> mensaje) {
        try {
            String textoJson = JSON.writeValueAsString(mensaje);
            productor.send(new ProducerRecord<>(tema, clave, textoJson), (metadatos, excepcion) -> {
                if (excepcion != null) System.err.println("Error publicando mensaje asíncrono en " + tema + ": " + excepcion.getMessage());
            });
            return true;
        } catch (Exception e) {
            System.err.println("No se pudo encolar mensaje Kafka en " + tema + ": " + e.getMessage());
            return false;
        }
    }

    private void encolarEventoTerminal(String idSolicitud, String clave, String textoJson) {
        if (idSolicitud == null || textoJson == null || eventosTerminalesEnVuelo.putIfAbsent(idSolicitud, Boolean.TRUE) != null) return;
        try {
            buzonSalida.execute(() -> {
                try {
                    if (publicarDirecto(TOPICO_RESPUESTAS, clave, textoJson)) db.marcarEventoTerminalEnviado(idSolicitud);
                } finally {
                    eventosTerminalesEnVuelo.remove(idSolicitud);
                }
            });
        } catch (RejectedExecutionException e) {
            eventosTerminalesEnVuelo.remove(idSolicitud);
            System.err.println("Buzón de CENTRAL cerrado; el evento queda pendiente en SQLite para el siguiente arranque.");
        }
    }

    private boolean publicar(String tema, String clave, Map<String, Object> mensaje) {
        try { return publicarDirecto(tema, clave, JSON.writeValueAsString(mensaje)); }
        catch (Exception e) { System.err.println("No se pudo serializar mensaje Kafka: " + e.getMessage()); return false; }
    }

    private boolean publicarDirecto(String tema, String clave, String textoJson) {
        try {
            productor.send(new ProducerRecord<>(tema, clave, textoJson)).get(35, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            System.err.println("No se pudo publicar en " + tema + ": " + e.getMessage()); return false;
        }
    }

    private void finalizarServicio(RiegoActivo activo, String estado, String mensaje) {
        if (activo == null) return;
        synchronized (activo) {
            if (!activo.terminal.compareAndSet(false, true)) return;
            activosPorWs.remove(activo.idWs, activo);
            activosPorSolicitud.remove(activo.idSolicitud, activo);
            // publicarEvento usa el mismo monitor: el bloque es reentrante en Java.
            publicarEvento(activo, estado, mensaje, true);
            System.out.println("Servicio " + activo.idSolicitud + " finalizado con estado " + estado + ".");
        }
    }

    private void comprobarTimeouts() {
        try {
            long ahora = System.currentTimeMillis();
            for (Map.Entry<Integer, ControlPendiente> entrada : controlesPendientes.entrySet()) {
                ControlPendiente pendiente = entrada.getValue();
                if (pendiente != null && ahora - pendiente.creadoEnMillis > 15_000L) {
                    if (controlesPendientes.remove(entrada.getKey(), pendiente)) {
                        System.out.println("WS " + entrada.getKey() + ": la orden " + pendiente.tipoComando
                                + " no se confirmó en 15 s; se libera el bloqueo de nuevas solicitudes.");
                    }
                }
            }
            for (RiegoActivo activo : activosPorSolicitud.values()) {
                if (activo.comandoEnviado && !activo.iniciado && ahora - activo.comandoEnviadoEnMillis > TIMEOUT_ACK_INICIO_MS) {
                    finalizarServicio(activo, "FAILED", "La WS no confirmó el inicio dentro del tiempo esperado.");
                    publicarParadaSeguridad(activo.idWs, "STOP_IRRIGATION");
                } else if (activo.iniciado && ahora - activo.iniciadoEnMillis > (activo.duracionSegundos + SEGUNDOS_EXTRA_TIMEOUT_SOLICITUD) * 1000L) {
                    finalizarServicio(activo, "FAILED", "Se agotó el tiempo máximo de supervisión del riego.");
                    publicarParadaSeguridad(activo.idWs, "STOP_IRRIGATION");
                }
            }
        } catch (Exception e) { System.err.println("Error supervisando timeouts: " + e.getMessage()); }
    }

    private void reintentarEventosTerminales() {
        try {
            for (Map<String, Object> fila : db.listarEventosTerminalesPendientes()) {
                String idSolicitud = texto(fila.get("requestId"));
                String textoJson = texto(fila.get("terminalEvent"));
                if (textoJson != null) encolarEventoTerminal(idSolicitud, String.valueOf(fila.get("operatorId")), textoJson);
            }
        } catch (Exception e) { System.err.println("Error reintentando notificaciones terminales: " + e.getMessage()); }
    }

    private void reconciliarSolicitudesHuerfanas() {
        try {
            long ahora = System.currentTimeMillis();
            for (Map<String, Object> fila : db.listarSolicitudesNoTerminales()) {
                String idSolicitud = texto(fila.get("requestId"));
                if (idSolicitud == null || activosPorSolicitud.containsKey(idSolicitud)) continue;
                int idWs = entero(fila.get("wsId"), -1);
                InstantaneaEstacion instantanea = ultimosEstados.get(idWs);
                if (instantanea != null && idSolicitud.equals(instantanea.idSolicitud)) {
                    recuperarActivo(instantanea);
                    if (activosPorSolicitud.containsKey(idSolicitud)) continue;
                }
                long actualizadoEn = 0;
                try {
                    String marca = texto(fila.get("updatedAt"));
                    if (marca == null || marca.isEmpty()) marca = texto(fila.get("createdAt"));
                    actualizadoEn = Instant.parse(marca).toEpochMilli();
                } catch (Exception ignorada) { }
                // Margen amplio para que una WS que sigue regando tras reiniciar CENTRAL
                // pueda reconectarse y recuperar el seguimiento sin marcarse como fallida.
                if (actualizadoEn > 0 && ahora - actualizadoEn < 60_000) continue;
                int idOperario = entero(fila.get("operatorId"), -1);
                Map<String, Object> evento = crearEvento(idSolicitud, idOperario, idWs, "FAILED",
                        "CENTRAL no pudo recuperar el estado de la solicitud después de una interrupción. No se repetirá automáticamente.", 0, 0, 0);
                enviarTerminal(evento);
                HiloCentral sesion = sesiones.get(idWs);
                if (sesion != null && sesion.sesionAbierta()) publicarParadaSeguridad(idWs, "STOP_IRRIGATION");
            }
        } catch (Exception e) { System.err.println("Error reconciliando solicitudes pendientes: " + e.getMessage()); }
    }

    private void publicarParadaSeguridad(int idWs, String tipo) {
        HiloCentral sesion = sesiones.get(idWs);
        if (sesion == null || !sesion.sesionAbierta()) return;
        encolarComando(idWs, tipo, "seguridad-" + UUID.randomUUID(), 0, 0);
    }

    /** Inicia un riego administrativo en una WS concreta o en todas las WS conectadas (wsId=0). */
    public boolean adminRiego(int idWs, int duracionSegundos) {
        if (idWs < 0) {
            System.out.println("El ID de WS debe ser positivo o 0 para todas las WS conectadas.");
            return false;
        }
        if (duracionSegundos < 1 || duracionSegundos > 86400) {
            System.out.println("Duración inválida (1-86400 s).");
            return false;
        }

        List<Integer> destinos = new ArrayList<>();
        if (idWs == 0) {
            for (Integer id : sesiones.keySet()) {
                HiloCentral sesion = sesiones.get(id);
                if (sesion != null && sesion.sesionAbierta() && estadoReciente(id)) destinos.add(id);
            }
        } else {
            destinos.add(idWs);
        }
        Collections.sort(destinos);
        if (destinos.isEmpty()) {
            System.out.println("No hay WS conectadas y con estado reciente para iniciar el riego.");
            return false;
        }
        boolean enviado = false;
        for (Integer id : destinos) enviado |= adminRiegoUnaWs(id, duracionSegundos);
        return enviado;
    }

    private boolean adminRiegoUnaWs(int idWs, int duracionSegundos) {
        Map<String, Object> ws = db.obtenerWS(idWs);
        HiloCentral sesion = sesiones.get(idWs);
        InstantaneaEstacion instantanea = ultimosEstados.get(idWs);
        if (controlesPendientes.containsKey(idWs) || ws == null || !"ACTIVADA".equalsIgnoreCase(texto(ws.get("state"))) ||
                sesion == null || !sesion.sesionAbierta() || !estadoReciente(idWs) ||
                instantanea == null || !"ACTIVADA".equals(instantanea.estado)) {
            System.out.println("WS " + idWs + ": omitida; no existe, no está conectada o no está ACTIVADA.");
            return false;
        }

        RiegoActivo activo = new RiegoActivo("CENTRAL-" + UUID.randomUUID(), 0, idWs, duracionSegundos);
        activo.volumenBase = instantanea.litros;
        if (!db.guardarSolicitudSiNueva(activo.idSolicitud, 0, idWs, duracionSegundos)) {
            System.out.println("WS " + idWs + ": no se pudo guardar la solicitud administrativa; no se iniciará.");
            return false;
        }
        RiegoActivo previo = activosPorWs.putIfAbsent(idWs, activo);
        if (previo != null) {
            finalizarServicio(activo, "REJECTED", "La WS ya tiene un servicio en curso.");
            return false;
        }
        activosPorSolicitud.put(activo.idSolicitud, activo);
        db.actualizarEstadoSolicitud(activo.idSolicitud, "AUTHORIZED", null);
        publicarEvento(activo, "AUTHORIZED", "Riego iniciado desde el menú administrativo de CENTRAL.", false);

        encolarComando(idWs, "START_IRRIGATION", activo.idSolicitud, 0, duracionSegundos);
        activo.comandoEnviado = true;
        activo.comandoEnviadoEnMillis = System.currentTimeMillis();
        System.out.println("Riego administrativo encolado para WS " + idWs + ".");
        return true;
    }

    /** Envía una orden a una WS concreta o a todas las WS conectadas (wsId=0). */
    public boolean adminOrden(String tipo, int idWs) {
        if (!Arrays.asList("BLOCK_WS", "ACTIVATE_WS", "STOP_IRRIGATION").contains(tipo)) {
            System.out.println("Orden administrativa desconocida: " + tipo);
            return false;
        }
        if (idWs < 0) {
            System.out.println("El ID de WS debe ser positivo o 0 para todas las WS conectadas.");
            return false;
        }

        List<Integer> ids = new ArrayList<>();
        if (idWs == 0) {
            for (Map.Entry<Integer, HiloCentral> entrada : sesiones.entrySet()) {
                if (entrada.getValue().sesionAbierta() && estadoReciente(entrada.getKey())) ids.add(entrada.getKey());
            }
        } else {
            HiloCentral sesion = sesiones.get(idWs);
            if (sesion != null && sesion.sesionAbierta() && estadoReciente(idWs)) ids.add(idWs);
        }
        Collections.sort(ids);
        if (ids.isEmpty()) {
            System.out.println("No hay WS conectadas y con estado reciente para esa orden.");
            return false;
        }

        for (Integer id : ids) {
            ControlPendiente pendiente = null;
            if ("BLOCK_WS".equals(tipo)) pendiente = new ControlPendiente(tipo, "FUERA_SERVICIO");
            else if ("ACTIVATE_WS".equals(tipo)) pendiente = new ControlPendiente(tipo, "ACTIVADA");
            if (pendiente != null) controlesPendientes.put(id, pendiente);

            encolarComando(id, tipo, "CENTRAL-" + UUID.randomUUID(), 0, 0);
            System.out.println(tipo + " encolada para WS " + id
                    + "; la WS no se considera en el nuevo estado hasta recibir confirmación.");
        }
        return true;
    }

    private void publicarLatido() {
        if (!ejecutando.get()) return;
        try {
            Map<String, Object> latido = new LinkedHashMap<>();
            latido.put("type", "CENTRAL_HEARTBEAT");
            latido.put("stations", sesiones.size());
            latido.put("createdAt", Instant.now().toString());
            productor.send(new ProducerRecord<>(TOPICO_LATIDO, "central", JSON.writeValueAsString(latido)));
        } catch (Exception e) {
            // Kafka puede estar temporalmente caído; el latido se reintenta en el
            // siguiente ciclo y no debe afectar al servicio de sockets de CENTRAL.
        }
    }

    // ------------------------------------------------------------ Panel / consola

    /** Refresca el panel sólo cuando cambia algo relevante (evita ruido en la terminal). */
    public void panelSiCambia() { mostrarPanel(false); }

    public void mostrarPanel(boolean forzar) {
        List<Map<String, Object>> estaciones = db.listarWS();
        StringBuilder firma = new StringBuilder();
        StringBuilder texto = new StringBuilder();
        texto.append("\n================= PANEL WM_CENTRAL (").append(hora()).append(") =================\n");
        texto.append(String.format("%-4s %-24s %-32s %-14s %s%n",
                "ID", "UBICACIÓN", "ESTADO", "AGUA TOTAL", "DETALLE"));
        texto.append("--------------------------------------------------------------------------------\n");
        if (estaciones.isEmpty()) texto.append("No hay estaciones WS registradas.\n");

        for (Map<String, Object> fila : estaciones) {
            int id = entero(fila.get("wsId"), -1);
            if (id <= 0) continue;
            HiloCentral sesion = sesiones.get(id);
            InstantaneaEstacion instantanea = ultimosEstados.get(id);
            boolean enLinea = sesion != null && sesion.sesionAbierta() && estadoReciente(id) && instantanea != null;
            String estado = enLinea ? instantanea.estado : "DESCONECTADA";
            double aguaDb = numero(fila.get("volumeLitres"), 0);
            double agua = enLinea ? Math.max(instantanea.litros, aguaDb) : aguaDb;

            String detalle = "";
            if ("REGANDO".equals(estado)) {
                RiegoActivo activo = activosPorWs.get(id);
                double servicio = activo != null ? Math.max(0, instantanea.litros - activo.volumenBase)
                        : instantanea.litros;
                detalle = "Caudal: " + fmt(instantanea.caudalLMin) + " L/min | Servicio: " + fmt(servicio)
                        + " L | Operario: " + (instantanea.idOperario > 0 ? instantanea.idOperario : "-");
            } else if ("FUGA".equals(estado)) {
                detalle = "Fuga/avería detectada; riego detenido";
            } else if ("FUERA_SERVICIO".equals(estado)) {
                detalle = "Out of Service";
            } else if ("DESCONECTADA".equals(estado)) {
                detalle = "Sin conexión con el monitor";
            }

            texto.append(String.format("%-4d %-24s %s %-14s %s%n",
                    id, acotar(String.valueOf(fila.get("location")), 24),
                    estadoColoreado(estado), fmt(agua) + " L", detalle));
            firma.append(id).append('|').append(estado).append('|')
                    .append(Math.round(agua * 10)).append('|')
                    .append(enLinea ? Math.round(instantanea.caudalLMin * 10) : 0).append('|')
                    .append(enLinea ? instantanea.idOperario : -1).append(';');
        }
        texto.append("================================================================================\n");

        if (forzar || !firma.toString().equals(firmaPanel)) {
            firmaPanel = firma.toString();
            System.out.print(texto);
        }
    }

    private static String estadoColoreado(String estado) {
        String etiqueta;
        String color;
        switch (estado.toUpperCase(Locale.ROOT)) {
            case "ACTIVADA": etiqueta = "Available"; color = "\u001B[32m"; break;
            case "REGANDO": etiqueta = "Watering"; color = "\u001B[1;5;32m"; break;
            case "FUGA": etiqueta = "Leak"; color = "\u001B[31m"; break;
            case "FUERA_SERVICIO": etiqueta = "Out of Service"; color = "\u001B[38;5;208m"; break;
            case "DESCONECTADA": etiqueta = "Disconnected"; color = "\u001B[90m"; break;
            default: etiqueta = estado; color = "";
        }
        return color + pad(etiqueta, 32) + "\u001B[0m";
    }

    private static String pad(String texto, int ancho) {
        if (texto.length() >= ancho) return texto;
        StringBuilder sb = new StringBuilder(texto);
        while (sb.length() < ancho) sb.append(' ');
        return sb.toString();
    }

    private static String acotar(String texto, int maximo) {
        if (texto == null) return "";
        if (texto.length() <= maximo) return texto;
        return texto.substring(0, Math.max(0, maximo - 3)) + "...";
    }

    private static String hora() {
        return java.time.LocalTime.now().withNano(0).toString();
    }

    private static String fmt(double v) { return String.format(Locale.ROOT, "%.2f", v); }

    private static double numero(Object valor, double porDefecto) {
        if (valor instanceof Number) return ((Number) valor).doubleValue();
        try { return Double.parseDouble(String.valueOf(valor)); }
        catch (Exception e) { return porDefecto; }
    }

    public List<Map<String, Object>> listarWS() { return db.listarWS(); }

    /**
     * Vista de solo lectura para la interfaz gráfica: reúne la información de SQLite y
     * del último estado recibido de cada monitor. El estado efectivo de una estación
     * sin sesión válida es siempre DESCONECTADA.
     */
    public List<VistaEstacion> vistaEstaciones() {
        List<Map<String, Object>> estaciones = db.listarWS();
        List<VistaEstacion> vista = new ArrayList<>();
        for (Map<String, Object> fila : estaciones) {
            int id = entero(fila.get("wsId"), -1);
            if (id <= 0) continue;

            HiloCentral sesion = sesiones.get(id);
            InstantaneaEstacion instantanea = ultimosEstados.get(id);
            boolean enLinea = sesion != null && sesion.sesionAbierta() && estadoReciente(id) && instantanea != null;

            String estado = enLinea ? instantanea.estado : "DESCONECTADA";
            double aguaDb = numero(fila.get("volumeLitres"), 0);
            double agua = enLinea ? Math.max(instantanea.litros, aguaDb) : aguaDb;
            double caudal = enLinea ? instantanea.caudalLMin : 0;
            long segundos = enLinea ? instantanea.segundos : 0;
            int idOperario = enLinea ? instantanea.idOperario : 0;
            String idSolicitud = enLinea ? instantanea.idSolicitud : null;

            RiegoActivo activo = activosPorWs.get(id);
            double servicio = enLinea && activo != null
                    ? Math.max(0, instantanea.litros - activo.volumenBase) : 0;

            ControlPendiente pendiente = controlesPendientes.get(id);
            vista.add(new VistaEstacion(id, String.valueOf(fila.get("location")), estado, enLinea,
                    agua, caudal, servicio, segundos, idOperario, idSolicitud,
                    pendiente == null ? null : pendiente.tipoComando));
        }
        return vista;
    }

    private boolean estadoReciente(int idWs) {
        InstantaneaEstacion instantanea = ultimosEstados.get(idWs);
        return instantanea != null && System.currentTimeMillis() - instantanea.vistoEnMillis <= ANTIGUEDAD_MAXIMA_ESTADO_MS;
    }

    private List<Map<String, Object>> listarWsDisponiblesEnLinea() {
        List<Map<String, Object>> disponibles = db.listarWSDisponibles();
        disponibles.removeIf(ws -> {
            int id = entero(ws.get("wsId"), -1);
            HiloCentral sesion = sesiones.get(id);
            InstantaneaEstacion instantanea = ultimosEstados.get(id);
            return sesion == null || !sesion.sesionAbierta() || !estadoReciente(id) ||
                    instantanea == null || !"ACTIVADA".equals(instantanea.estado) || activosPorWs.containsKey(id) || controlesPendientes.containsKey(id);
        });
        return disponibles;
    }

    public List<Map<String, Object>> listarOperarios() { return db.listarOperarios(); }
    public boolean registrarOperario(int id, String nombre) { return db.registrarOperario(id, nombre); }
    public boolean buscarOperario(int id) { return db.buscarOperario(id); }
    public boolean estaEjecutando() { return ejecutando.get(); }

    @Override public void close() {
        if (!ejecutando.compareAndSet(true, false)) return;
        planificador.shutdownNow();
        if (hiloKafka != null) hiloKafka.interrupt();
        List<HiloCentral> sesionesAbiertas = new ArrayList<>(sesiones.values());
        for (HiloCentral s : sesionesAbiertas) s.cerrarDesdeReemplazo();
        for (HiloCentral s : sesionesAbiertas) {
            if (s != Thread.currentThread()) {
                try { s.join(1500L); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }
        sesiones.clear();
        buzonSalida.shutdown();
        try { buzonSalida.awaitTermination(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { productor.close(Duration.ofSeconds(5)); } catch (Exception ignorada) { }
    }

    private static boolean esTerminal(String estado) {
        if (estado == null) return false;
        return Arrays.asList("COMPLETED", "REJECTED", "FAILED", "CANCELLED").contains(estado.toUpperCase(Locale.ROOT));
    }
    private static String texto(Object x) { return x == null ? null : String.valueOf(x); }
    private static String vacioANulo(String x) { return x == null || x.trim().isEmpty() ? null : x; }
    private static int entero(Object x, int porDefecto) {
        if (x instanceof Number) return ((Number) x).intValue();
        try { return Integer.parseInt(String.valueOf(x)); } catch (Exception e) { return porDefecto; }
    }
    private static double decimal(String x, double porDefecto) {
        try { double d = Double.parseDouble(x); return Double.isFinite(d) ? d : porDefecto; } catch (Exception e) { return porDefecto; }
    }
    private static long enteroLargo(String x, long porDefecto) { try { return Long.parseLong(x); } catch (Exception e) { return porDefecto; } }
    private static Throwable causaRaiz(Throwable e) { Throwable c = e; while (c.getCause() != null && c.getCause() != c) c = c.getCause(); return c; }
    private static void dormir(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }

    private static boolean marcaTiempoFresca(Object valor) {
        if (valor == null) return false;
        try {
            long marca = Instant.parse(String.valueOf(valor)).toEpochMilli();
            long ahora = System.currentTimeMillis();
            // Se admiten 5 minutos de antigüedad y hasta 15 minutos de desviación futura.
            return marca >= ahora - TimeUnit.MINUTES.toMillis(5)
                    && marca <= ahora + TimeUnit.MINUTES.toMillis(15);
        } catch (Exception e) {
            return false;
        }
    }

    private static final class ControlPendiente {
        final String tipoComando;
        final String estadoEsperado;
        final long creadoEnMillis = System.currentTimeMillis();
        ControlPendiente(String tipoComando, String estadoEsperado) {
            this.tipoComando = tipoComando;
            this.estadoEsperado = estadoEsperado;
        }
    }

    private static final class RiegoActivo {
        final String idSolicitud; final int idOperario; final int idWs; final int duracionSegundos;
        final long creadoEnMillis = System.currentTimeMillis();
        final AtomicBoolean terminal = new AtomicBoolean(false);
        volatile boolean iniciado; volatile long iniciadoEnMillis;
        volatile boolean comandoEnviado; volatile long comandoEnviadoEnMillis;
        volatile double ultimoCaudal; volatile double ultimoVolumen; volatile long ultimoTiempo;
        // Volumen total de la estación en el momento de autorizar el riego: permite
        // informar al operario del consumo exacto de SU servicio.
        volatile double volumenBase;
        RiegoActivo(String idSolicitud, int idOperario, int idWs, int duracionSegundos) {
            this.idSolicitud = idSolicitud; this.idOperario = idOperario; this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
            this.ultimoVolumen = 0.0;
        }
    }

    /** Datos de una estación para la vista gráfica de WM_Central. */
    public static final class VistaEstacion {
        public final int idWs;
        public final String ubicacion;
        public final String estado;
        public final boolean enLinea;
        public final double litros;
        public final double caudalLMin;
        public final double litrosServicio;
        public final long segundos;
        public final int idOperario;
        public final String idSolicitud;
        public final String controlPendiente;

        VistaEstacion(int idWs, String ubicacion, String estado, boolean enLinea,
                      double litros, double caudalLMin, double litrosServicio,
                      long segundos, int idOperario, String idSolicitud, String controlPendiente) {
            this.idWs = idWs;
            this.ubicacion = ubicacion;
            this.estado = estado;
            this.enLinea = enLinea;
            this.litros = litros;
            this.caudalLMin = caudalLMin;
            this.litrosServicio = litrosServicio;
            this.segundos = segundos;
            this.idOperario = idOperario;
            this.idSolicitud = idSolicitud;
            this.controlPendiente = controlPendiente;
        }
    }

    private static final class InstantaneaEstacion {
        final int idWs; final String estado; final double caudalLMin; final double litros;
        final long segundos; final String idSolicitud; final int idOperario; final long vistoEnMillis;
        InstantaneaEstacion(int id, String e, double c, double l, long s, String r, int o, long t) {
            idWs = id; estado = e; caudalLMin = c; litros = l; segundos = s; idSolicitud = r; idOperario = o; vistoEnMillis = t;
        }
        InstantaneaEstacion conEstado(String e) { return new InstantaneaEstacion(idWs, e, caudalLMin, litros, segundos, idSolicitud, idOperario, System.currentTimeMillis()); }
    }
}

```


## WM_Central/src/HiloCentral.java

```java
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Atiende una conexión persistente de monitor WS. Un hilo por estación conectada. */
public final class HiloCentral extends Thread {
    private final Socket socket;
    private final CentralService central;
    private volatile int idWs = -1;
    private volatile boolean registrada;
    private volatile boolean cerrada;
    private InputStream entrada;
    private OutputStream salida;

    public HiloCentral(Socket socket, CentralService central) {
        super("wm-central-ws-socket-" + socket.getRemoteSocketAddress());
        this.socket = socket;
        this.central = central;
        setDaemon(true);
    }

    @Override
    public void run() {
        try {
            socket.setKeepAlive(true);
            socket.setSoTimeout(6_000); // el monitor envía estado cada segundo
            entrada = new BufferedInputStream(socket.getInputStream());
            salida = new BufferedOutputStream(socket.getOutputStream());

            int enq = entrada.read();
            if (enq != WireProtocol.ENQ) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                throw new IOException("Se esperaba ENQ al establecer la sesión");
            }
            WireProtocol.enviarControl(salida, WireProtocol.ACK);

            String saludo = recibirTramaCliente();
            if (saludo == null) throw new EOFException("La WS cerró el socket durante el saludo");
            String respuestaSaludo = procesarSaludo(saludo);
            enviarRespuestaYEsperarAck(respuestaSaludo);
            if (!registrada) throw new IOException("La WS no pudo registrarse/autenticarse: " + respuestaSaludo);

            System.out.println("WS " + idWs + " autenticada; sesión persistente establecida.");
            while (!cerrada && !socket.isClosed()) {
                String mensaje = recibirTramaCliente();
                if (mensaje == null) break; // EOT del monitor
                String respuesta = procesarMensaje(mensaje);
                List<String> comandos = central.tomarComandos(idWs);
                String respuestaFinal = adjuntarComandos(respuesta, comandos);
                try {
                    enviarRespuestaYEsperarAck(respuestaFinal);
                } catch (IOException e) {
                    // Las órdenes no llegaron a confirmarse: se reencolan para la
                    // siguiente sesión en lugar de perderse.
                    central.reencolarComandos(idWs, comandos);
                    throw e;
                }
            }
        } catch (SocketTimeoutException e) {
            if (!cerrada) System.err.println("Timeout de comunicación con WS " + idWs + "; se marca desconectada.");
        } catch (IOException e) {
            if (!cerrada) System.err.println("Conexión de WS " + idWs + " finalizada: " + e.getMessage());
        } catch (RuntimeException e) {
            if (!cerrada) System.err.println("Error atendiendo WS " + idWs + ": " + e.getMessage());
        } finally {
            if (registrada) central.desregistrarEstacion(idWs, this);
            cerrarSocket();
        }
    }

    /** Lee una petición, notifica ACK/NACK de su trama y devuelve DATA. */
    private String recibirTramaCliente() throws IOException {
        while (!cerrada && !socket.isClosed()) {
            int primero = entrada.read();
            if (primero < 0) throw new EOFException("La WS cerró el socket");
            if (primero == WireProtocol.EOT) return null;
            if (primero != WireProtocol.STX) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                System.err.println("WS " + idWs + ": se esperaba STX/EOT; recibido " + primero);
                continue;
            }
            try {
                String datos = WireProtocol.leerTramaTrasStx(entrada);
                WireProtocol.enviarControl(salida, WireProtocol.ACK);
                return datos;
            } catch (WireProtocol.BadLrcException e) {
                WireProtocol.enviarControl(salida, WireProtocol.NACK);
                System.err.println("WS " + idWs + ": " + e.getMessage() + "; esperando retransmisión.");
            }
        }
        return null;
    }

    private void enviarRespuestaYEsperarAck(String respuesta) throws IOException {
        for (int intento = 1; intento <= 3; intento++) {
            WireProtocol.enviarTrama(salida, respuesta);
            int confirmacion = entrada.read();
            if (confirmacion == WireProtocol.ACK) return;
            if (confirmacion == WireProtocol.NACK) {
                System.err.println("WS " + idWs + ": no validó la respuesta de CENTRAL; reintentando (" + intento + "/3).");
                continue;
            }
            if (confirmacion < 0) throw new EOFException("La WS cerró antes de confirmar la respuesta");
            if (confirmacion == WireProtocol.EOT) throw new EOFException("La WS terminó la sesión antes de confirmar la respuesta");
            throw new IOException("Se esperaba ACK/NACK de la respuesta y llegó " + confirmacion);
        }
        throw new IOException("La WS rechazó tres veces una respuesta de CENTRAL");
    }

    private static String adjuntarComandos(String respuesta, List<String> comandos) {
        if (comandos == null || comandos.isEmpty()) return respuesta;
        StringBuilder sb = new StringBuilder(respuesta == null ? "" : respuesta);
        for (String comando : comandos) sb.append("#CMD#").append(comando);
        return sb.toString();
    }

    private String procesarSaludo(String mensaje) {
        String[] h = mensaje.split("#", -1);
        if (h.length != 4 || !"HELLO".equals(h[0])) {
            return "HELLO_OK#KO#Se esperaba HELLO#id#ubicacion_codificada#caudal";
        }
        idWs = entero(h[1], -1);
        String ubicacion;
        try { ubicacion = URLDecoder.decode(h[2], StandardCharsets.UTF_8.name()); }
        catch (Exception e) { ubicacion = h[2]; }
        double caudalNominal = decimal(h[3], -1);
        if (idWs <= 0 || ubicacion.trim().isEmpty() || caudalNominal <= 0) {
            return "HELLO_OK#KO#ID, ubicación o caudal inválidos";
        }
        if (!central.puedeRegistrar(idWs)) {
            return "HELLO_OK#KO#WS no registrada en CENTRAL";
        }
        if (!central.registrarEstacion(idWs, ubicacion, caudalNominal, this)) {
            return "HELLO_OK#KO#No se pudo registrar la estación en SQLite";
        }
        registrada = true;
        return "HELLO_OK#OK#WS_" + idWs + "_REGISTRADA";
    }

    private String procesarMensaje(String mensaje) {
        String[] p = mensaje.split("#", -1);
        if (p.length == 7 && "STATUS".equals(p[0])) {
            String estado = p[1];
            double caudal = decimal(p[2], -1);
            double volumen = decimal(p[3], -1);
            long transcurrido = enteroLargo(p[4], -1);
            String idSolicitud = p[5];
            int idOperario = entero(p[6], 0);
            if (caudal < 0 || volumen < 0 || transcurrido < 0) return "ACK#STATUS#KO#TELEMETRIA_INVALIDA";
            boolean aceptado = central.alRecibirEstado(idWs, estado, caudal, volumen, transcurrido, idSolicitud, idOperario, this);
            return aceptado ? "ACK#STATUS#OK" : "ACK#STATUS#KO#ESTADO_INVALIDO_O_SESION_ANTIGUA";
        }
        if (p.length == 1 && "PING".equals(p[0])) return "PONG";
        return "ACK#KO#OPERACION_DESCONOCIDA";
    }

    public boolean sesionAbierta() { return !cerrada && socket.isConnected() && !socket.isClosed(); }
    public String direccionRemota() { SocketAddress direccion = socket.getRemoteSocketAddress(); return String.valueOf(direccion); }

    public void cerrarDesdeReemplazo() {
        cerrada = true;
        cerrarSocket();
    }

    private void cerrarSocket() {
        cerrada = true;
        try { socket.close(); } catch (IOException ignorada) { }
    }

    private static int entero(String x, int porDefecto) {
        try { return Integer.parseInt(x.trim()); } catch (Exception e) { return porDefecto; }
    }
    private static long enteroLargo(String x, long porDefecto) {
        try { return Long.parseLong(x.trim()); } catch (Exception e) { return porDefecto; }
    }
    private static double decimal(String x, double porDefecto) {
        try { double valor = Double.parseDouble(x.trim()); return Double.isFinite(valor) ? valor : porDefecto; }
        catch (Exception e) { return porDefecto; }
    }
}

```


## WM_Central/src/ServidorCentral.java

```java
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
import javax.swing.SwingUtilities;

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
            dbPath = args.length >= 4 ? args[3].trim() : env("WM_DB_PATH", buscarRutaBaseDatos());
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
            central.iniciar();

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

            boolean grafico = false;
            if (UiSupport.guiEnabled()) {
                grafico = lanzarInterfaz(servicio, socketPort, kafkaHost, kafkaPort, dbPath);
            }
            if (!grafico) {
                // Sin escritorio (Docker, servidores) se mantiene el panel y el menú de consola.
                central.mostrarPanel(true);
                iniciarPanel(central);
                iniciarConsolaAdmin(central);
            }

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

    private static boolean lanzarInterfaz(CentralService central, int socketPort, String kafkaHost,
                                          int kafkaPort, String dbPath) {
        try {
            SwingUtilities.invokeAndWait(() -> {
                CentralFrame frame = new CentralFrame(central, socketPort, kafkaHost, kafkaPort, dbPath);
                frame.setVisible(true);
            });
            System.out.println("Interfaz gráfica de WM_Central iniciada (WM_GUI=false para usar la consola).");
            return true;
        } catch (Throwable t) {
            System.err.println("No se pudo iniciar la interfaz gráfica de WM_Central: "
                    + t.getMessage() + ". Se usará la consola.");
            return false;
        }
    }

    private static void iniciarPanel(CentralService central) {
        Thread panel = new Thread(() -> {
            while (central.estaEjecutando()) {
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

    private static void iniciarConsolaAdmin(CentralService central) {
        Thread admin = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (central.estaEjecutando()) {
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
                            int id = enteroPositivo(in, "ID de operario: ");
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
                            int wsId = enteroNoNegativo(in, "ID de WS (0=todas conectadas y disponibles): ");
                            int segundos = enteroPositivo(in, "Duración en segundos (1-86400): ");
                            if (segundos > 86400) System.out.println("La duración máxima es 86400 segundos.");
                            else central.adminRiego(wsId, segundos);
                            break;
                        case "5":
                            central.adminOrden("BLOCK_WS", enteroNoNegativo(in, "ID de WS (0=todas conectadas): "));
                            break;
                        case "6":
                            central.adminOrden("ACTIVATE_WS", enteroNoNegativo(in, "ID de WS (0=todas conectadas): "));
                            break;
                        case "7":
                            central.adminOrden("STOP_IRRIGATION", enteroNoNegativo(in, "ID de WS (0=todas conectadas): "));
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

    private static int enteroPositivo(Scanner in, String prompt) {
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

    private static int enteroNoNegativo(Scanner in, String prompt) {
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

    private static String buscarRutaBaseDatos() {
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

```


## WM_Central/src/UiSupport.java

```java
import java.awt.GraphicsEnvironment;
import java.util.Locale;

/** Decides whether a graphical interface can be used in this environment. */
final class UiSupport {

    private UiSupport() { }

    /**
     * La interfaz gráfica se usa por defecto cuando hay un escritorio disponible.
     * Se puede desactivar con WM_GUI=false (por ejemplo, en Docker o en servidores
     * sin pantalla) para conservar el panel y el menú de consola.
     */
    static boolean guiEnabled() {
        String configured = System.getenv("WM_GUI");
        if (configured != null) {
            String value = configured.trim().toLowerCase(Locale.ROOT);
            if (value.equals("false") || value.equals("0") || value.equals("no") || value.equals("off")) {
                return false;
            }
        }
        try {
            return !GraphicsEnvironment.isHeadless();
        } catch (Throwable t) {
            return false;
        }
    }
}

```


## WM_Central/src/WireProtocol.java

```java
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Protocolo de tramas para el socket CENTRAL<->monitor WS.
 * Cada trama es STX + DATA UTF-8 + ETX + LRC; LRC es XOR de los bytes DATA.
 * ENQ/ACK/NACK/EOT se transmiten como bytes de control individuales, no como texto.
 */
public final class WireProtocol {
    public static final int ENQ = 0x05;
    public static final int ACK = 0x06;
    public static final int NACK = 0x15;
    public static final int STX = 0x02;
    public static final int ETX = 0x03;
    public static final int EOT = 0x04;

    private WireProtocol() { }

    public static void enviarControl(OutputStream out, int control) throws IOException {
        if (control != ENQ && control != ACK && control != NACK && control != EOT) {
            throw new IllegalArgumentException("Byte de control no válido: " + control);
        }
        synchronized (out) {
            out.write(control);
            out.flush();
        }
    }

    public static void enviarTrama(OutputStream out, String data) throws IOException {
        if (data == null || data.indexOf((char) STX) >= 0 || data.indexOf((char) ETX) >= 0) {
            throw new IllegalArgumentException("Mensaje nulo o con caracteres STX/ETX reservados");
        }
        byte[] payload = data.getBytes(StandardCharsets.UTF_8);
        int lrc = xor(payload);
        synchronized (out) {
            out.write(STX);
            out.write(payload);
            out.write(ETX);
            out.write(lrc);
            out.flush();
        }
    }

    /** Lee una trama completa; el primer byte de la trama todavía está por leer. */
    public static String leerTrama(InputStream in) throws IOException {
        int first = in.read();
        if (first < 0) throw new EOFException("Socket cerrado mientras se esperaba STX");
        if (first != STX) throw new IOException("Se esperaba STX; recibido " + first);
        return leerTramaTrasStx(in);
    }

    /** Lee DATA, ETX y LRC; el llamador ya ha consumido STX. */
    public static String leerTramaTrasStx(InputStream in) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int value;
        while ((value = in.read()) != ETX) {
            if (value < 0) throw new EOFException("Socket cerrado antes de ETX");
            data.write(value);
            if (data.size() > 1024 * 1024) throw new IOException("Trama mayor que 1 MiB; se rechaza");
        }
        int receivedLrc = in.read();
        if (receivedLrc < 0) throw new EOFException("Socket cerrado antes de LRC");
        byte[] payload = data.toByteArray();
        int calculated = xor(payload);
        if ((receivedLrc & 0xFF) != calculated) {
            throw new BadLrcException("LRC incorrecto: recibido=" + receivedLrc + ", calculado=" + calculated);
        }
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static int xor(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) value ^= (b & 0xFF);
        return value & 0xFF;
    }

    public static final class BadLrcException extends IOException {
        public BadLrcException(String message) { super(message); }
    }
}

```


## WM_FO/src/ClienteKafkaOperario.java

```java
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cliente Kafka de WM_FO.
 *
 * Contrato acordado con WM_Central (ver README.md):
 * - wm.riego.solicitudes: solicitudes de riego, listados y consultas de estado.
 * - wm.riego.respuestas: eventos de avance/finalización y respuestas a las consultas.
 *
 * Cada solicitud tiene un requestId UUID. El consumidor Kafka se ejecuta en un
 * hilo independiente; la interfaz nunca accede directamente al KafkaConsumer, que
 * no es thread-safe.
 *
 * Las solicitudes de riego se conservan en un fichero local mientras no se conozca
 * su conclusión, de modo que si el operario se cierra durante un servicio, al
 * recuperarse puede volver a preguntar su estado a CENTRAL (CONSULTAR_ESTADO) y
 * conocer el resultado sin perder ninguna transacción.
 */
public final class ClienteKafkaOperario implements AutoCloseable {

    public static final String TOPICO_SOLICITUDES = "wm.riego.solicitudes";
    public static final String TOPICO_RESPUESTAS = "wm.riego.respuestas";
    public static final String TOPICO_LATIDO = "wm.central.latido";

    private static final long VENTANA_CENTRAL_VIVA_MS = 8_000L;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TIPO_MAPA =
            new TypeReference<Map<String, Object>>() { };

    private static final int PARTICIONES_TEMA = 3;
    private static final short FACTOR_REPLICACION_TEMA = 1;
    private static final long TIMEOUT_ENVIO_SEGUNDOS = 35;
    private static final long TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS = 30;

    private final String servidoresKafka;
    private final int idOperario;
    private final String idSesion = UUID.randomUUID().toString();
    private final String idGrupoConsumidor;
    private final KafkaProducer<String, String> productor;

    private final AtomicBoolean activo = new AtomicBoolean(false);
    private final AtomicReference<Throwable> falloConsumidor = new AtomicReference<>();
    private final CountDownLatch consumidorListo = new CountDownLatch(1);
    private final Map<String, BlockingQueue<Map<String, Object>>> solicitudesPendientes =
            new ConcurrentHashMap<>();

    private volatile KafkaConsumer<String, String> consumidor;
    private volatile Thread hiloConsumidor;
    private volatile boolean iniciado;
    private volatile boolean cerrado;
    private volatile long ultimoLatidoMillis;
    private volatile long iniciadoEnMillis;

    public ClienteKafkaOperario(String servidoresKafka, int idOperario) {
        if (servidoresKafka == null || servidoresKafka.trim().isEmpty()) {
            throw new IllegalArgumentException("La dirección del broker no puede estar vacía.");
        }
        if (idOperario <= 0) {
            throw new IllegalArgumentException("El ID del operario debe ser un entero positivo.");
        }

        this.servidoresKafka = servidoresKafka.trim();
        this.idOperario = idOperario;
        this.idGrupoConsumidor = "wm-fo-" + idOperario + "-" + idSesion;
        this.productor = new KafkaProducer<>(crearConfiguracionProductor());
    }

    /**
     * Comprueba/crea los topics, arranca el consumidor y espera a que esté
     * asignado al topic de respuestas antes de permitir publicar peticiones.
     */
    public synchronized void iniciar() throws Exception {
        if (cerrado) {
            throw new IllegalStateException("Este cliente Kafka ya está cerrado.");
        }
        if (iniciado) {
            return;
        }

        if (gestionTopicsHabilitada()) asegurarTopics();
        activo.set(true);
        hiloConsumidor = new Thread(this::bucleConsumo, "wm-fo-kafka-consumidor-" + idOperario);
        hiloConsumidor.setDaemon(true);
        hiloConsumidor.start();

        boolean listo = consumidorListo.await(TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS, TimeUnit.SECONDS);
        Throwable fallo = falloConsumidor.get();
        if (!listo || fallo != null || !hiloConsumidor.isAlive()) {
            cerrar();
            if (fallo != null) {
                throw new IllegalStateException("El consumidor Kafka no pudo arrancar: "
                        + fallo.getMessage(), fallo);
            }
            throw new TimeoutException("Kafka no asignó el topic de respuestas en "
                    + TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS + " segundos. Comprueba el broker y los topics.");
        }

        iniciado = true;
        iniciadoEnMillis = System.currentTimeMillis();
        System.out.println("Kafka conectado. Consumidor listo para recibir respuestas.");
        System.out.println("Topics: " + TOPICO_SOLICITUDES + " / " + TOPICO_RESPUESTAS + " / " + TOPICO_LATIDO);
    }

    /**
     * Indica si WM_Central sigue viva según los latidos publicados por Kafka.
     * Permite avisar al operario aunque el broker de Kafka siga funcionando.
     */
    public boolean centralViva() {
        if (!iniciado || cerrado) return false;
        long referencia = Math.max(ultimoLatidoMillis, iniciadoEnMillis);
        return System.currentTimeMillis() - referencia <= VENTANA_CENTRAL_VIVA_MS;
    }

    public String solicitarRiego(int idWs, int duracionSegundos) throws Exception {
        if (idWs <= 0) {
            throw new IllegalArgumentException("El ID de la WS debe ser positivo.");
        }
        if (duracionSegundos <= 0 || duracionSegundos > 86400) {
            throw new IllegalArgumentException("La duración debe estar entre 1 y 86400 segundos.");
        }

        String idSolicitud = UUID.randomUUID().toString();
        Map<String, Object> carga = new HashMap<>();
        carga.put("type", "RIEGO_REQUEST");
        carga.put("requestId", idSolicitud);
        carga.put("operatorId", idOperario);
        carga.put("wsId", idWs);
        carga.put("durationSeconds", duracionSegundos);
        carga.put("createdAt", Instant.now().toString());

        publicarSolicitud(idSolicitud, carga);
        persistirPendiente(idSolicitud, idWs, duracionSegundos);
        return idSolicitud;
    }

    public String solicitarListadoWsDisponibles() throws Exception {
        String idSolicitud = UUID.randomUUID().toString();
        Map<String, Object> carga = new HashMap<>();
        carga.put("type", "LIST_AVAILABLE_WS");
        carga.put("requestId", idSolicitud);
        carga.put("operatorId", idOperario);
        carga.put("createdAt", Instant.now().toString());

        publicarSolicitud(idSolicitud, carga);
        return idSolicitud;
    }

    /**
     * Pregunta a CENTRAL el estado de una solicitud previa (p. ej. un riego que
     * quedó pendiente cuando el operario se cerró). La respuesta llega con el
     * mismo requestId, por lo que se correlaciona con {@link #esperarEvento}.
     */
    public void solicitarEstado(String idSolicitud) throws Exception {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = new ArrayBlockingQueue<>(1000);
        solicitudesPendientes.put(idSolicitud, cola);
        try {
            Map<String, Object> carga = new HashMap<>();
            carga.put("type", "CONSULTAR_ESTADO");
            carga.put("requestId", idSolicitud);
            carga.put("operatorId", idOperario);
            carga.put("createdAt", Instant.now().toString());
            String textoJson = JSON.writeValueAsString(carga);
            productor.send(new ProducerRecord<>(TOPICO_SOLICITUDES, Integer.toString(idOperario), textoJson))
                    .get(TIMEOUT_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
            System.out.println("Consulta de estado publicada en Kafka. ID: " + idSolicitud);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("Se interrumpió el envío de la consulta a Kafka.", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("No se pudo confirmar el envío de la consulta a Kafka: "
                    + causaRaiz(e).getMessage(), e);
        } catch (IOException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw e;
        }
    }

    private void publicarSolicitud(String idSolicitud, Map<String, Object> carga) throws Exception {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = new ArrayBlockingQueue<>(1000);
        solicitudesPendientes.put(idSolicitud, cola);

        try {
            String textoJson = JSON.writeValueAsString(carga);
            ProducerRecord<String, String> registro = new ProducerRecord<>(
                    TOPICO_SOLICITUDES,
                    Integer.toString(idOperario),
                    textoJson);

            productor.send(registro).get(TIMEOUT_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
            System.out.println("Solicitud publicada en Kafka. ID: " + idSolicitud);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("Se interrumpió el envío a Kafka.", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("No se pudo confirmar el envío a Kafka: "
                    + causaRaiz(e).getMessage()
                    + ". El resultado del envío puede ser incierto si se agotó el tiempo de espera.", e);
        } catch (IOException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw e;
        }
    }

    /** Devuelve null al agotar el timeout; el hilo consumidor permanece independiente. */
    public Map<String, Object> esperarEvento(String idSolicitud, Duration timeout)
            throws InterruptedException, IOException {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = solicitudesPendientes.get(idSolicitud);
        if (cola == null) {
            throw new IllegalStateException("No existe una solicitud pendiente con ID " + idSolicitud);
        }

        Map<String, Object> evento = cola.poll(Math.max(1L, timeout.toMillis()), TimeUnit.MILLISECONDS);
        Throwable fallo = falloConsumidor.get();
        if (evento == null && fallo != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + fallo.getMessage(), fallo);
        }
        if (evento == null && (hiloConsumidor == null || !hiloConsumidor.isAlive())) {
            throw new IOException("El consumidor Kafka ya no está activo.");
        }
        return evento;
    }

    /** Elimina únicamente la cola en memoria; no altera el fichero de pendientes. */
    public void finalizarSolicitud(String idSolicitud) {
        if (idSolicitud != null) {
            solicitudesPendientes.remove(idSolicitud);
        }
    }

    /** Marca una solicitud de riego como resuelta (elimina su entrada del fichero). */
    public void resolverPendiente(String idSolicitud) {
        eliminarPendiente(idSolicitud);
    }

    private void bucleConsumo() {
        boolean posicionInicialFijada = false;
        try {
            // Si se pierde la conexión o Kafka reinicia, se crea un consumidor nuevo.
            // Se conserva el group.id de esta ejecución y se confirman offsets para
            // poder reanudar desde el último lote procesado después de una reconexión.
            while (activo.get()) {
                try (KafkaConsumer<String, String> kafkaConsumer =
                             new KafkaConsumer<>(crearConfiguracionConsumidor())) {
                    consumidor = kafkaConsumer;
                    kafkaConsumer.subscribe(Arrays.asList(TOPICO_RESPUESTAS, TOPICO_LATIDO));

                    while (activo.get()) {
                        ConsumerRecords<String, String> registros =
                                kafkaConsumer.poll(Duration.ofMillis(500));

                        // Este grupo es exclusivo de esta instancia de WM_FO. Al iniciar,
                        // salta mensajes antiguos y confirma esa posición antes de enviar
                        // solicitudes para no perder respuestas nuevas.
                        if (!posicionInicialFijada && !kafkaConsumer.assignment().isEmpty()) {
                            Collection<TopicPartition> asignacion = kafkaConsumer.assignment();
                            kafkaConsumer.seekToEnd(asignacion);
                            for (TopicPartition particion : asignacion) {
                                kafkaConsumer.position(particion); // resuelve el offset final
                            }
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                            posicionInicialFijada = true;
                            consumidorListo.countDown();
                            continue;
                        }

                        if (!registros.isEmpty()) {
                            for (ConsumerRecord<String, String> registro : registros) {
                                despachar(registro.topic(), registro.value());
                            }
                            // Confirmar solo después de colocar los eventos en sus colas.
                            // En caso de fallo podría haber una redelivery, que se tolera.
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                        }
                    }
                } catch (WakeupException e) {
                    if (!activo.get()) {
                        break;
                    }
                    System.err.println("Se interrumpió el consumidor Kafka; se recreará la conexión.");
                    dormirReintento();
                } catch (Exception e) {
                    if (!activo.get()) {
                        break;
                    }
                    System.err.println("Error del consumidor Kafka: " + e.getMessage()
                            + ". Se reintentará automáticamente.");
                    dormirReintento();
                } finally {
                    consumidor = null;
                }
            }
        } catch (Throwable t) {
            if (activo.get()) {
                falloConsumidor.set(t);
                System.err.println("El consumidor Kafka se ha detenido de forma inesperada: " + t.getMessage());
            }
        } finally {
            consumidorListo.countDown();
        }
    }

    private void despachar(String tema, String textoJson) {
        if (TOPICO_LATIDO.equals(tema)) {
            ultimoLatidoMillis = System.currentTimeMillis();
            return;
        }
        try {
            Map<String, Object> evento = JSON.readValue(textoJson, TIPO_MAPA);
            String idSolicitud = texto(evento.get("requestId"));
            if (idSolicitud == null || idSolicitud.trim().isEmpty()) {
                System.err.println("Evento Kafka ignorado: no contiene requestId.");
                return;
            }

            Object valorOperario = evento.get("operatorId");
            if (valorOperario != null && entero(valorOperario, -1) != idOperario) {
                return;
            }

            BlockingQueue<Map<String, Object>> cola = solicitudesPendientes.get(idSolicitud);
            if (cola != null && !cola.offer(evento)) {
                System.err.println("Se ha llenado la cola local de eventos de " + idSolicitud
                        + "; se ha descartado un evento para no bloquear el consumidor Kafka.");
            }
            // Los eventos de otras solicitudes/operarios se descartan aquí: no bloquean
            // ni se muestran como notificaciones propias de esta instancia de WM_FO.
        } catch (Exception e) {
            System.err.println("Se ha ignorado un mensaje de respuesta Kafka mal formado: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ Persistencia

    private Path rutaFicheroPendientes() {
        String directorio = System.getenv("WM_FO_STATE_DIR");
        if (directorio == null || directorio.trim().isEmpty()) directorio = "data";
        return Paths.get(directorio, "fo_" + idOperario + ".pendientes");
    }

    /** Carga las solicitudes de riego sin resolver de ejecuciones anteriores. */
    public List<Pendiente> cargarPendientes() {
        List<Pendiente> pendientes = new ArrayList<>();
        Path ruta = rutaFicheroPendientes();
        if (!Files.isRegularFile(ruta)) return pendientes;
        try (BufferedReader lector = Files.newBufferedReader(ruta, StandardCharsets.UTF_8)) {
            String linea;
            while ((linea = lector.readLine()) != null) {
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) continue;
                String[] campos = contenido.split(";", -1);
                if (campos.length < 2) continue;
                try {
                    String idSolicitud = campos[0].trim();
                    int idWs = Integer.parseInt(campos[1].trim());
                    int duracion = campos.length >= 3 ? Integer.parseInt(campos[2].trim()) : 0;
                    if (!idSolicitud.isEmpty() && idWs > 0) {
                        pendientes.add(new Pendiente(idSolicitud, idWs, duracion));
                    }
                } catch (NumberFormatException ignorada) {
                    // Línea corrupta: se omite sin detener la recuperación.
                }
            }
        } catch (IOException e) {
            System.err.println("No se pudo leer el fichero de pendientes " + ruta + ": " + e.getMessage());
        }
        return pendientes;
    }

    private synchronized void persistirPendiente(String idSolicitud, int idWs, int duracion) {
        try {
            Path ruta = rutaFicheroPendientes();
            if (ruta.getParent() != null) Files.createDirectories(ruta.getParent());
            try (BufferedWriter escritor = Files.newBufferedWriter(ruta, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
                escritor.write(idSolicitud + ";" + idWs + ";" + duracion);
                escritor.newLine();
            }
        } catch (IOException e) {
            System.err.println("No se pudo persistir la solicitud pendiente: " + e.getMessage());
        }
    }

    private synchronized void eliminarPendiente(String idSolicitud) {
        if (idSolicitud == null) return;
        Path ruta = rutaFicheroPendientes();
        if (!Files.isRegularFile(ruta)) return;
        try {
            List<String> lineas = Files.readAllLines(ruta, StandardCharsets.UTF_8);
            List<String> restantes = new ArrayList<>();
            for (String linea : lineas) {
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) {
                    restantes.add(linea);
                    continue;
                }
                String[] campos = contenido.split(";", -1);
                if (campos.length > 0 && idSolicitud.equals(campos[0].trim())) continue;
                restantes.add(linea);
            }
            Files.write(ruta, restantes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("No se pudo actualizar el fichero de pendientes: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ Kafka (apoyo)

    private boolean gestionTopicsHabilitada() {
        String configurado = System.getenv("WM_FO_MANAGE_TOPICS");
        return configurado == null || Boolean.parseBoolean(configurado.trim());
    }

    private void asegurarTopics() throws Exception {
        Properties propiedades = new Properties();
        propiedades.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        propiedades.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        propiedades.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

        try (AdminClient admin = AdminClient.create(propiedades)) {
            List<String> temas = Arrays.asList(TOPICO_SOLICITUDES, TOPICO_RESPUESTAS, TOPICO_LATIDO);
            for (String tema : temas) {
                boolean verificado = false;
                for (int intento = 1; intento <= 3 && !verificado; intento++) {
                    try {
                        Set<String> existentes = admin.listTopics().names().get(10, TimeUnit.SECONDS);
                        if (existentes.contains(tema)) {
                            verificado = true;
                            continue;
                        }
                        NewTopic nuevoTema = new NewTopic(tema, PARTICIONES_TEMA, FACTOR_REPLICACION_TEMA);
                        admin.createTopics(Collections.singleton(nuevoTema)).all().get(15, TimeUnit.SECONDS);
                        verificado = true;
                        System.out.println("Topic Kafka creado: " + tema);
                    } catch (ExecutionException e) {
                        Throwable causa = causaRaiz(e);
                        if (causa instanceof TopicExistsException) {
                            verificado = true; // otro proceso lo pudo crear al mismo tiempo
                        } else if (intento == 3) {
                            throw new IOException("No se pudo crear/verificar el topic '" + tema
                                    + "'. Comprueba la disponibilidad del broker y los permisos de administración.", causa);
                        } else {
                            dormirBackoff(intento);
                        }
                    } catch (TimeoutException e) {
                        if (intento == 3) {
                            throw new IOException("Timeout al crear/verificar el topic '" + tema + "'.", e);
                        }
                        dormirBackoff(intento);
                    }
                }
                if (!verificado) {
                    throw new IOException("No se pudo verificar el topic Kafka '" + tema + "'.");
                }
            }
        }
    }

    private Properties crearConfiguracionProductor() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "wm-fo-productor-" + idOperario + "-" + idSesion);
        return p;
    }

    private Properties crearConfiguracionConsumidor() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, idGrupoConsumidor);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "wm-fo-consumidor-" + idOperario + "-" + idSesion);
        return p;
    }

    private void comprobarListo() throws IOException {
        if (!iniciado || cerrado) {
            throw new IOException("El cliente Kafka de WM_FO no está iniciado o ya está cerrado.");
        }
        if (falloConsumidor.get() != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + falloConsumidor.get().getMessage(),
                    falloConsumidor.get());
        }
    }

    // ------------------------------------------------------------ Utilidades

    private static Throwable causaRaiz(Throwable t) {
        Throwable actual = t;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual;
    }

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static int entero(Object valor, int porDefecto) {
        if (valor instanceof Number) {
            return ((Number) valor).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(valor));
        } catch (Exception ignorada) {
            return porDefecto;
        }
    }

    private static void dormirReintento() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void dormirBackoff(int intento) throws InterruptedException {
        Thread.sleep(Math.min(1000L * intento, 3000L));
    }

    @Override
    public void close() {
        cerrar();
    }

    public synchronized void cerrar() {
        if (cerrado) {
            return;
        }
        cerrado = true;
        activo.set(false);
        KafkaConsumer<String, String> c = consumidor;
        if (c != null) {
            c.wakeup();
        }
        Thread t = hiloConsumidor;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            productor.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            System.err.println("Aviso cerrando el productor Kafka: " + e.getMessage());
        }
        solicitudesPendientes.clear();
        iniciado = false;
    }

    /** Solicitud de riego conservada entre ejecuciones mientras no se resuelva. */
    public static final class Pendiente {
        public final String idSolicitud;
        public final int idWs;
        public final int duracionSegundos;

        Pendiente(String idSolicitud, int idWs, int duracionSegundos) {
            this.idSolicitud = idSolicitud;
            this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
        }
    }
}

```


## WM_FO/src/FOFrame.java

```java
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.Rectangle;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Interfaz gráfica de WM_FO (operario de campo).
 *
 * Permite solicitar un riego manual, procesar un fichero de servicios de forma
 * secuencial y consultar las WS disponibles, mostrando el resultado de CENTRAL y
 * avisando cuando dejan de llegar los latidos de la central. Al arrancar recupera
 * el estado de los riegos que quedaron pendientes de una sesión anterior.
 */
final class FOFrame extends JFrame {

    private static final long PAUSA_ENTRE_SERVICIOS_MS = 4000L;
    private static final long TIMEOUT_LISTADO_SEGUNDOS = 15L;
    private static final long TIMEOUT_MINIMO_SERVICIO_SEGUNDOS = 120L;
    private static final long SEGUNDOS_EXTRA_SERVICIO = 120L;
    private static final long TIMEOUT_RECUPERACION_SEGUNDOS = 30L;

    private static final Color FONDO = new Color(0x1E1E1E);
    private static final Color FONDO_PANEL = new Color(0x252526);
    private static final Color TEXTO = new Color(0xF2F2F2);
    private static final Color ATENUADO = new Color(0xBBBBBB);
    private static final Color VERDE = new Color(0x66BB6A);
    private static final Color ROJO = new Color(0xEF5350);
    private static final Color NARANJA = new Color(0xFFA726);

    private final String ipBroker;
    private final int puertoBroker;
    private final int idOperario;

    private volatile ClienteKafkaOperario kafka;
    private volatile boolean conectado;
    private volatile boolean ocupado;
    private volatile boolean conectando;
    private volatile boolean cerrando;

    private final JLabel etiquetaKafka = new JLabel("Kafka: desconectado");
    private final JLabel etiquetaCentral = new JLabel("Central: sin conexión");
    private final JTextArea areaRegistro = new JTextArea(10, 80);
    private final DefaultTableModel modeloEstaciones = new DefaultTableModel(
            new Object[]{"ID WS", "Ubicación", "Estado"}, 0) {
        @Override public boolean isCellEditable(int fila, int columna) { return false; }
    };

    private final JSpinner selectorWs = new JSpinner(new SpinnerNumberModel(1, 1, Integer.MAX_VALUE, 1));
    private final JSpinner selectorDuracion = new JSpinner(new SpinnerNumberModel(10, 1, 86400, 1));
    private final JTextField campoFichero = new JTextField();
    private final JButton botonManual = new JButton("Solicitar riego");
    private final JButton botonFichero = new JButton("Procesar fichero");
    private final JButton botonListado = new JButton("Consultar WS disponibles");
    private final JButton botonReconectar = new JButton("Reconectar");

    FOFrame(String ipBroker, int puertoBroker, int idOperario) {
        super("WM_FO - Operario " + idOperario);
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperario = idOperario;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                cerrarCliente();
                dispose();
                System.exit(0);
            }
        });

        getContentPane().setBackground(FONDO);
        setLayout(new BorderLayout());
        add(cabecera(), BorderLayout.NORTH);
        add(pestanas(), BorderLayout.CENTER);
        add(panelSur(), BorderLayout.SOUTH);

        botonManual.addActionListener(e -> solicitarManual());
        botonFichero.addActionListener(e -> procesarFichero());
        botonListado.addActionListener(e -> consultarDisponibles());
        botonReconectar.addActionListener(e -> conectar());

        Rectangle pantalla = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(1000, Math.max(760, pantalla.width - 60)),
                Math.min(720, Math.max(500, pantalla.height - 60)));
        setMinimumSize(new Dimension(720, 480));
        setLocationRelativeTo(null);

        actualizarControles();
        conectar();
        new Timer(1000, e -> actualizarEstadoCentral()).start();
    }

    // ------------------------------------------------------------------ Layout

    private JComponent cabecera() {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel titulo = new JLabel("WM_FO - Operario de campo");
        titulo.setFont(new Font("SansSerif", Font.BOLD, 22));
        titulo.setForeground(TEXTO);

        JLabel subtitulo = new JLabel("Operario " + idOperario + "  ·  Kafka "
                + formatearBootstrap(ipBroker, puertoBroker));
        subtitulo.setFont(new Font("SansSerif", Font.PLAIN, 12));
        subtitulo.setForeground(ATENUADO);

        JPanel izquierda = new JPanel();
        izquierda.setOpaque(false);
        izquierda.setLayout(new BoxLayout(izquierda, BoxLayout.Y_AXIS));
        izquierda.add(titulo);
        izquierda.add(Box.createVerticalStrut(3));
        izquierda.add(subtitulo);

        etiquetaKafka.setFont(new Font("SansSerif", Font.BOLD, 12));
        etiquetaKafka.setForeground(ATENUADO);
        etiquetaCentral.setFont(new Font("SansSerif", Font.BOLD, 12));
        etiquetaCentral.setForeground(ATENUADO);
        botonReconectar.setEnabled(false);

        JPanel distintivos = new JPanel();
        distintivos.setOpaque(false);
        distintivos.setLayout(new BoxLayout(distintivos, BoxLayout.Y_AXIS));
        etiquetaKafka.setAlignmentX(Component.RIGHT_ALIGNMENT);
        etiquetaCentral.setAlignmentX(Component.RIGHT_ALIGNMENT);
        botonReconectar.setAlignmentX(Component.RIGHT_ALIGNMENT);
        distintivos.add(etiquetaKafka);
        distintivos.add(Box.createVerticalStrut(3));
        distintivos.add(etiquetaCentral);
        distintivos.add(Box.createVerticalStrut(5));
        distintivos.add(botonReconectar);

        panel.add(izquierda, BorderLayout.WEST);
        panel.add(distintivos, BorderLayout.EAST);
        return panel;
    }

    private JComponent pestanas() {
        JTabbedPane pestanas = new JTabbedPane();
        pestanas.addTab("Riego manual", pestanaManual());
        pestanas.addTab("Servicios desde fichero", pestanaFichero());
        pestanas.addTab("WS disponibles", pestanaDisponibles());
        return pestanas;
    }

    private JComponent pestanaManual() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(FONDO);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        panel.add(fila(etiqueta("Estación (WS):"), selectorWs));
        panel.add(fila(etiqueta("Duración del riego (s):"), selectorDuracion));
        panel.add(Box.createVerticalStrut(6));
        panel.add(nota("La solicitud se envía a CENTRAL por Kafka. La ventana mostrará los "
                + "eventos RECEIVED, CHECKING, AUTHORIZED, STARTED, PROGRESS y COMPLETED."));
        panel.add(Box.createVerticalStrut(10));
        panel.add(filaIzquierda(botonManual));
        return panel;
    }

    private JComponent pestanaFichero() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(FONDO);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        campoFichero.setPreferredSize(new Dimension(430, 26));
        JButton examinar = new JButton("Examinar...");
        examinar.addActionListener(e -> elegirFichero());
        panel.add(fila(etiqueta("Fichero:"), campoFichero, examinar));
        panel.add(Box.createVerticalStrut(6));
        panel.add(nota("Formato: una línea por servicio con wsId;duraciónSegundos. "
                + "Se admiten líneas vacías y comentarios que empiecen por #. "
                + "Los servicios se envían en orden con una pausa de 4 segundos."));
        panel.add(Box.createVerticalStrut(10));
        panel.add(filaIzquierda(botonFichero));
        return panel;
    }

    private JComponent pestanaDisponibles() {
        JPanel panel = new JPanel(new BorderLayout(0, 10));
        panel.setBackground(FONDO);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        JTable tabla = new JTable(modeloEstaciones);
        tabla.setRowHeight(24);
        JScrollPane desplazamiento = new JScrollPane(tabla);
        desplazamiento.getViewport().setBackground(Color.WHITE);

        JPanel superior = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        superior.setOpaque(false);
        superior.add(botonListado);

        panel.add(superior, BorderLayout.NORTH);
        panel.add(desplazamiento, BorderLayout.CENTER);
        return panel;
    }

    private JComponent panelSur() {
        JPanel sur = new JPanel(new BorderLayout(0, 4));
        sur.setBackground(FONDO_PANEL);
        sur.setBorder(new EmptyBorder(6, 10, 6, 10));

        areaRegistro.setEditable(false);
        areaRegistro.setBackground(new Color(0x111111));
        areaRegistro.setForeground(new Color(0xDCDCDC));
        areaRegistro.setFont(new Font("Consolas", Font.PLAIN, 12));
        areaRegistro.setLineWrap(true);
        areaRegistro.setWrapStyleWord(true);
        JScrollPane desplazamiento = new JScrollPane(areaRegistro);
        desplazamiento.setPreferredSize(new Dimension(0, 220));
        desplazamiento.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(0x505050)), "Registro de eventos",
                javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.TOP,
                new Font("SansSerif", Font.BOLD, 12), TEXTO));

        JLabel sugerencia = new JLabel("Consejo: mantén abierta esta ventana para no perder eventos de CENTRAL.");
        sugerencia.setForeground(ATENUADO);
        sugerencia.setFont(new Font("SansSerif", Font.PLAIN, 11));

        sur.add(desplazamiento, BorderLayout.CENTER);
        sur.add(sugerencia, BorderLayout.SOUTH);
        return sur;
    }

    private JPanel fila(JComponent... componentes) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        for (JComponent componente : componentes) panel.add(componente);
        return panel;
    }

    private JPanel filaIzquierda(JComponent componente) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.add(componente);
        return panel;
    }

    private JLabel etiqueta(String texto) {
        JLabel etiqueta = new JLabel(texto);
        etiqueta.setForeground(TEXTO);
        etiqueta.setFont(new Font("SansSerif", Font.PLAIN, 13));
        return etiqueta;
    }

    private JLabel nota(String texto) {
        JLabel etiqueta = new JLabel("<html><div style='width:620px'>" + texto + "</div></html>");
        etiqueta.setForeground(ATENUADO);
        etiqueta.setFont(new Font("SansSerif", Font.PLAIN, 12));
        etiqueta.setAlignmentX(Component.LEFT_ALIGNMENT);
        return etiqueta;
    }

    // ------------------------------------------------------------------ Kafka

    private synchronized void conectar() {
        if (conectando || cerrando) return;
        conectando = true;
        ocupado = true;
        actualizarControles();
        actualizarEstadoKafka("Kafka: conectando...", ATENUADO);

        new Thread(() -> {
            ClienteKafkaOperario anterior = kafka;
            kafka = null;
            conectado = false;
            if (anterior != null) {
                try { anterior.cerrar(); } catch (Exception ignorada) { }
            }
            ClienteKafkaOperario cliente = null;
            try {
                cliente = new ClienteKafkaOperario(
                        formatearBootstrap(ipBroker, puertoBroker), idOperario);
                cliente.iniciar();
                if (cerrando) {
                    cliente.cerrar();
                    return;
                }
                kafka = cliente;
                conectado = true;
                registrar("Kafka conectado. Topics: " + ClienteKafkaOperario.TOPICO_SOLICITUDES
                        + " / " + ClienteKafkaOperario.TOPICO_RESPUESTAS);
                SwingUtilities.invokeLater(() -> actualizarEstadoKafka("Kafka: conectado", VERDE));
                recuperarPendientes();
            } catch (Exception e) {
                conectado = false;
                if (cliente != null) {
                    try { cliente.cerrar(); } catch (Exception ignorada) { }
                }
                registrar("No se pudo iniciar la comunicación Kafka: " + e.getMessage());
                SwingUtilities.invokeLater(() -> actualizarEstadoKafka("Kafka: sin conexión", ROJO));
            } finally {
                conectando = false;
                ocupado = false;
                SwingUtilities.invokeLater(this::actualizarControles);
            }
        }, "wm-fo-gui-conectar").start();
    }

    /** Recupera el estado de los riegos pendientes de sesiones anteriores. */
    private void recuperarPendientes() {
        ClienteKafkaOperario cliente = kafka;
        if (cliente == null) return;
        List<ClienteKafkaOperario.Pendiente> pendientes = cliente.cargarPendientes();
        if (pendientes.isEmpty()) return;
        registrar("Se han encontrado " + pendientes.size()
                + " servicios pendientes de sesiones anteriores. Consultando su estado...");
        for (ClienteKafkaOperario.Pendiente pendiente : pendientes) {
            try {
                cliente.solicitarEstado(pendiente.idSolicitud);
                long limite = System.nanoTime()
                        + Duration.ofSeconds(TIMEOUT_RECUPERACION_SEGUNDOS).toNanos();
                boolean resuelto = false;
                while (System.nanoTime() < limite) {
                    long restante = limite - System.nanoTime();
                    Map<String, Object> evento = cliente.esperarEvento(pendiente.idSolicitud,
                            Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                    if (evento == null) continue;
                    registrar("[RECUPERADO] " + describir(evento));
                    String estado = texto(evento.get("status"));
                    if (esExito(estado) || esFracaso(estado)) {
                        cliente.resolverPendiente(pendiente.idSolicitud);
                        resuelto = true;
                        break;
                    }
                }
                if (!resuelto) {
                    registrar("No se conoce aún la conclusión de " + pendiente.idSolicitud
                            + "; seguirá pendiente para la próxima ejecución.");
                }
            } catch (Exception e) {
                registrar("No se pudo recuperar " + pendiente.idSolicitud + ": " + e.getMessage());
            } finally {
                cliente.finalizarSolicitud(pendiente.idSolicitud);
            }
        }
    }

    private void cerrarCliente() {
        cerrando = true;
        ClienteKafkaOperario cliente = kafka;
        kafka = null;
        conectado = false;
        if (cliente != null) {
            try { cliente.cerrar(); } catch (Exception ignorada) { }
        }
    }

    private void actualizarControles() {
        boolean listo = conectado && !ocupado;
        botonManual.setEnabled(listo);
        botonFichero.setEnabled(listo);
        botonListado.setEnabled(listo);
        botonReconectar.setEnabled(!ocupado);
    }

    private void actualizarEstadoKafka(String texto, Color color) {
        etiquetaKafka.setText(texto);
        etiquetaKafka.setForeground(color);
    }

    private void actualizarEstadoCentral() {
        boolean vivo = conectado && !cerrando && kafka != null && kafka.centralViva();
        if (!conectado) {
            etiquetaCentral.setText("Central: sin conexión");
            etiquetaCentral.setForeground(ATENUADO);
        } else if (vivo) {
            etiquetaCentral.setText("Central: CONECTADA");
            etiquetaCentral.setForeground(VERDE);
        } else {
            etiquetaCentral.setText("Central: SIN LATIDO");
            etiquetaCentral.setForeground(ROJO);
        }
    }

    // ------------------------------------------------------------------ Acciones

    private void solicitarManual() {
        if (!listoParaOperar()) return;
        int idWs = ((Number) selectorWs.getValue()).intValue();
        int duracion = ((Number) selectorDuracion.getValue()).intValue();
        comenzarOperacion();
        new Thread(() -> {
            try {
                ejecutarServicio(idWs, duracion, "Solicitud manual");
            } finally {
                SwingUtilities.invokeLater(this::terminarOperacion);
            }
        }, "wm-fo-gui-manual").start();
    }

    private void procesarFichero() {
        if (!listoParaOperar()) return;
        String rutaTexto = campoFichero.getText().trim();
        if (rutaTexto.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Indica la ruta del fichero de servicios.",
                    "WM_FO", JOptionPane.WARNING_MESSAGE);
            return;
        }

        final List<Servicio> servicios;
        try {
            servicios = leerServicios(Paths.get(rutaTexto));
        } catch (IOException | IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, "No se ha podido cargar el fichero:\n" + e.getMessage(),
                    "WM_FO", JOptionPane.ERROR_MESSAGE);
            return;
        }
        if (servicios.isEmpty()) {
            JOptionPane.showMessageDialog(this, "El fichero no contiene servicios.",
                    "WM_FO", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        comenzarOperacion();
        new Thread(() -> {
            try {
                registrar("Se han validado " + servicios.size() + " servicios. Comienza el envío secuencial.");
                for (int i = 0; i < servicios.size(); i++) {
                    Servicio servicio = servicios.get(i);
                    registrar("Servicio " + (i + 1) + "/" + servicios.size() + " (línea " + servicio.linea
                            + "): WS=" + servicio.idWs + ", duración=" + servicio.duracionSegundos + " s");
                    Resultado resultado = ejecutarServicio(servicio.idWs, servicio.duracionSegundos,
                            "Servicio de fichero, línea " + servicio.linea);
                    if (resultado == Resultado.DESCONOCIDO) {
                        registrar("Se detiene el procesamiento: no se conoce la conclusión del servicio.");
                        return;
                    }
                    if (i < servicios.size() - 1) {
                        registrar("Esperando 4 segundos antes del siguiente servicio...");
                        try {
                            Thread.sleep(PAUSA_ENTRE_SERVICIOS_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            registrar("Procesamiento del fichero interrumpido.");
                            return;
                        }
                    }
                }
                registrar("Procesamiento del fichero terminado.");
            } finally {
                SwingUtilities.invokeLater(this::terminarOperacion);
            }
        }, "wm-fo-gui-fichero").start();
    }

    private void consultarDisponibles() {
        if (!listoParaOperar()) return;
        comenzarOperacion();
        new Thread(() -> {
            try {
                listarDisponibles();
            } finally {
                SwingUtilities.invokeLater(this::terminarOperacion);
            }
        }, "wm-fo-gui-listado").start();
    }

    private Resultado ejecutarServicio(int idWs, int duracionSegundos, String origen) {
        ClienteKafkaOperario cliente = kafka;
        if (cliente == null) {
            registrar("Kafka no está conectado.");
            return Resultado.DESCONOCIDO;
        }

        final String idSolicitud;
        try {
            registrar(origen + ". Operario=" + idOperario + ", WS=" + idWs
                    + ", duración=" + duracionSegundos + " s");
            idSolicitud = cliente.solicitarRiego(idWs, duracionSegundos);
            registrar("Solicitud publicada en Kafka. Request ID: " + idSolicitud);
        } catch (Exception e) {
            registrar("No se pudo publicar la solicitud: " + e.getMessage());
            return Resultado.DESCONOCIDO;
        }

        try {
            long timeout = segundosTimeoutServicio(duracionSegundos);
            long limite = System.nanoTime() + Duration.ofSeconds(timeout).toNanos();
            while (true) {
                long restante = limite - System.nanoTime();
                if (restante <= 0) {
                    registrar("Timeout: han pasado " + timeout
                            + " s sin conclusión. El resultado es desconocido; el riego no se cancela.");
                    return Resultado.DESCONOCIDO;
                }
                Map<String, Object> evento = cliente.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                if (evento == null) continue;

                registrar(describir(evento));
                String estado = texto(evento.get("status"));
                if (esExito(estado)) {
                    registrar("Riego finalizado correctamente.");
                    cliente.resolverPendiente(idSolicitud);
                    return Resultado.EXITO;
                }
                if (esFracaso(estado)) {
                    registrar("El servicio ha concluido sin éxito (" + estado + ").");
                    cliente.resolverPendiente(idSolicitud);
                    return Resultado.FRACASO;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            registrar("Espera interrumpida; el resultado del servicio es desconocido.");
            return Resultado.DESCONOCIDO;
        } catch (Exception e) {
            registrar("Se ha perdido la capacidad de recibir el resultado: " + e.getMessage());
            return Resultado.DESCONOCIDO;
        } finally {
            cliente.finalizarSolicitud(idSolicitud);
        }
    }

    private void listarDisponibles() {
        ClienteKafkaOperario cliente = kafka;
        if (cliente == null) {
            registrar("Kafka no está conectado.");
            return;
        }

        final String idSolicitud;
        try {
            idSolicitud = cliente.solicitarListadoWsDisponibles();
            registrar("Consultando estaciones disponibles... Request ID: " + idSolicitud);
        } catch (Exception e) {
            registrar("No se pudo solicitar el listado a CENTRAL: " + e.getMessage());
            return;
        }

        try {
            long limite = System.nanoTime() + Duration.ofSeconds(TIMEOUT_LISTADO_SEGUNDOS).toNanos();
            while (System.nanoTime() < limite) {
                long restante = limite - System.nanoTime();
                Map<String, Object> evento = cliente.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                if (evento == null) continue;

                if ("AVAILABLE_WS_LIST".equalsIgnoreCase(texto(evento.get("type")))
                        || evento.get("stations") instanceof List<?>) {
                    final List<Map<String, Object>> estaciones = aEstaciones(evento.get("stations"));
                    SwingUtilities.invokeLater(() -> mostrarEstaciones(estaciones));
                    return;
                }
                registrar(describir(evento));
                if (esFracaso(texto(evento.get("status")))) {
                    registrar("CENTRAL no ha podido proporcionar el listado.");
                    return;
                }
            }
            registrar("CENTRAL no respondió al listado en " + TIMEOUT_LISTADO_SEGUNDOS + " segundos.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            registrar("Consulta de WS interrumpida.");
        } catch (Exception e) {
            registrar("No se pudo recibir el listado: " + e.getMessage());
        } finally {
            cliente.finalizarSolicitud(idSolicitud);
        }
    }

    private void mostrarEstaciones(List<Map<String, Object>> estaciones) {
        modeloEstaciones.setRowCount(0);
        for (Map<String, Object> estacion : estaciones) {
            String estado = texto(estacion.get("state"));
            Object valorAvailable = estacion.get("available");
            boolean disponible = Boolean.TRUE.equals(valorAvailable)
                    || "ACTIVADA".equalsIgnoreCase(estado) || "AVAILABLE".equalsIgnoreCase(estado);
            if (valorAvailable != null && !Boolean.TRUE.equals(valorAvailable)) continue;
            if (valorAvailable == null && estado != null && !disponible) continue;

            String id = texto(estacion.containsKey("wsId") ? estacion.get("wsId") : estacion.get("id"));
            String ubicacion = texto(estacion.get("location"));
            modeloEstaciones.addRow(new Object[]{id == null ? "?" : id,
                    ubicacion == null ? "Sin ubicación" : ubicacion,
                    estado == null ? "ACTIVADA" : estado});
            registrar("WS disponible: " + id + " | " + ubicacion + " | " + estado);
        }
        if (modeloEstaciones.getRowCount() == 0) {
            registrar("No hay estaciones disponibles en este momento.");
        }
    }

    private void elegirFichero() {
        JFileChooser selector = new JFileChooser();
        if (!campoFichero.getText().trim().isEmpty()) {
            selector.setSelectedFile(Paths.get(campoFichero.getText().trim()).toFile());
        }
        if (selector.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            campoFichero.setText(selector.getSelectedFile().getAbsolutePath());
        }
    }

    // ------------------------------------------------------------------ Apoyo

    private boolean listoParaOperar() {
        if (!conectado || kafka == null) {
            JOptionPane.showMessageDialog(this, "Kafka no está conectado. Pulsa 'Reconectar'.",
                    "WM_FO", JOptionPane.WARNING_MESSAGE);
            return false;
        }
        if (ocupado) return false;
        return true;
    }

    private void comenzarOperacion() {
        ocupado = true;
        actualizarControles();
    }

    private void terminarOperacion() {
        ocupado = false;
        actualizarControles();
    }

    private void registrar(String mensaje) {
        final String linea = "[" + LocalTime.now().withNano(0) + "] " + mensaje;
        SwingUtilities.invokeLater(() -> {
            areaRegistro.append(linea + "\n");
            areaRegistro.setCaretPosition(areaRegistro.getDocument().getLength());
        });
    }

    private static String describir(Map<String, Object> evento) {
        StringBuilder linea = new StringBuilder("[CENTRAL] ");
        String estado = texto(evento.get("status"));
        linea.append(estado == null || estado.isEmpty() ? "Evento recibido" : estado);
        String mensaje = texto(evento.get("message"));
        if (mensaje != null && !mensaje.isEmpty()) linea.append(" - ").append(mensaje);

        Object caudal = evento.get("flowLMin");
        Object volumen = evento.get("volumeLitres");
        Object volumenServicio = evento.get("serviceVolumeLitres");
        Object transcurrido = evento.get("elapsedSeconds");
        if (caudal != null || volumen != null || volumenServicio != null || transcurrido != null) {
            linea.append(" | Telemetría");
            if (caudal != null) linea.append(": caudal ").append(decimal(caudal)).append(" L/min");
            if (volumen != null) linea.append(", acumulado ").append(decimal(volumen)).append(" L");
            if (volumenServicio != null) linea.append(", servicio ").append(decimal(volumenServicio)).append(" L");
            if (transcurrido != null) linea.append(", tiempo ").append(texto(transcurrido)).append(" s");
        }
        return linea.toString();
    }

    private List<Servicio> leerServicios(Path ruta) throws IOException {
        List<Servicio> servicios = new ArrayList<>();
        int numeroLinea = 0;
        try (BufferedReader lector = Files.newBufferedReader(ruta, StandardCharsets.UTF_8)) {
            String linea;
            while ((linea = lector.readLine()) != null) {
                numeroLinea++;
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) continue;

                String[] campos = contenido.split(";", -1);
                if (campos.length != 2) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": se esperaba el formato wsId;duracionSegundos.");
                }
                int idWs;
                int duracion;
                try {
                    idWs = Integer.parseInt(campos[0].trim());
                    duracion = Integer.parseInt(campos[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": el ID de WS y la duración deben ser números enteros.");
                }
                if (idWs <= 0) {
                    throw new IllegalArgumentException("Línea " + numeroLinea + ": el ID de WS debe ser positivo.");
                }
                if (duracion <= 0 || duracion > 86400) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": la duración debe estar entre 1 y 86400 segundos.");
                }
                servicios.add(new Servicio(idWs, duracion, numeroLinea));
            }
        }
        return servicios;
    }

    private static long segundosTimeoutServicio(int duracionSegundos) {
        long porDefecto = Math.max(TIMEOUT_MINIMO_SERVICIO_SEGUNDOS,
                (long) duracionSegundos + SEGUNDOS_EXTRA_SERVICIO);
        String configurado = System.getenv("WM_FO_TIMEOUT_SECONDS");
        if (configurado == null || configurado.trim().isEmpty()) return porDefecto;
        try {
            long valor = Long.parseLong(configurado.trim());
            return valor > 0 ? valor : porDefecto;
        } catch (NumberFormatException e) {
            return porDefecto;
        }
    }

    private static String formatearBootstrap(String anfitrion, int puerto) {
        if (anfitrion.contains(":") && !anfitrion.startsWith("[")) return "[" + anfitrion + "]:" + puerto;
        return anfitrion + ":" + puerto;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> aEstaciones(Object valor) {
        List<Map<String, Object>> estaciones = new ArrayList<>();
        if (!(valor instanceof List<?>)) return estaciones;
        for (Object elemento : (List<?>) valor) {
            if (!(elemento instanceof Map<?, ?>)) continue;
            Map<String, Object> estacion = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entrada : ((Map<?, ?>) elemento).entrySet()) {
                estacion.put(String.valueOf(entrada.getKey()), entrada.getValue());
            }
            estaciones.add(estacion);
        }
        return estaciones;
    }

    private static boolean esExito(String estado) {
        String valor = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return valor.equals("COMPLETED") || valor.equals("COMPLETADO")
                || valor.equals("FINALIZADO") || valor.equals("SUCCESS")
                || valor.equals("EXITO") || valor.equals("ÉXITO");
    }

    private static boolean esFracaso(String estado) {
        String valor = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return valor.equals("REJECTED") || valor.equals("RECHAZADO")
                || valor.equals("FAILED") || valor.equals("FALLO")
                || valor.equals("ERROR") || valor.equals("CANCELLED")
                || valor.equals("CANCELADO") || valor.equals("FRACASO")
                || valor.equals("DENEGADO") || valor.equals("RECHAZADA");
    }

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static String decimal(Object valor) {
        try {
            double numero = valor instanceof Number
                    ? ((Number) valor).doubleValue()
                    : Double.parseDouble(String.valueOf(valor));
            return String.format(Locale.ROOT, "%.2f", numero);
        } catch (Exception e) {
            return String.valueOf(valor);
        }
    }

    private enum Resultado { EXITO, FRACASO, DESCONOCIDO }

    private static final class Servicio {
        final int idWs;
        final int duracionSegundos;
        final int linea;

        Servicio(int idWs, int duracionSegundos, int linea) {
            this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
            this.linea = linea;
        }
    }
}

```


## WM_FO/src/UiSupport.java

```java
import java.awt.GraphicsEnvironment;
import java.util.Locale;

/** Decides whether a graphical interface can be used in this environment. */
final class UiSupport {

    private UiSupport() { }

    static boolean guiEnabled() {
        String configured = System.getenv("WM_GUI");
        if (configured != null) {
            String value = configured.trim().toLowerCase(Locale.ROOT);
            if (value.equals("false") || value.equals("0") || value.equals("no") || value.equals("off")) {
                return false;
            }
        }
        try {
            return !GraphicsEnvironment.isHeadless();
        } catch (Throwable t) {
            return false;
        }
    }
}

```


## WM_FO/src/WM_FO.java

```java

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import javax.swing.SwingUtilities;

/**
 * Aplicación de operario de campo (WM_FO).
 *
 * Requisitos cubiertos:
 * - argumentos: IP del broker, puerto e ID del operario;
 * - solicitud de riego manual para una WS y una duración;
 * - ejecución secuencial de solicitudes desde un fichero;
 * - espera de una conclusión real publicada por WM_Central;
 * - pausa de 4 segundos entre servicios del fichero;
 * - consulta de WS disponibles;
 * - recuperación de servicios pendientes tras cerrarse durante un riego;
 * - correlación de respuestas, timeout y cierre controlado.
 *
 * El contrato de mensajes Kafka se documenta en ../README.md y debe ser respetado
 * por WM_Central para que la integración distribuida funcione.
 */
public final class WM_FO implements AutoCloseable {

    private static final int DURACION_MAXIMA_SEGUNDOS = 86400;
    private static final long TIMEOUT_LISTADO_SEGUNDOS = 15;
    private static final long TIMEOUT_MINIMO_SERVICIO_SEGUNDOS = 120;
    private static final long SEGUNDOS_EXTRA_SERVICIO = 120;
    private static final long PAUSA_ENTRE_SERVICIOS_MS = 4000;
    private static final long TIMEOUT_RECUPERACION_SEGUNDOS = 30;

    private final String ipBroker;
    private final int puertoBroker;
    private final int idOperario;
    private final Scanner teclado;

    private ClienteKafkaOperario kafka;
    private Boolean ultimoEstadoCentral;

    private WM_FO(String ipBroker, int puertoBroker, int idOperario) {
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.idOperario = idOperario;
        this.teclado = new Scanner(System.in);
        crearClienteKafka();
    }

    private void crearClienteKafka() {
        String servidoresKafka = formatearBootstrap(ipBroker, puertoBroker);
        kafka = new ClienteKafkaOperario(servidoresKafka, idOperario);
    }

    private static String formatearBootstrap(String anfitrion, int puerto) {
        // Kafka espera host:puerto. Los IPv6 literales deben ir entre corchetes.
        if (anfitrion.contains(":") && !anfitrion.startsWith("[")) {
            return "[" + anfitrion + "]:" + puerto;
        }
        return anfitrion + ":" + puerto;
    }

    public void iniciar() {
        imprimirCabecera();
        if (!conectarKafkaConReintentos()) {
            return;
        }
        recuperarServiciosPendientes();

        boolean salir = false;
        while (!salir) {
            mostrarMenu();
            String opcion = teclado.nextLine().trim();
            switch (opcion) {
                case "1":
                    solicitarServicioManual();
                    break;
                case "2":
                    solicitarServiciosDesdeFichero();
                    break;
                case "3":
                    mostrarWsDisponibles();
                    break;
                case "4":
                    salir = true;
                    System.out.println("Saliendo de WM_FO...");
                    break;
                default:
                    System.out.println("Opción no válida. Selecciona 1, 2, 3 o 4.");
            }
        }
    }

    private void imprimirCabecera() {
        System.out.println("========================================");
        System.out.println("           WM_FO - OPERARIO");
        System.out.println("========================================");
        System.out.println("ID Operario: " + idOperario);
        System.out.println("Broker Kafka: " + formatearBootstrap(ipBroker, puertoBroker));
    }

    private boolean conectarKafkaConReintentos() {
        while (true) {
            try {
                System.out.println("Conectando con Kafka...");
                kafka.iniciar();
                return true;
            } catch (Exception e) {
                System.err.println("No se pudo iniciar la comunicación Kafka: " + e.getMessage());
                System.out.print("Escribe R para reintentar o S para salir: ");
                String opcion = teclado.nextLine().trim();
                if (opcion.equalsIgnoreCase("s")) {
                    return false;
                }

                try {
                    kafka.cerrar();
                } catch (Exception ignorada) {
                    // No se permite que un error de limpieza oculte el fallo original.
                }
                crearClienteKafka();
            }
        }
    }

    /** Recupera el estado de los riegos que quedaron pendientes de una ejecución anterior. */
    private void recuperarServiciosPendientes() {
        List<ClienteKafkaOperario.Pendiente> pendientes = kafka.cargarPendientes();
        if (pendientes.isEmpty()) {
            return;
        }
        System.out.println("Se han encontrado " + pendientes.size()
                + " servicios pendientes de sesiones anteriores. Consultando su estado...");
        for (ClienteKafkaOperario.Pendiente pendiente : pendientes) {
            try {
                kafka.solicitarEstado(pendiente.idSolicitud);
            } catch (Exception e) {
                System.err.println("No se pudo preguntar por " + pendiente.idSolicitud + ": " + e.getMessage());
                continue;
            }
            try {
                long limite = System.nanoTime()
                        + Duration.ofSeconds(TIMEOUT_RECUPERACION_SEGUNDOS).toNanos();
                boolean resuelto = false;
                while (System.nanoTime() < limite) {
                    long restante = limite - System.nanoTime();
                    Map<String, Object> evento = kafka.esperarEvento(pendiente.idSolicitud,
                            Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                    if (evento == null) {
                        continue;
                    }
                    System.out.print("[RECUPERADO] ");
                    mostrarNotificacion(evento);
                    String estado = texto(evento.get("status"));
                    if (esEstadoExito(estado) || esEstadoFracaso(estado)) {
                        kafka.resolverPendiente(pendiente.idSolicitud);
                        resuelto = true;
                        break;
                    }
                }
                if (!resuelto) {
                    System.err.println("No se conoce aún la conclusión de " + pendiente.idSolicitud
                            + "; seguirá pendiente para la próxima ejecución.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Recuperación de " + pendiente.idSolicitud + " interrumpida.");
            } catch (IOException | IllegalStateException e) {
                System.err.println("No se pudo recuperar " + pendiente.idSolicitud + ": " + e.getMessage());
            } finally {
                kafka.finalizarSolicitud(pendiente.idSolicitud);
            }
        }
    }

    private void mostrarMenu() {
        avisarCentral();
        System.out.println();
        System.out.println("--------------- MENÚ ---------------");
        System.out.println("1. Solicitar suministro manualmente");
        System.out.println("2. Procesar servicios desde fichero");
        System.out.println("3. Consultar WS disponibles");
        System.out.println("4. Salir");
        System.out.print("Selecciona una opción: ");
    }

    /** Avisa al operario cuando WM_Central deja de publicar latidos o se recupera. */
    private void avisarCentral() {
        if (kafka == null) return;
        boolean vivo = kafka.centralViva();
        if (ultimoEstadoCentral == null) {
            ultimoEstadoCentral = vivo;
            if (!vivo) {
                System.err.println("[CENTRAL] Todavía no se reciben latidos de WM_Central. "
                        + "Si la central no está desplegada, no se podrán atender solicitudes.");
            }
            return;
        }
        if (vivo != ultimoEstadoCentral.booleanValue()) {
            ultimoEstadoCentral = vivo;
            if (vivo) System.out.println("[CENTRAL] Conexión con WM_Central restablecida.");
            else System.err.println("Imposible conectar con WM_Central: no se reciben latidos de la central.");
        }
    }

    private void solicitarServicioManual() {
        System.out.println();
        System.out.println("===== SOLICITUD DE RIEGO =====");
        int idWs = leerEnteroPositivo("ID de la estación (WS): ", Integer.MAX_VALUE);
        int duracion = leerEnteroPositivo("Duración del riego en segundos (1-86400): ", DURACION_MAXIMA_SEGUNDOS);
        ejecutarServicio(idWs, duracion, "Solicitud manual");
    }

    private void solicitarServiciosDesdeFichero() {
        System.out.print("Ruta del fichero de servicios: ");
        String nombreFichero = teclado.nextLine().trim();
        if (nombreFichero.isEmpty()) {
            System.out.println("No se ha indicado una ruta.");
            return;
        }

        final List<Servicio> servicios;
        try {
            servicios = leerServicios(Paths.get(nombreFichero));
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("No se ha podido cargar el fichero: " + e.getMessage());
            System.out.println("No se ha enviado ninguna solicitud de este fichero.");
            return;
        }

        if (servicios.isEmpty()) {
            System.out.println("El fichero no contiene servicios.");
            return;
        }

        System.out.println("Se han validado " + servicios.size() + " servicios. Comienza el envío secuencial.");
        for (int i = 0; i < servicios.size(); i++) {
            Servicio servicio = servicios.get(i);
            System.out.println();
            System.out.println("========== SERVICIO " + (i + 1) + "/" + servicios.size() + " ==========");
            System.out.println("Fichero, línea " + servicio.linea + ": WS=" + servicio.idWs
                    + ", duración=" + servicio.duracionSegundos + " s");

            Resultado resultado = ejecutarServicio(servicio.idWs, servicio.duracionSegundos,
                    "Servicio de fichero, línea " + servicio.linea);

            if (resultado == Resultado.DESCONOCIDO) {
                System.err.println("Se detiene el procesamiento del fichero porque no se conoce la conclusión "
                        + "del servicio. No se enviarán más solicitudes para evitar solapamientos.");
                return;
            }

            if (i < servicios.size() - 1) {
                System.out.println("Esperando 4 segundos antes del siguiente servicio...");
                try {
                    Thread.sleep(PAUSA_ENTRE_SERVICIOS_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Procesamiento del fichero interrumpido; no se enviarán más servicios.");
                    return;
                }
            }
        }
        System.out.println("Se ha terminado el procesamiento del fichero.");
    }

    /**
     * Formato acordado para el fichero: wsId;duracionSegundos
     * Se admiten líneas vacías y comentarios que comiencen por #.
     * Se valida el fichero completo antes de enviar la primera petición.
     */
    private List<Servicio> leerServicios(Path ruta) throws IOException {
        List<Servicio> servicios = new ArrayList<>();
        int numeroLinea = 0;

        try (BufferedReader lector = Files.newBufferedReader(ruta, StandardCharsets.UTF_8)) {
            String linea;
            while ((linea = lector.readLine()) != null) {
                numeroLinea++;
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) {
                    continue;
                }

                String[] campos = contenido.split(";", -1);
                if (campos.length != 2) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": se esperaba el formato wsId;duracionSegundos.");
                }

                int idWs;
                int duracion;
                try {
                    idWs = Integer.parseInt(campos[0].trim());
                    duracion = Integer.parseInt(campos[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": el ID de WS y la duración deben ser números enteros.");
                }

                if (idWs <= 0) {
                    throw new IllegalArgumentException("Línea " + numeroLinea + ": el ID de WS debe ser positivo.");
                }
                if (duracion <= 0 || duracion > DURACION_MAXIMA_SEGUNDOS) {
                    throw new IllegalArgumentException("Línea " + numeroLinea
                            + ": la duración debe estar entre 1 y " + DURACION_MAXIMA_SEGUNDOS + " segundos.");
                }
                servicios.add(new Servicio(idWs, duracion, numeroLinea));
            }
        }
        return servicios;
    }

    private Resultado ejecutarServicio(int idWs, int duracionSegundos, String origen) {
        String idSolicitud;
        try {
            System.out.println(origen + ". Operario=" + idOperario + ", WS=" + idWs
                    + ", duración=" + duracionSegundos + " s");
            idSolicitud = kafka.solicitarRiego(idWs, duracionSegundos);
        } catch (Exception e) {
            System.err.println("No se pudo publicar la solicitud: " + e.getMessage());
            System.err.println("No se puede confirmar si CENTRAL recibió la petición. Comprueba el estado "
                    + "del sistema antes de volver a enviar un riego similar.");
            return Resultado.DESCONOCIDO;
        }

        try {
            long timeout = obtenerTimeoutServicio(duracionSegundos);
            System.out.println("Esperando la conclusión de CENTRAL. Request ID: " + idSolicitud);
            long limite = System.nanoTime() + Duration.ofSeconds(timeout).toNanos();

            while (true) {
                long restante = limite - System.nanoTime();
                if (restante <= 0) {
                    System.err.println("Timeout: han pasado " + timeout
                            + " segundos sin recibir una conclusión del servicio.");
                    System.err.println("Esto no cancela el riego en CENTRAL/WS; el resultado es desconocido.");
                    avisarCentral();
                    return Resultado.DESCONOCIDO;
                }

                Map<String, Object> evento = kafka.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                avisarCentral();
                if (evento == null) {
                    continue;
                }

                mostrarNotificacion(evento);
                String estado = texto(evento.get("status"));
                if (esEstadoExito(estado)) {
                    System.out.println("Riego finalizado correctamente.");
                    kafka.resolverPendiente(idSolicitud);
                    return Resultado.EXITO;
                }
                if (esEstadoFracaso(estado)) {
                    System.out.println("El servicio ha concluido sin éxito (" + estado + ").");
                    kafka.resolverPendiente(idSolicitud);
                    return Resultado.FRACASO;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Espera interrumpida. No se conoce el resultado final del servicio.");
            return Resultado.DESCONOCIDO;
        } catch (IOException | IllegalStateException e) {
            System.err.println("Se ha perdido la capacidad de recibir el resultado: " + e.getMessage());
            return Resultado.DESCONOCIDO;
        } finally {
            kafka.finalizarSolicitud(idSolicitud);
        }
    }

    private long obtenerTimeoutServicio(int duracionSegundos) {
        long porDefecto = Math.max(TIMEOUT_MINIMO_SERVICIO_SEGUNDOS,
                (long) duracionSegundos + SEGUNDOS_EXTRA_SERVICIO);
        String variable = System.getenv("WM_FO_TIMEOUT_SECONDS");
        if (variable == null || variable.trim().isEmpty()) {
            return porDefecto;
        }
        try {
            long configurado = Long.parseLong(variable.trim());
            if (configurado <= 0) {
                System.err.println("WM_FO_TIMEOUT_SECONDS debe ser positivo; se usará " + porDefecto + " s.");
                return porDefecto;
            }
            return configurado;
        } catch (NumberFormatException e) {
            System.err.println("WM_FO_TIMEOUT_SECONDS no es válido; se usará " + porDefecto + " s.");
            return porDefecto;
        }
    }

    private void mostrarNotificacion(Map<String, Object> evento) {
        String estado = texto(evento.get("status"));
        String mensaje = texto(evento.get("message"));
        if (estado != null && !estado.isEmpty()) {
            System.out.print("[CENTRAL] " + estado);
        } else {
            System.out.print("[CENTRAL] Evento recibido");
        }
        if (mensaje != null && !mensaje.isEmpty()) {
            System.out.print(" — " + mensaje);
        }
        System.out.println();

        Object caudal = evento.get("flowLMin");
        Object volumen = evento.get("volumeLitres");
        Object consumoServicio = evento.get("serviceVolumeLitres");
        Object duracion = evento.get("elapsedSeconds");
        if (caudal != null || volumen != null || consumoServicio != null || duracion != null) {
            System.out.print("  Telemetría");
            if (caudal != null) {
                System.out.print(" | Caudal: " + decimal(caudal) + " L/min");
            }
            if (volumen != null) {
                System.out.print(" | Volumen acumulado: " + decimal(volumen) + " L");
            }
            if (consumoServicio != null) {
                System.out.print(" | Consumo del servicio: " + decimal(consumoServicio) + " L");
            }
            if (duracion != null) {
                System.out.print(" | Tiempo: " + texto(duracion) + " s");
            }
            System.out.println();
        }
    }

    private void mostrarWsDisponibles() {
        String idSolicitud;
        try {
            idSolicitud = kafka.solicitarListadoWsDisponibles();
        } catch (Exception e) {
            System.err.println("No se pudo solicitar el listado a CENTRAL: " + e.getMessage());
            return;
        }

        try {
            System.out.println("Consultando las estaciones disponibles...");
            long limite = System.nanoTime() + Duration.ofSeconds(TIMEOUT_LISTADO_SEGUNDOS).toNanos();
            while (System.nanoTime() < limite) {
                long restante = limite - System.nanoTime();
                Map<String, Object> evento = kafka.esperarEvento(idSolicitud,
                        Duration.ofNanos(Math.min(restante, Duration.ofSeconds(1).toNanos())));
                avisarCentral();
                if (evento == null) {
                    continue;
                }

                String tipo = texto(evento.get("type"));
                if ("AVAILABLE_WS_LIST".equalsIgnoreCase(tipo)
                        || evento.get("stations") instanceof List<?>) {
                    mostrarListaWs(evento);
                    return;
                }
                mostrarNotificacion(evento);
                String estado = texto(evento.get("status"));
                if (esEstadoFracaso(estado)) {
                    System.out.println("CENTRAL no ha podido proporcionar el listado.");
                    return;
                }
            }
            System.err.println("CENTRAL no respondió al listado en " + TIMEOUT_LISTADO_SEGUNDOS + " segundos.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Consulta de WS interrumpida.");
        } catch (IOException | IllegalStateException e) {
            System.err.println("No se pudo recibir el listado: " + e.getMessage());
        } finally {
            kafka.finalizarSolicitud(idSolicitud);
        }
    }

    private void mostrarListaWs(Map<String, Object> evento) {
        Object objetoLista = evento.get("stations");
        if (!(objetoLista instanceof List<?>)) {
            System.out.println("CENTRAL respondió sin una lista de estaciones.");
            return;
        }

        List<?> estaciones = (List<?>) objetoLista;
        int mostradas = 0;
        System.out.println();
        System.out.println("========== WS DISPONIBLES ==========");
        System.out.printf("%-8s %-32s %-18s%n", "ID WS", "Ubicación", "Estado");
        System.out.println("------------------------------------------------------------");

        for (Object elemento : estaciones) {
            if (!(elemento instanceof Map<?, ?>)) {
                continue;
            }
            Map<?, ?> estacion = (Map<?, ?>) elemento;
            String estado = texto(estacion.get("state"));
            Object valorAvailable = estacion.get("available");
            boolean disponible = Boolean.TRUE.equals(valorAvailable)
                    || "ACTIVADA".equalsIgnoreCase(estado)
                    || "AVAILABLE".equalsIgnoreCase(estado);
            // La respuesta de esta consulta debe incluir únicamente estaciones disponibles.
            if (valorAvailable != null && !Boolean.TRUE.equals(valorAvailable)) {
                continue;
            }
            if (valorAvailable == null && estado != null && !disponible) {
                continue;
            }

            String id = texto(estacion.containsKey("wsId") ? estacion.get("wsId") : estacion.get("id"));
            String ubicacion = texto(estacion.get("location"));
            if (id == null) id = "?";
            if (ubicacion == null) ubicacion = "Sin ubicación";
            if (estado == null) estado = "ACTIVADA";
            System.out.printf(Locale.ROOT, "%-8s %-32s %-18s%n", acortar(id, 8), acortar(ubicacion, 32), estado);
            mostradas++;
        }

        if (mostradas == 0) {
            System.out.println("No hay estaciones disponibles en este momento.");
        }
        System.out.println("====================================");
    }

    private int leerEnteroPositivo(String mensaje, int maximo) {
        while (true) {
            System.out.print(mensaje);
            String entrada = teclado.nextLine().trim();
            try {
                int valor = Integer.parseInt(entrada);
                if (valor > 0 && valor <= maximo) {
                    return valor;
                }
            } catch (NumberFormatException ignorada) {
                // Se vuelve a solicitar el dato sin terminar la aplicación.
            }
            System.out.println("Introduce un número entero entre 1 y " + maximo + ".");
        }
    }

    private static boolean esEstadoExito(String estado) {
        String normalizado = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return normalizado.equals("COMPLETED") || normalizado.equals("COMPLETADO")
                || normalizado.equals("FINALIZADO") || normalizado.equals("SUCCESS")
                || normalizado.equals("EXITO") || normalizado.equals("ÉXITO");
    }

    private static boolean esEstadoFracaso(String estado) {
        String normalizado = estado == null ? "" : estado.trim().toUpperCase(Locale.ROOT);
        return normalizado.equals("REJECTED") || normalizado.equals("RECHAZADO")
                || normalizado.equals("FAILED") || normalizado.equals("FALLO")
                || normalizado.equals("ERROR") || normalizado.equals("CANCELLED")
                || normalizado.equals("CANCELADO") || normalizado.equals("FRACASO")
                || normalizado.equals("DENEGADO") || normalizado.equals("RECHAZADA");
    }

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static String decimal(Object valor) {
        try {
            double numero = valor instanceof Number
                    ? ((Number) valor).doubleValue()
                    : Double.parseDouble(String.valueOf(valor));
            return String.format(Locale.ROOT, "%.2f", numero);
        } catch (Exception e) {
            return String.valueOf(valor);
        }
    }

    private static String acortar(String texto, int maximo) {
        if (texto.length() <= maximo) return texto;
        return texto.substring(0, Math.max(0, maximo - 3)) + "...";
    }

    @Override
    public void close() {
        if (kafka != null) {
            kafka.cerrar();
        }
    }

    public static void main(String[] args) {
        String ipBroker;
        int puertoBroker;
        int idOperario;
        try {
            ipBroker = (args.length >= 1 ? args[0] : env("WM_KAFKA_HOST", "localhost")).trim();
            puertoBroker = Integer.parseInt((args.length >= 2 ? args[1] : env("WM_KAFKA_PORT", "9092")).trim());
            idOperario = Integer.parseInt((args.length >= 3 ? args[2] : env("WM_FO_ID", "1")).trim());
        } catch (NumberFormatException e) {
            System.err.println("El puerto del broker y el ID del operario deben ser números enteros.");
            System.err.println("Uso: java WM_FO [IP_BROKER] [PUERTO_BROKER] [ID_OPERARIO]");
            System.err.println("También se admiten WM_KAFKA_HOST, WM_KAFKA_PORT y WM_FO_ID.");
            return;
        }

        if (ipBroker.isEmpty() || puertoBroker < 1 || puertoBroker > 65535 || idOperario <= 0) {
            System.err.println("Argumentos inválidos. La IP no puede estar vacía, el puerto debe estar entre 1 y 65535 y el ID debe ser positivo.");
            return;
        }

        final String anfitrionBroker = ipBroker;
        final int puertoBrokerFinal = puertoBroker;
        final int operario = idOperario;
        if (UiSupport.guiEnabled()) {
            final boolean[] mostrada = {false};
            try {
                SwingUtilities.invokeAndWait(() -> {
                    FOFrame ventana = new FOFrame(anfitrionBroker, puertoBrokerFinal, operario);
                    ventana.setVisible(true);
                    mostrada[0] = true;
                });
            } catch (Throwable t) {
                System.err.println("No se pudo iniciar la interfaz gráfica de WM_FO: "
                        + t.getMessage() + ". Se usará la consola.");
            }
            if (mostrada[0]) return;
        }

        try (WM_FO operador = new WM_FO(ipBroker, puertoBroker, idOperario)) {
            operador.iniciar();
        } catch (Exception e) {
            System.err.println("WM_FO se ha cerrado por un error: " + e.getMessage());
        }
    }

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private enum Resultado {
        EXITO,
        FRACASO,
        DESCONOCIDO
    }

    private static final class Servicio {
        private final int idWs;
        private final int duracionSegundos;
        private final int linea;

        private Servicio(int idWs, int duracionSegundos, int linea) {
            this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
            this.linea = linea;
        }
    }
}

```


## WM_WS/src/UiSupport.java

```java
import java.awt.GraphicsEnvironment;
import java.util.Locale;

/** Decides whether a graphical interface can be used in this environment. */
final class UiSupport {

    private UiSupport() { }

    static boolean guiEnabled() {
        String configured = System.getenv("WM_GUI");
        if (configured != null) {
            String value = configured.trim().toLowerCase(Locale.ROOT);
            if (value.equals("false") || value.equals("0") || value.equals("no") || value.equals("off")) {
                return false;
            }
        }
        try {
            return !GraphicsEnvironment.isHeadless();
        } catch (Throwable t) {
            return false;
        }
    }
}

```


## WM_WS/src/WireProtocol.java

```java
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Protocolo de tramas compartido por WM_Central, WM_WS_M y WM_WS_E.
 *
 * Formato de trama: STX + DATA (UTF-8) + ETX + LRC.
 * LRC = XOR de todos los bytes de DATA.
 * Los bytes de control ENQ/ACK/NACK/EOT viajan sueltos (no forman trama).
 *
 * Al recibir una trama se responde con ACK si el LRC es correcto o NACK si no
 * lo es, lo que permite al emisor retransmitirla.
 */
public final class WireProtocol {
    public static final int ENQ = 0x05;
    public static final int ACK = 0x06;
    public static final int NACK = 0x15;
    public static final int STX = 0x02;
    public static final int ETX = 0x03;
    public static final int EOT = 0x04;

    private WireProtocol() { }

    public static void enviarControl(OutputStream out, int control) throws IOException {
        if (control != ENQ && control != ACK && control != NACK && control != EOT) {
            throw new IllegalArgumentException("Byte de control no válido: " + control);
        }
        synchronized (out) {
            out.write(control);
            out.flush();
        }
    }

    public static void enviarTrama(OutputStream out, String data) throws IOException {
        if (data == null || data.indexOf((char) STX) >= 0 || data.indexOf((char) ETX) >= 0) {
            throw new IllegalArgumentException("Mensaje nulo o con caracteres STX/ETX reservados");
        }
        byte[] payload = data.getBytes(StandardCharsets.UTF_8);
        int lrc = xor(payload);
        synchronized (out) {
            out.write(STX);
            out.write(payload);
            out.write(ETX);
            out.write(lrc);
            out.flush();
        }
    }

    /** Lee una trama completa; el primer byte (STX) todavía está por leer. */
    public static String leerTrama(InputStream in) throws IOException {
        int first = in.read();
        if (first < 0) throw new EOFException("Socket cerrado mientras se esperaba STX");
        if (first != STX) throw new IOException("Se esperaba STX; recibido " + first);
        return leerTramaTrasStx(in);
    }

    /** Lee DATA, ETX y LRC; el llamador ya ha consumido STX. */
    public static String leerTramaTrasStx(InputStream in) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int value;
        while ((value = in.read()) != ETX) {
            if (value < 0) throw new EOFException("Socket cerrado antes de ETX");
            data.write(value);
            if (data.size() > 1024 * 1024) throw new IOException("Trama mayor que 1 MiB; se rechaza");
        }
        int receivedLrc = in.read();
        if (receivedLrc < 0) throw new EOFException("Socket cerrado antes de LRC");
        byte[] payload = data.toByteArray();
        int calculated = xor(payload);
        if ((receivedLrc & 0xFF) != calculated) {
            throw new BadLrcException("LRC incorrecto: recibido=" + receivedLrc + ", calculado=" + calculated);
        }
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static int xor(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) value ^= (b & 0xFF);
        return value & 0xFF;
    }

    public static final class BadLrcException extends IOException {
        public BadLrcException(String message) { super(message); }
    }
}

```


## WM_WS/src/WM_WS_E.java

```java
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;
import javax.swing.SwingUtilities;

/**
 * WM_WS_E - Motor (engine) de una estación WS.
 *
 * Se conecta al monitor WM_WS_M, que le entrega su identidad. A partir de ahí:
 *  - Responde cada segundo a la comprobación de salud (POLL) con su estado y telemetría.
 *  - Ejecuta las órdenes que le llegan del monitor (que las recibe de WM_Central o del
 *    menú local): iniciar/parar riego, bloquear y activar.
 *  - Acumula el agua suministrada y la persiste en disco para no perderla nunca.
 *  - Permite simular una avería (KO) o un motor que no responde con una tecla, para
 *    demostrar la resiliencia del sistema.
 *
 * Parámetros (argumentos o variables de entorno):
 *   ip-monitor    WM_MONITOR_HOST
 *   puerto-monitor WM_MONITOR_PORT
 *   [dir-estado]  WM_WS_STATE_DIR (por defecto "data")
 */
public final class WM_WS_E {

    private static final long TIC_MS = 100L;
    private static final long MONITOR_OBSOLETO_MS = 5_000L;
    private static final long RECONEXION_MS = 2_000L;

    private final String ipMonitor;
    private final int puertoMonitor;
    private final String directorioEstado;

    private final Object lock = new Object();
    private volatile boolean ejecutando = true;

    private int idWs = -1;
    private String ubicacion = "";
    private double caudalNominal = 0;

    private volatile boolean monitorConectado;
    private volatile long ultimoPoll;
    private volatile boolean averia;
    private volatile boolean sinRespuesta;
    private volatile Socket socketActual;

    private boolean bloqueada;
    private Riego activo;
    private Pendiente pendiente;
    private double totalLitros;

    private boolean estadoCargado;
    private long ultimoGuardado;

    private WM_WS_E(String ipMonitor, int puertoMonitor, String directorioEstado) {
        this.ipMonitor = ipMonitor;
        this.puertoMonitor = puertoMonitor;
        this.directorioEstado = directorioEstado;
    }

    public static void main(String[] args) {
        String ip;
        int puerto;
        String dir;
        try {
            if (args.length >= 2) {
                ip = args[0].trim();
                puerto = Integer.parseInt(args[1].trim());
                dir = args.length >= 3 ? args[2].trim() : env("WM_WS_STATE_DIR", "data");
            } else {
                ip = env("WM_MONITOR_HOST", "localhost").trim();
                puerto = Integer.parseInt(env("WM_MONITOR_PORT", "6000").trim());
                dir = env("WM_WS_STATE_DIR", "data");
            }
        } catch (Exception e) {
            System.err.println("Uso: java WM_WS_E <ipMonitor> <puertoMonitor> [dirEstado]");
            System.err.println("También se admiten WM_MONITOR_HOST, WM_MONITOR_PORT y WM_WS_STATE_DIR.");
            System.exit(2);
            return;
        }
        if (ip.isEmpty() || puerto < 1 || puerto > 65535) {
            System.err.println("Parámetros del motor no válidos.");
            System.exit(2);
            return;
        }

        WM_WS_E motor = new WM_WS_E(ip, puerto, dir);
        Runtime.getRuntime().addShutdownHook(new Thread(motor::detener, "wm-ws-engine-shutdown"));

        boolean graphical = false;
        if (UiSupport.guiEnabled()) {
            try {
                SwingUtilities.invokeAndWait(() -> new WSEngineFrame(motor).setVisible(true));
                graphical = true;
            } catch (Throwable t) {
                System.err.println("No se pudo iniciar la interfaz gráfica del motor: "
                        + t.getMessage() + ". Se usará la consola.");
            }
        }
        motor.iniciar(!graphical);
    }

    private void iniciar(boolean conConsola) {
        System.out.println("=================================================");
        System.out.println(" WM_WS_E - MOTOR DE ESTACIÓN");
        System.out.println("=================================================");
        System.out.println("Monitor .........: " + ipMonitor + ":" + puertoMonitor);
        System.out.println("Estado en disco .: " + directorioEstado);
        System.out.println("Teclas: K=avería KO | S=motor sin respuesta | E=estado | Q=salir");
        System.out.println("=================================================");

        if (conConsola) iniciarConsola();
        iniciarReloj();

        while (ejecutando) {
            try {
                conectarYAtender();
            } catch (Exception e) {
                if (ejecutando) {
                    System.err.println("[MONITOR] Imposible conectar con el monitor: " + mensajeCorto(e));
                }
            } finally {
                perderMonitor("Monitor desconectado; la estación queda AVERIADA.");
            }
            if (ejecutando) dormir(RECONEXION_MS);
        }
        detener();
    }

    /** Abre la conexión con el monitor y procesa sus tramas hasta que se pierda. */
    private void conectarYAtender() throws IOException {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(ipMonitor, puertoMonitor), 3_000);
            s.setKeepAlive(true);
            s.setSoTimeout(2_500);
            InputStream in = new BufferedInputStream(s.getInputStream());
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            String identificacion = WireProtocol.leerTrama(in);
            if (identificacion == null || !identificacion.startsWith("ID#")) {
                throw new IOException("El monitor no envió la identificación esperada");
            }
            String[] datos = identificacion.split("#", -1);
            if (datos.length < 4) throw new IOException("Identificación incompleta del monitor");
            int idRecibido;
            try {
                idRecibido = Integer.parseInt(datos[1].trim());
                caudalNominal = Double.parseDouble(datos[3].trim().replace(',', '.'));
            } catch (Exception e) {
                throw new IOException("Identificación con valores no válidos");
            }
            idWs = idRecibido;
            ubicacion = URLDecoder.decode(datos[2], StandardCharsets.UTF_8.name());
            WireProtocol.enviarControl(out, WireProtocol.ACK);

            if (!estadoCargado) cargarEstado();
            synchronized (lock) {
                monitorConectado = true;
                ultimoPoll = System.currentTimeMillis();
            }
            socketActual = s;
            System.out.println("[MONITOR] Conectado. WS " + idWs + " (" + ubicacion + "), caudal "
                    + fmt(caudalNominal) + " L/min. Agua acumulada: " + fmt(totalLitros) + " L.");
            imprimirEstado();

            leerTramas(in, out);
        } finally {
            socketActual = null;
            try { s.close(); } catch (IOException ignored) { }
        }
    }

    private void leerTramas(InputStream in, OutputStream out) throws IOException {
        while (ejecutando) {
            String trama;
            try {
                trama = WireProtocol.leerTrama(in);
            } catch (SocketTimeoutException timeout) {
                // Sin tráfico en este intervalo; el reloj comprueba la salud del enlace.
                continue;
            }

            if (trama.startsWith("POLL#")) {
                String[] p = trama.split("#", -1);
                boolean centralConectada = p.length > 1 && "1".equals(p[1]);
                String ackCentral = p.length > 2 ? p[2].trim() : "";
                synchronized (lock) {
                    ultimoPoll = System.currentTimeMillis();
                    monitorConectado = true;
                    if (!ackCentral.isEmpty() && pendiente != null && ackCentral.equals(pendiente.idSolicitud)) {
                        System.out.println("[CENTRAL] Confirmado el estado final del servicio " + ackCentral + ".");
                        pendiente = null;
                        guardar();
                    }
                }
                if (sinRespuesta) continue; // motor "colgado": no envía estado
                if (averia) {
                    WireProtocol.enviarTrama(out, "KO");
                } else {
                    WireProtocol.enviarTrama(out, construirEstado());
                }
            } else if (trama.startsWith("CMD#")) {
                WireProtocol.enviarControl(out, WireProtocol.ACK);
                ejecutarComando(trama.substring(4));
            } else {
                WireProtocol.enviarControl(out, WireProtocol.NACK);
            }
        }
    }

    /** Texto de estado según el protocolo acordado con el monitor. */
    private String construirEstado() {
        synchronized (lock) {
            String estado;
            String req = "";
            int operario = 0;
            long segundos = 0;
            double caudal = 0;

            if (activo != null) {
                estado = "REGANDO";
                req = activo.idSolicitud;
                operario = activo.operario;
                segundos = activo.segundos();
                caudal = caudalNominal;
            } else {
                estado = bloqueada ? "FUERA_SERVICIO" : "ACTIVADA";
                if (pendiente != null) {
                    req = pendiente.idSolicitud;
                    operario = pendiente.operario;
                    segundos = pendiente.segundos;
                }
            }
            return "STATUS#" + estado + "#" + fmt(caudal) + "#" + fmt(totalLitros)
                    + "#" + segundos + "#" + (req == null ? "" : req) + "#" + operario;
        }
    }

    private void ejecutarComando(String comando) {
        String[] p = comando.split("\\|", -1);
        String tipo = p.length > 0 ? p[0].trim() : "";
        String idSolicitud = p.length > 1 ? p[1].trim() : "";
        int operario = p.length > 2 ? entero(p[2], 0) : 0;
        int duracion = p.length > 3 ? entero(p[3], 0) : 0;

        synchronized (lock) {
            switch (tipo) {
                case "START_IRRIGATION":
                    iniciarRiego(idSolicitud, operario, duracion);
                    break;
                case "STOP_IRRIGATION":
                    if (activo != null) {
                        System.out.println("[CENTRAL] Orden de parada recibida.");
                        finalizar("Parada solicitada");
                    } else {
                        System.out.println("[CENTRAL] Orden de parada sin riego en curso.");
                    }
                    break;
                case "BLOCK_WS":
                    if (activo != null) finalizar("Bloqueo de la estación");
                    bloqueada = true;
                    System.out.println("[CENTRAL] Estación BLOQUEADA (Out of Service).");
                    guardar();
                    break;
                case "ACTIVATE_WS":
                    bloqueada = false;
                    System.out.println("[CENTRAL] Estación ACTIVADA (Available).");
                    guardar();
                    break;
                default:
                    System.out.println("[CENTRAL] Orden desconocida: " + tipo);
            }
        }
        imprimirEstado();
    }

    private void iniciarRiego(String idSolicitud, int operario, int duracion) {
        if (averia || sinRespuesta) {
            System.out.println("[RIEGO] Rechazado: la estación está averiada.");
            return;
        }
        if (!monitorConectado) {
            System.out.println("[RIEGO] Rechazado: no hay enlace con el monitor (Averiada).");
            return;
        }
        if (bloqueada) {
            System.out.println("[RIEGO] Rechazado: la estación está bloqueada (Out of Service).");
            return;
        }
        if (activo != null) {
            System.out.println("[RIEGO] Rechazado: ya hay un riego en curso.");
            return;
        }
        if (duracion < 1 || duracion > 86400) {
            System.out.println("[RIEGO] Rechazado: duración no válida.");
            return;
        }
        if (idSolicitud.isEmpty()) idSolicitud = "LOCAL-" + java.util.UUID.randomUUID();
        activo = new Riego(idSolicitud, operario, duracion);
        System.out.println("[RIEGO] INICIADO " + idSolicitud + " | Operario: " + operario
                + " | Duración: " + duracion + " s | Caudal: " + fmt(caudalNominal) + " L/min");
        guardar();
    }

    /** Finaliza el riego en curso y deja su resultado pendiente de confirmar por CENTRAL. */
    private void finalizar(String motivo) {
        if (activo == null) return;
        Riego r = activo;
        long segundos = r.segundos();
        pendiente = new Pendiente(r.idSolicitud, r.operario, segundos, r.servicioLitros);
        activo = null;
        System.out.println("[RIEGO] FINALIZADO (" + motivo + ") " + r.idSolicitud
                + " | Servicio: " + fmt(r.servicioLitros) + " L en " + segundos
                + " s | Total acumulado: " + fmt(totalLitros) + " L");
        guardar();
    }

    // ---------------------------------------------------------------- Reloj

    private void iniciarReloj() {
        Thread reloj = new Thread(() -> {
            long anterior = System.currentTimeMillis();
            while (ejecutando) {
                long ahora = System.currentTimeMillis();
                long dt = ahora - anterior;
                anterior = ahora;

                boolean cerrarEnlace = false;
                synchronized (lock) {
                    if (activo != null && monitorConectado && !averia && !sinRespuesta) {
                        double incremento = caudalNominal / 60.0 * (dt / 1000.0);
                        activo.servicioLitros += incremento;
                        totalLitros += incremento;
                        if (activo.segundos() >= activo.duracion) {
                            finalizar("Duración completada");
                        }
                    }
                    if (monitorConectado && ahora - ultimoPoll > MONITOR_OBSOLETO_MS) {
                        cerrarEnlace = true;
                    }
                    if (ahora - ultimoGuardado >= 1_000L && (activo != null || pendiente != null)) {
                        guardar();
                    }
                }
                if (cerrarEnlace) {
                    System.out.println("[MONITOR] Sin comprobaciones de salud; se cierra el enlace averiado.");
                    perderMonitor("Monitor sin respuesta; la estación queda AVERIADA.");
                    Socket s = socketActual;
                    if (s != null) try { s.close(); } catch (IOException ignored) { }
                }
                dormir(TIC_MS);
            }
        }, "wm-ws-engine-clock");
        reloj.setDaemon(true);
        reloj.start();
    }

    private void perderMonitor(String motivo) {
        boolean teniaRiego;
        synchronized (lock) {
            teniaRiego = activo != null;
            if (monitorConectado || teniaRiego) {
                monitorConectado = false;
                if (activo != null) finalizar(motivo);
            }
            monitorConectado = false;
        }
        System.err.println("[ESTADO] " + motivo + (teniaRiego ? " El riego en curso finaliza." : ""));
        imprimirEstado();
    }

    // ---------------------------------------------------------------- Consola

    private void iniciarConsola() {
        Thread consola = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (ejecutando) {
                System.out.println();
                System.out.println("--- WM_WS_E (motor) ---");
                System.out.println("K. Simular avería KO (ON/OFF) | S. Motor sin respuesta (ON/OFF)");
                System.out.println("E. Ver estado                 | Q. Salir");
                System.out.print("Opción: ");
                if (!in.hasNextLine()) return;
                String opcion = in.nextLine().trim().toUpperCase(Locale.ROOT);
                switch (opcion) {
                    case "K": alternarAveria(); break;
                    case "S": alternarSinRespuesta(); break;
                    case "E": imprimirEstado(); break;
                    case "Q":
                        ejecutando = false;
                        return;
                    default: System.out.println("Opción no válida.");
                }
            }
        }, "wm-ws-engine-console");
        consola.setDaemon(true);
        consola.start();
    }

    public void alternarAveria() {
        synchronized (lock) {
            averia = !averia;
            if (averia) {
                sinRespuesta = false;
                System.out.println(">>> AVERÍA SIMULADA (KO): el motor responderá KO al monitor.");
                if (activo != null) finalizar("Avería simulada (KO)");
            } else {
                System.out.println(">>> AVERÍA RESUELTA: el motor vuelve a responder con su estado.");
            }
            guardar();
        }
        imprimirEstado();
    }

    public void alternarSinRespuesta() {
        synchronized (lock) {
            sinRespuesta = !sinRespuesta;
            if (sinRespuesta) {
                averia = false;
                System.out.println(">>> MOTOR SIN RESPUESTA: el monitor detectará la avería por timeout.");
                if (activo != null) finalizar("Motor sin respuesta");
            } else {
                System.out.println(">>> MOTOR RECUPERADO: vuelve a responder al monitor.");
            }
            guardar();
        }
        imprimirEstado();
    }

    /** Fotografía del motor para la interfaz gráfica. */
    public VistaMotor vistaMotor() {
        synchronized (lock) {
            String estado;
            if (averia || sinRespuesta || !monitorConectado) estado = "FUGA";
            else if (activo != null) estado = "REGANDO";
            else if (bloqueada) estado = "FUERA_SERVICIO";
            else estado = "ACTIVADA";

            double servicio = activo != null ? activo.servicioLitros : 0;
            long segundos = activo != null ? activo.segundos() : 0;
            int operario = activo != null ? activo.operario
                    : (pendiente != null ? pendiente.operario : 0);
            String idSolicitud = activo != null ? activo.idSolicitud
                    : (pendiente != null ? pendiente.idSolicitud : null);

            return new VistaMotor(idWs, ubicacion, caudalNominal, monitorConectado, averia,
                    sinRespuesta, bloqueada, estado, totalLitros, servicio, segundos,
                    operario, idSolicitud, pendiente == null ? null : pendiente.idSolicitud);
        }
    }

    private void imprimirEstado() {
        synchronized (lock) {
            String estado;
            if (averia) estado = "AVERÍA KO (ROJO)";
            else if (sinRespuesta) estado = "SIN RESPUESTA (ROJO)";
            else if (!monitorConectado) estado = "AVERIADA / MONITOR DESCONECTADO";
            else if (activo != null) estado = "Watering (REGANDO)";
            else if (bloqueada) estado = "Out of Service (NARANJA)";
            else estado = "Available (VERDE)";
            System.out.println("[" + hora() + "] WS " + (idWs > 0 ? idWs : "?") + " | " + estado
                    + " | Agua total: " + fmt(totalLitros) + " L"
                    + (activo != null ? " | Servicio: " + fmt(activo.servicioLitros) + " L"
                    + " | Operario: " + activo.operario : "")
                    + (pendiente != null ? " | Pendiente de reportar: " + pendiente.idSolicitud : ""));
        }
    }

    // ---------------------------------------------------------------- Persistencia

    private Path rutaEstado() {
        if (idWs <= 0) return null;
        return Paths.get(directorioEstado, "ws_" + idWs + ".estado");
    }

    /** Carga el volumen acumulado y el último riego no confirmado tras un reinicio. */
    private void cargarEstado() {
        estadoCargado = true;
        Path ruta = rutaEstado();
        if (ruta == null || !Files.isRegularFile(ruta)) return;
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(ruta)) {
            p.load(in);
            totalLitros = decimal(p.getProperty("total", "0"), 0);
            String activoRequest = p.getProperty("activeRequest", "").trim();
            if (!activoRequest.isEmpty()) {
                // El motor cayó durante un riego: se conserva lo ya regado y se reporta
                // como resultado final al recuperar la conexión.
                long segundos = (long) decimal(p.getProperty("activeElapsed", "0"), 0);
                double servicio = decimal(p.getProperty("activeService", "0"), 0);
                try {
                    long inicio = Long.parseLong(p.getProperty("activeStart", "0"));
                    if (inicio > 0) {
                        long transcurrido = (System.currentTimeMillis() - inicio) / 1000L;
                        long duracion = (long) decimal(p.getProperty("activeDuration", "0"), 0);
                        if (duracion > 0) transcurrido = Math.min(transcurrido, duracion);
                        segundos = Math.max(segundos, transcurrido);
                    }
                } catch (Exception ignored) { }
                pendiente = new Pendiente(activoRequest,
                        (int) decimal(p.getProperty("activeOperator", "0"), 0),
                        segundos, servicio);
                System.out.println("[ESTADO] Recuperado riego interrumpido " + activoRequest
                        + " (" + fmt(servicio) + " L); se reportará su estado final.");
            } else {
                String pendRequest = p.getProperty("pendRequest", "").trim();
                if (!pendRequest.isEmpty()) {
                    pendiente = new Pendiente(pendRequest,
                            (int) decimal(p.getProperty("pendOperator", "0"), 0),
                            (long) decimal(p.getProperty("pendElapsed", "0"), 0),
                            decimal(p.getProperty("pendService", "0"), 0));
                }
            }
            System.out.println("[ESTADO] Volumen acumulado recuperado: " + fmt(totalLitros) + " L.");
            guardar();
        } catch (Exception e) {
            System.err.println("[ESTADO] No se pudo leer " + ruta + ": " + mensajeCorto(e));
        }
    }

    /** Guarda el estado en disco. Se invoca con el monitor de {@link #lock} tomado. */
    private void guardar() {
        Path ruta = rutaEstado();
        if (ruta == null) return;
        Properties p = new Properties();
        p.setProperty("total", Double.toString(totalLitros));
        if (activo != null) {
            p.setProperty("activeRequest", activo.idSolicitud);
            p.setProperty("activeOperator", Integer.toString(activo.operario));
            p.setProperty("activeDuration", Long.toString(activo.duracion));
            p.setProperty("activeStart", Long.toString(activo.inicioMillis));
            p.setProperty("activeElapsed", Long.toString(activo.segundos()));
            p.setProperty("activeService", Double.toString(activo.servicioLitros));
        }
        if (pendiente != null) {
            p.setProperty("pendRequest", pendiente.idSolicitud);
            p.setProperty("pendOperator", Integer.toString(pendiente.operario));
            p.setProperty("pendElapsed", Long.toString(pendiente.segundos));
            p.setProperty("pendService", Double.toString(pendiente.servicio));
        }
        try {
            Path padre = ruta.getParent();
            if (padre != null) Files.createDirectories(padre);
            try (OutputStream out = Files.newOutputStream(ruta)) {
                p.store(out, "WM_WS_E - estado persistente de la estación " + idWs);
            }
            ultimoGuardado = System.currentTimeMillis();
        } catch (Exception e) {
            System.err.println("[ESTADO] No se pudo guardar " + ruta + ": " + mensajeCorto(e));
        }
    }

    public void detener() {
        ejecutando = false;
        synchronized (lock) {
            if (activo != null) finalizar("Cierre del motor");
            guardar();
        }
        Socket s = socketActual;
        if (s != null) try { s.close(); } catch (IOException ignored) { }
    }

    // ---------------------------------------------------------------- Utilidades

    private static String env(String nombre, String porDefecto) {
        String v = System.getenv(nombre);
        return v == null || v.trim().isEmpty() ? porDefecto : v;
    }

    private static int entero(String valor, int porDefecto) {
        try { return Integer.parseInt(valor.trim()); } catch (Exception e) { return porDefecto; }
    }

    private static double decimal(String valor, double porDefecto) {
        try {
            double d = Double.parseDouble(valor.trim());
            return Double.isFinite(d) ? d : porDefecto;
        } catch (Exception e) {
            return porDefecto;
        }
    }

    private static String fmt(double v) { return String.format(Locale.ROOT, "%.2f", v); }

    private static String hora() { return java.time.LocalTime.now().withNano(0).toString(); }

    private static String mensajeCorto(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.trim().isEmpty()) m = t.getClass().getSimpleName();
        return m;
    }

    private static void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /** Datos del motor para la interfaz gráfica. */
    public static final class VistaMotor {
        public final int idWs;
        public final String ubicacion;
        public final double caudalNominal;
        public final boolean monitorConectado;
        public final boolean averia;
        public final boolean sinRespuesta;
        public final boolean bloqueada;
        public final String estado;
        public final double litrosTotales;
        public final double litrosServicio;
        public final long segundos;
        public final int idOperario;
        public final String idSolicitud;
        public final String idSolicitudPendiente;

        VistaMotor(int idWs, String ubicacion, double caudalNominal, boolean monitorConectado,
                   boolean averia, boolean sinRespuesta, boolean bloqueada, String estado,
                   double litrosTotales, double litrosServicio, long segundos,
                   int idOperario, String idSolicitud, String idSolicitudPendiente) {
            this.idWs = idWs;
            this.ubicacion = ubicacion;
            this.caudalNominal = caudalNominal;
            this.monitorConectado = monitorConectado;
            this.averia = averia;
            this.sinRespuesta = sinRespuesta;
            this.bloqueada = bloqueada;
            this.estado = estado;
            this.litrosTotales = litrosTotales;
            this.litrosServicio = litrosServicio;
            this.segundos = segundos;
            this.idOperario = idOperario;
            this.idSolicitud = idSolicitud;
            this.idSolicitudPendiente = idSolicitudPendiente;
        }
    }

    private static final class Riego {
        final String idSolicitud;
        final int operario;
        final long duracion;
        final long inicioMillis = System.currentTimeMillis();
        double servicioLitros;

        Riego(String idSolicitud, int operario, long duracion) {
            this.idSolicitud = idSolicitud;
            this.operario = operario;
            this.duracion = duracion;
        }

        long segundos() {
            return Math.max(0, (System.currentTimeMillis() - inicioMillis) / 1000L);
        }
    }

    private static final class Pendiente {
        final String idSolicitud;
        final int operario;
        final long segundos;
        final double servicio;

        Pendiente(String idSolicitud, int operario, long segundos, double servicio) {
            this.idSolicitud = idSolicitud;
            this.operario = operario;
            this.segundos = segundos;
            this.servicio = servicio;
        }
    }
}

```


## WM_WS/src/WM_WS_M.java

```java
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.swing.SwingUtilities;

/**
 * WM_WS_M - Monitor de una estación WS.
 *
 * Responsabilidades:
 *  - Autenticarse contra WM_Central mediante ENQ + HELLO y aceptar/rechazar el registro.
 *  - Mantener una sesión persistente con WM_Central y enviar cada segundo el estado
 *    y la telemetría de la estación (protocolo STX/DATA/ETX/LRC).
 *  - Escuchar a WM_WS_E (motor) en un puerto propio y comprobar su salud cada segundo.
 *    Si el motor no responde o responde KO, la estación pasa a FUGA (Leak, rojo).
 *  - Recibir de WM_Central las órdenes (iniciar/parar/bloquear/activar) y reenviarlas
 *    al motor.
 *  - Ofrecer un menú local para controlar la estación (riego manual, bloqueo, etc.).
 *
 * Parámetros (argumentos o variables de entorno):
 *   puerto-motor  WM_MONITOR_PORT
 *   ip-central    WM_CENTRAL_HOST
 *   puerto-central WM_CENTRAL_PORT
 *   id-ws         WM_WS_ID
 *   ubicacion     WM_WS_LOCATION
 *   caudal L/min  WM_WS_FLOW
 *   [intervalo]   WM_STATUS_INTERVAL_MS  (por defecto 1000)
 */
public final class WM_WS_M {

    private static final int TIMEOUT_CONEXION_CENTRAL_MS = 5_000;
    private static final int TIMEOUT_LECTURA_CENTRAL_MS = 8_000;
    private static final int TIMEOUT_CONEXION_MOTOR_MS = 3_000;
    private static final long MOTOR_OBSOLETO_MS = 3_500L;
    private static final long SONDEO_MOTOR_MS = 700L;

    private final int puertoMotor;
    private final String ipCentral;
    private final int puertoCentral;
    private final int idWs;
    private final String ubicacion;
    private final double caudalNominal;
    private final long intervaloEstadoMs;

    private final EnlaceMotor motor;

    private volatile boolean ejecutando = true;
    private volatile boolean centralConectada;
    private volatile String ultimoAckCentral = "";

    private Socket socketCentral;
    private InputStream entradaCentral;
    private OutputStream salidaCentral;

    private volatile String ultimaLineaEstado = "";
    private volatile String ultimoEstadoCentral = "";

    private WM_WS_M(int puertoMotor, String ipCentral, int puertoCentral, int idWs,
                    String ubicacion, double caudalNominal, long intervaloEstadoMs) {
        this.puertoMotor = puertoMotor;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.idWs = idWs;
        this.ubicacion = ubicacion;
        this.caudalNominal = caudalNominal;
        this.intervaloEstadoMs = Math.max(250L, intervaloEstadoMs);
        this.motor = new EnlaceMotor(this, puertoMotor);
    }

    public static void main(String[] args) {
        Parametros p;
        try {
            p = Parametros.cargar(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error de configuración: " + e.getMessage());
            System.err.println("Uso: java WM_WS_M <puertoMotor> <ipCentral> <puertoCentral> <idWS> <ubicacion> <caudalLMin> [intervaloMs]");
            System.err.println("También se admiten WM_MONITOR_PORT, WM_CENTRAL_HOST, WM_CENTRAL_PORT,");
            System.err.println("WM_WS_ID, WM_WS_LOCATION, WM_WS_FLOW y WM_STATUS_INTERVAL_MS.");
            System.exit(2);
            return;
        }

        WM_WS_M monitor = new WM_WS_M(p.puertoMotor, p.ipCentral, p.puertoCentral, p.idWs,
                p.ubicacion, p.caudal, p.intervaloMs);
        Runtime.getRuntime().addShutdownHook(new Thread(monitor::detener, "wm-ws-monitor-shutdown"));

        boolean graphical = false;
        if (UiSupport.guiEnabled()) {
            try {
                SwingUtilities.invokeAndWait(() -> new WSMonitorFrame(monitor).setVisible(true));
                graphical = true;
            } catch (Throwable t) {
                System.err.println("No se pudo iniciar la interfaz gráfica del monitor: "
                        + t.getMessage() + ". Se usará la consola.");
            }
        }
        monitor.iniciar(!graphical);
    }

    private void iniciar(boolean conConsola) {
        System.out.println("=================================================");
        System.out.println(" WM_WS_M - MONITOR DE ESTACIÓN");
        System.out.println("=================================================");
        System.out.println("WS ID ...........: " + idWs);
        System.out.println("Ubicación .......: " + ubicacion);
        System.out.println("Caudal nominal ..: " + fmt(caudalNominal) + " L/min");
        System.out.println("Motor escuchando : puerto " + puertoMotor);
        System.out.println("WM_Central ......: " + ipCentral + ":" + puertoCentral);
        System.out.println("Intervalo estado : " + intervaloEstadoMs + " ms");
        System.out.println("=================================================");

        motor.start();
        if (conConsola) iniciarConsola();

        long siguiente = System.currentTimeMillis();
        while (ejecutando) {
            try {
                if (socketCentral == null || socketCentral.isClosed()) {
                    if (!conectarYAutenticar()) {
                        // Credenciales rechazadas: no tiene sentido reintentar en bucle.
                        if (ejecutando) dormir(3_000L);
                        continue;
                    }
                }
                enviarEstadoCentral();
            } catch (Exception e) {
                desconectarCentral("Imposible conectar con WM_Central: " + mensajeCorto(e));
            }

            imprimirEstado();
            siguiente += intervaloEstadoMs;
            long espera = siguiente - System.currentTimeMillis();
            if (espera < 0) {
                siguiente = System.currentTimeMillis();
                espera = 0;
            }
            dormir(espera);
        }
        detener();
    }

    // ---------------------------------------------------------------- Central

    private boolean conectarYAutenticar() {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(ipCentral, puertoCentral), TIMEOUT_CONEXION_CENTRAL_MS);
            s.setKeepAlive(true);
            s.setSoTimeout(TIMEOUT_LECTURA_CENTRAL_MS);
            InputStream in = new BufferedInputStream(s.getInputStream());
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            WireProtocol.enviarControl(out, WireProtocol.ENQ);
            int respuesta = leerControl(in, TIMEOUT_LECTURA_CENTRAL_MS);
            if (respuesta != WireProtocol.ACK) {
                throw new IOException("WM_Central no aceptó la petición de sesión (código " + respuesta + ")");
            }

            String ubicacionCodificada = URLEncoder.encode(ubicacion, StandardCharsets.UTF_8.name());
            WireProtocol.enviarTrama(out, "HELLO#" + idWs + "#" + ubicacionCodificada + "#" + fmt(caudalNominal));
            int ack = leerControl(in, TIMEOUT_LECTURA_CENTRAL_MS);
            if (ack != WireProtocol.ACK) throw new IOException("WM_Central no confirmó el HELLO (código " + ack + ")");

            String respuestaHello = WireProtocol.leerTrama(in);
            WireProtocol.enviarControl(out, WireProtocol.ACK);
            if (respuestaHello == null || !respuestaHello.startsWith("HELLO_OK#OK")) {
                System.err.println("REGISTRO DENEGADO por WM_Central: " + respuestaHello);
                System.err.println("La WS " + idWs + " no está autorizada. Revise WM_WS_ALLOWED_IDS en la central.");
                s.close();
                ejecutando = false;
                return false;
            }

            socketCentral = s;
            entradaCentral = in;
            salidaCentral = out;
            centralConectada = true;
            ultimoEstadoCentral = "";
            System.out.println("[CENTRAL] Sesión autenticada: " + respuestaHello);
            return true;
        } catch (Exception e) {
            try { s.close(); } catch (IOException ignored) { }
            desconectarCentral("Imposible conectar con WM_Central: " + mensajeCorto(e));
            return false;
        }
    }

    private void enviarEstadoCentral() throws IOException {
        EstadoMotor estado = motor.estadoParaCentral();
        String idSolicitud = estado.idSolicitud == null ? "" : estado.idSolicitud;
        String trama = "STATUS#" + estado.estado + "#" + fmt(estado.caudal) + "#" + fmt(estado.volumen)
                + "#" + estado.segundos + "#" + idSolicitud + "#" + estado.operario;
        WireProtocol.enviarTrama(salidaCentral, trama);

        int ack = leerControl(entradaCentral, TIMEOUT_LECTURA_CENTRAL_MS);
        if (ack != WireProtocol.ACK) {
            throw new IOException("WM_Central no confirmó la trama de estado (código " + ack + ")");
        }
        String respuesta = WireProtocol.leerTrama(entradaCentral);
        WireProtocol.enviarControl(salidaCentral, WireProtocol.ACK);
        centralConectada = true;

        if (respuesta != null && respuesta.startsWith("ACK#STATUS#OK")) {
            // Sólo se confirma al motor el informe FINAL de un servicio. Si se confirmara
            // también mientras la estación está REGANDO, el motor borraría su informe
            // final antes de que CENTRAL llegue a observar la transición a disponible y
            // el riego se cerraría como fallido por timeout.
            if (!idSolicitud.isEmpty() && !"REGANDO".equals(estado.estado)) {
                ultimoAckCentral = idSolicitud;
            }
        }
        motor.confirmarConCentral(ultimoAckCentral, centralConectada);
        procesarComandosCentral(respuesta);
    }

    /**
     * WM_Central adjunta las órdenes pendientes a la respuesta de cada STATUS:
     *   ACK#STATUS#OK#CMD#TIPO|idSolicitud|operario|duracion#CMD#TIPO...
     * Se reenvían al motor sin bloquear el envío de estados.
     */
    private void procesarComandosCentral(String respuesta) {
        if (respuesta == null) return;
        String[] partes = respuesta.split("#", -1);
        for (int i = 0; i < partes.length - 1; i++) {
            if ("CMD".equals(partes[i])) {
                String comando = partes[i + 1];
                if (!comando.trim().isEmpty()) {
                    motor.encolarComando(comando);
                    System.out.println("[CENTRAL] Orden recibida: " + comando);
                }
            }
        }
    }

    private void desconectarCentral(String motivo) {
        if (centralConectada || socketCentral != null) {
            System.err.println(motor.estaDisponible()
                    ? "[CENTRAL] " + motivo + ". La WS continúa su riego en curso."
                    : "[CENTRAL] " + motivo);
        }
        centralConectada = false;
        motor.confirmarConCentral("", false);
        cerrarSocketCentral();
        if (!"SIN CONEXION".equals(ultimoEstadoCentral)) {
            ultimoEstadoCentral = "SIN CONEXION";
            ultimaLineaEstado = "";
        }
    }

    private void cerrarSocketCentral() {
        Socket s = socketCentral;
        socketCentral = null;
        entradaCentral = null;
        salidaCentral = null;
        if (s != null) try { s.close(); } catch (IOException ignored) { }
    }

    // ---------------------------------------------------------------- Consola

    private void iniciarConsola() {
        Thread consola = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (ejecutando) {
                try {
                    System.out.println();
                    System.out.println("--- MENÚ WM_WS (monitor) ---");
                    System.out.println("1. Iniciar riego local | 2. Bloquear WS | 3. Activar WS");
                    System.out.println("4. Parar riego        | 5. Ver estado | 0. Salir");
                    System.out.print("Opción: ");
                    if (!in.hasNextLine()) return;
                    String opcion = in.nextLine().trim();
                    switch (opcion) {
                        case "1": iniciarRiegoLocal(in); break;
                        case "2": enviarComandoLocal("BLOCK_WS", 0); break;
                        case "3": enviarComandoLocal("ACTIVATE_WS", 0); break;
                        case "4": enviarComandoLocal("STOP_IRRIGATION", 0); break;
                        case "5": imprimirEstadoForzado(); break;
                        case "0":
                            System.out.println("Cerrando monitor...");
                            ejecutando = false;
                            return;
                        default: System.out.println("Opción no válida.");
                    }
                } catch (Exception e) {
                    System.err.println("Consola del monitor: " + mensajeCorto(e));
                }
            }
        }, "wm-ws-monitor-console");
        consola.setDaemon(true);
        consola.start();
    }

    private void iniciarRiegoLocal(Scanner in) {
        if (!centralConectada) {
            System.out.println("No se puede iniciar un riego: Imposible conectar con WM_Central.");
            return;
        }
        if (!motor.estaDisponible()) {
            System.out.println("No se puede iniciar un riego: la estación está Averiada (motor no disponible).");
            return;
        }
        System.out.print("Duración del riego en segundos (1-86400): ");
        if (!in.hasNextLine()) return;
        int duracion;
        try {
            duracion = Integer.parseInt(in.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Duración no válida.");
            return;
        }
        if (duracion < 1 || duracion > 86400) {
            System.out.println("La duración debe estar entre 1 y 86400 segundos.");
            return;
        }
        String idSolicitud = "LOCAL-" + UUID.randomUUID();
        motor.encolarComando("START_IRRIGATION|" + idSolicitud + "|0|" + duracion);
        System.out.println("Solicitud de riego local enviada al motor.");
    }

    private void enviarComandoLocal(String tipo, int operario) {
        String error = enviarComandoMonitor(tipo);
        System.out.println(error == null ? "Orden " + tipo + " enviada al motor." : error);
    }

    /** Inicia un riego local sin usar la consola. Devuelve null si se encoló correctamente. */
    public String solicitarRiegoLocal(int duracion) {
        if (!centralConectada) {
            return "No se puede iniciar un riego: Imposible conectar con WM_Central.";
        }
        if (!motor.estaDisponible()) {
            return "No se puede iniciar un riego: la estación está Averiada (motor no disponible).";
        }
        if (duracion < 1 || duracion > 86400) {
            return "La duración debe estar entre 1 y 86400 segundos.";
        }
        motor.encolarComando("START_IRRIGATION|LOCAL-" + UUID.randomUUID() + "|0|" + duracion);
        return null;
    }

    /** Encola una orden local (BLOCK_WS, ACTIVATE_WS o STOP_IRRIGATION). */
    public String enviarComandoMonitor(String tipo) {
        if (!motor.estaConectado()) {
            return "No se puede enviar la orden: el motor no está conectado.";
        }
        motor.encolarComando(tipo + "|LOCAL-" + UUID.randomUUID() + "|0|0");
        return null;
    }

    /** Fotografía del estado para la interfaz gráfica. */
    public VistaMonitor vistaMonitor() {
        EstadoMotor estado = motor.estadoParaCentral();
        return new VistaMonitor(idWs, ubicacion, caudalNominal, centralConectada,
                motor.estaConectado(), motor.sinRespuesta(), estado.estado,
                estado.caudal, estado.volumen, estado.segundos,
                estado.idSolicitud, estado.operario, ultimoAckCentral);
    }

    // ---------------------------------------------------------------- Panel

    private void imprimirEstado() {
        EstadoMotor e = motor.estadoParaCentral();
        String motorTxt = motor.estaConectado()
                ? (motor.sinRespuesta() ? "SIN RESPUESTA" : "OK")
                : "DESCONECTADO";
        String linea = String.format(Locale.ROOT,
                "WS %d [%s] | CENTRAL: %s | MOTOR: %s | ESTADO: %s | Caudal: %s L/min | Agua: %s L%s",
                idWs, ubicacion, centralConectada ? "CONECTADA" : "SIN CONEXIÓN", motorTxt,
                etiquetaEstado(e.estado), fmt(e.caudal), fmt(e.volumen),
                e.operario > 0 ? " | Operario: " + e.operario : "");
        if (!linea.equals(ultimaLineaEstado)) {
            ultimaLineaEstado = linea;
            System.out.println("[" + hora() + "] " + linea);
        }
    }

    private void imprimirEstadoForzado() {
        ultimaLineaEstado = "";
        imprimirEstado();
    }

    private static String etiquetaEstado(String estado) {
        if (estado == null) return "?";
        switch (estado) {
            case "ACTIVADA": return "Available (VERDE)";
            case "REGANDO": return "Watering (VERDE)";
            case "FUGA": return "Leak / Avería (ROJO)";
            case "FUERA_SERVICIO": return "Out of Service (NARANJA)";
            case "DESCONECTADA": return "Disconnected (GRIS)";
            default: return estado;
        }
    }

    void detener() {
        ejecutando = false;
        motor.detener();
        cerrarSocketCentral();
    }

    // ---------------------------------------------------------------- Utilidades

    private static int leerControl(InputStream in, int timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int b = in.read();
            if (b < 0) throw new IOException("La conexión se ha cerrado");
            if (b == WireProtocol.ACK || b == WireProtocol.NACK || b == WireProtocol.ENQ || b == WireProtocol.EOT) {
                return b;
            }
            // Ignora cualquier byte residual que no sea de control.
        }
        throw new SocketTimeoutException("Tiempo de espera agotado leyendo un byte de control");
    }

    private static String mensajeCorto(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.trim().isEmpty()) m = t.getClass().getSimpleName();
        return m;
    }

    private static String fmt(double v) { return String.format(Locale.ROOT, "%.2f", v); }

    private static String hora() {
        return java.time.LocalTime.now().withNano(0).toString();
    }

    private static void dormir(long ms) {
        if (ms <= 0) return;
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ---------------------------------------------------------------- Motor

    /** Datos de la estación para la interfaz gráfica del monitor. */
    public static final class VistaMonitor {
        public final int idWs;
        public final String ubicacion;
        public final double caudalNominal;
        public final boolean centralConectada;
        public final boolean motorConectado;
        public final boolean motorSinRespuesta;
        public final String estadoMotor;
        public final double caudalLMin;
        public final double litros;
        public final long segundos;
        public final String idSolicitud;
        public final int idOperario;
        public final String ultimoAckCentral;

        VistaMonitor(int idWs, String ubicacion, double caudalNominal, boolean centralConectada,
                    boolean motorConectado, boolean motorSinRespuesta, String estadoMotor,
                    double caudalLMin, double litros, long segundos,
                    String idSolicitud, int idOperario, String ultimoAckCentral) {
            this.idWs = idWs;
            this.ubicacion = ubicacion;
            this.caudalNominal = caudalNominal;
            this.centralConectada = centralConectada;
            this.motorConectado = motorConectado;
            this.motorSinRespuesta = motorSinRespuesta;
            this.estadoMotor = estadoMotor;
            this.caudalLMin = caudalLMin;
            this.litros = litros;
            this.segundos = segundos;
            this.idSolicitud = idSolicitud;
            this.idOperario = idOperario;
            this.ultimoAckCentral = ultimoAckCentral;
        }
    }

    /** Estado agregado que se envía a WM_Central. */
    static final class EstadoMotor {
        String estado = "FUGA";
        double caudal;
        double volumen;
        long segundos;
        String idSolicitud = "";
        int operario;
    }

    /**
     * Enlace con WM_WS_E. Mantiene una única conexión, le hace POLL cada segundo,
     * recibe su estado y le reenvía las órdenes de WM_Central o del menú local.
     */
    static final class EnlaceMotor extends Thread {
        private final WM_WS_M monitor;
        private final int puerto;
        private final BlockingQueue<String> comandos = new LinkedBlockingQueue<>();

        private volatile boolean activo = true;
        private volatile boolean conectado;
        private volatile boolean sinRespuesta;
        private volatile long ultimoMensaje;
        private volatile String ultimoAckCentral = "";
        private volatile boolean centralConectada;

        private volatile String estado = "FUGA";
        private volatile double caudal;
        private volatile double volumen;
        private volatile long segundos;
        private volatile String idSolicitud = "";
        private volatile int operario;

        private volatile Socket socketActual;
        private ServerSocket servidor;

        EnlaceMotor(WM_WS_M monitor, int puerto) {
            super("wm-ws-engine-link");
            this.monitor = monitor;
            this.puerto = puerto;
            setDaemon(true);
        }

        void encolarComando(String comando) {
            if (comando != null && !comando.trim().isEmpty()) comandos.offer(comando);
        }

        void confirmarConCentral(String idSolicitud, boolean online) {
            this.ultimoAckCentral = idSolicitud == null ? "" : idSolicitud;
            this.centralConectada = online;
        }

        boolean estaConectado() { return conectado; }
        boolean sinRespuesta() { return sinRespuesta || (conectado && System.currentTimeMillis() - ultimoMensaje > MOTOR_OBSOLETO_MS); }
        boolean estaDisponible() { return conectado && !sinRespuesta() && "ACTIVADA".equals(estado); }

        EstadoMotor estadoParaCentral() {
            EstadoMotor e = new EstadoMotor();
            boolean motorCaido = !conectado || sinRespuesta() || "FUGA".equals(estado) ||
                    System.currentTimeMillis() - ultimoMensaje > MOTOR_OBSOLETO_MS;
            if (motorCaido) {
                // La telemetría se conserva para poder informar del volumen ya regado.
                e.estado = "FUGA";
                e.caudal = 0;
                e.volumen = volumen;
                e.segundos = segundos;
                e.idSolicitud = idSolicitud;
                e.operario = operario;
            } else {
                e.estado = estado;
                e.caudal = caudal;
                e.volumen = volumen;
                e.segundos = segundos;
                e.idSolicitud = idSolicitud;
                e.operario = operario;
            }
            return e;
        }

        @Override
        public void run() {
            try {
                servidor = new ServerSocket();
                servidor.setReuseAddress(true);
                servidor.bind(new java.net.InetSocketAddress(puerto));
            } catch (IOException e) {
                System.err.println("[MOTOR] No se pudo abrir el puerto " + puerto + ": " + e.getMessage());
                activo = false;
                return;
            }
            System.out.println("[MOTOR] Esperando conexión de WM_WS_E en el puerto " + puerto + "...");

            while (activo) {
                try {
                    Socket s = servidor.accept();
                    atenderMotor(s);
                } catch (IOException e) {
                    if (activo) {
                        System.err.println("[MOTOR] Error de enlace: " + mensajeCorto(e) + ". Se reintenta...");
                        dormir(700);
                    }
                }
            }
            cerrarServidor();
        }

        private void atenderMotor(Socket s) {
            socketActual = s;
            try {
                s.setKeepAlive(true);
                s.setSoTimeout((int) Math.max(MOTOR_OBSOLETO_MS, SONDEO_MOTOR_MS + 1_500));
                InputStream in = new BufferedInputStream(s.getInputStream());
                OutputStream out = new BufferedOutputStream(s.getOutputStream());

                String ubicacion = URLEncoder.encode(monitor.ubicacion, StandardCharsets.UTF_8.name());
                WireProtocol.enviarTrama(out, "ID#" + monitor.idWs + "#" + ubicacion + "#" + fmt(monitor.caudalNominal));
                int ack = in.read();
                if (ack != WireProtocol.ACK) throw new IOException("El motor no confirmó la identificación (código " + ack + ")");
                System.out.println("[MOTOR] WM_WS_E identificado correctamente.");

                conectado = true;
                sinRespuesta = false;
                ultimoMensaje = System.currentTimeMillis();

                while (activo && !s.isClosed()) {
                    // 1) Órdenes pendientes (central o menú local).
                    String comando = comandos.poll(SONDEO_MOTOR_MS, TimeUnit.MILLISECONDS);
                    if (comando != null) {
                        WireProtocol.enviarTrama(out, "CMD#" + comando);
                        int c = in.read();
                        if (c == WireProtocol.NACK) {
                            comandos.offer(comando);
                        } else if (c != WireProtocol.ACK) {
                            throw new IOException("El motor no confirmó la orden (código " + c + ")");
                        }
                    }

                    // 2) Comprobación de salud + estado. POLL/STATUS es una
                    // petición-respuesta: el LRC protege la trama y no se envía ACK
                    // adicional (evita desincronizar el flujo de bytes).
                    WireProtocol.enviarTrama(out, "POLL#" + (centralConectada ? "1" : "0") + "#" + nz(ultimoAckCentral));

                    try {
                        String respuesta = WireProtocol.leerTrama(in);
                        if ("KO".equals(respuesta)) {
                            marcarCaida();
                        } else {
                            procesarEstado(respuesta);
                        }
                    } catch (SocketTimeoutException timeout) {
                        // El motor no ha respondido al POLL: se mantiene el enlace, pero
                        // desde este momento la estación se considera averiada.
                        marcarCaida();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (activo) System.err.println("[MOTOR] Enlace perdido: " + mensajeCorto(e));
            } finally {
                conectado = false;
                sinRespuesta = true;
                socketActual = null;
                try { s.close(); } catch (IOException ignored) { }
            }
        }

        private void procesarEstado(String respuesta) {
            if (respuesta == null || !respuesta.startsWith("STATUS#")) return;
            String[] p = respuesta.split("#", -1);
            if (p.length < 7) return;
            String nuevoEstado = p[1].trim().toUpperCase(Locale.ROOT);
            if (!"ACTIVADA".equals(nuevoEstado) && !"REGANDO".equals(nuevoEstado) &&
                    !"FUERA_SERVICIO".equals(nuevoEstado) && !"FUGA".equals(nuevoEstado)) {
                return;
            }
            estado = nuevoEstado;
            caudal = numero(p[2], caudal);
            volumen = numero(p[3], volumen);
            segundos = (long) numero(p[4], segundos);
            idSolicitud = p[5] == null ? "" : p[5];
            operario = (int) numero(p[6], operario);
            sinRespuesta = false;
            ultimoMensaje = System.currentTimeMillis();
        }

        private void marcarCaida() {
            sinRespuesta = true;
            estado = "FUGA";
            caudal = 0;
        }

        void detener() {
            activo = false;
            Socket s = socketActual;
            if (s != null) try { s.close(); } catch (IOException ignored) { }
            cerrarServidor();
            interrupt();
        }

        private void cerrarServidor() {
            ServerSocket ss = servidor;
            servidor = null;
            if (ss != null) try { ss.close(); } catch (IOException ignored) { }
        }

        private static String nz(String s) { return s == null ? "" : s; }

        private static double numero(String s, double fallback) {
            try {
                double v = Double.parseDouble(s.trim());
                return Double.isFinite(v) && v >= 0 ? v : fallback;
            } catch (Exception e) {
                return fallback;
            }
        }
    }

    // ---------------------------------------------------------------- Parámetros

    private static final class Parametros {
        int puertoMotor;
        String ipCentral;
        int puertoCentral;
        int idWs;
        String ubicacion;
        double caudal;
        long intervaloMs = 1_000L;

        static Parametros cargar(String[] args) {
            Parametros p = new Parametros();
            if (args.length >= 6) {
                p.puertoMotor = entero(args[0], "puerto del motor");
                p.ipCentral = args[1].trim();
                p.puertoCentral = entero(args[2], "puerto de WM_Central");
                p.idWs = entero(args[3], "ID de la WS");
                p.ubicacion = args[4].trim();
                p.caudal = decimal(args[5], "caudal");
                if (args.length >= 7) p.intervaloMs = entero(args[6], "intervalo de estado");
            } else {
                p.puertoMotor = entero(env("WM_MONITOR_PORT", "6000"), "WM_MONITOR_PORT");
                p.ipCentral = env("WM_CENTRAL_HOST", "localhost").trim();
                p.puertoCentral = entero(env("WM_CENTRAL_PORT", "5000"), "WM_CENTRAL_PORT");
                p.idWs = entero(env("WM_WS_ID", "1"), "WM_WS_ID");
                p.ubicacion = env("WM_WS_LOCATION", "Jardín principal").trim();
                p.caudal = decimal(env("WM_WS_FLOW", "10.0"), "WM_WS_FLOW");
                p.intervaloMs = entero(env("WM_STATUS_INTERVAL_MS", "1000"), "WM_STATUS_INTERVAL_MS");
            }
            if (p.puertoMotor < 1 || p.puertoMotor > 65535 || p.puertoCentral < 1 || p.puertoCentral > 65535) {
                throw new IllegalArgumentException("Los puertos deben estar entre 1 y 65535.");
            }
            if (p.ipCentral.isEmpty()) throw new IllegalArgumentException("La IP de WM_Central no puede estar vacía.");
            if (p.idWs <= 0) throw new IllegalArgumentException("El ID de la WS debe ser positivo.");
            if (p.ubicacion.isEmpty()) throw new IllegalArgumentException("La ubicación no puede estar vacía.");
            if (!Double.isFinite(p.caudal) || p.caudal <= 0) throw new IllegalArgumentException("El caudal debe ser mayor que cero.");
            if (p.intervaloMs < 250) throw new IllegalArgumentException("El intervalo de estado debe ser >= 250 ms.");
            return p;
        }

        private static String env(String nombre, String porDefecto) {
            String v = System.getenv(nombre);
            return v == null || v.trim().isEmpty() ? porDefecto : v;
        }

        private static int entero(String valor, String nombre) {
            try { return Integer.parseInt(valor.trim()); }
            catch (Exception e) { throw new IllegalArgumentException(nombre + " debe ser un entero."); }
        }

        private static double decimal(String valor, String nombre) {
            try {
                double v = Double.parseDouble(valor.trim().replace(',', '.'));
                if (!Double.isFinite(v)) throw new NumberFormatException();
                return v;
            } catch (Exception e) {
                throw new IllegalArgumentException(nombre + " debe ser un número.");
            }
        }
    }
}

```


## WM_WS/src/WSEngineFrame.java

```java
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.awt.Rectangle;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Locale;

/**
 * Interfaz gráfica de WM_WS_E (motor de la estación).
 *
 * Muestra el estado real del motor, el volumen acumulado y el informe pendiente,
 * y permite simular una avería KO o un motor sin respuesta para demostrar la
 * resiliencia del sistema sin usar la consola.
 */
final class WSEngineFrame extends JFrame {

    private static final Color VERDE = new Color(0x2E7D32);
    private static final Color RIEGO_A = new Color(0x1B5E20);
    private static final Color RIEGO_B = new Color(0x4CAF50);
    private static final Color ROJO = new Color(0xC62828);
    private static final Color NARANJA = new Color(0xEF6C00);

    private static final Color FONDO = new Color(0x1E1E1E);
    private static final Color FONDO_PANEL = new Color(0x252526);
    private static final Color TEXTO = new Color(0xF2F2F2);
    private static final Color ATENUADO = new Color(0xBBBBBB);

    private final WM_WS_E motor;

    private final JLabel etiquetaEstado = new JLabel("Leak");
    private final JLabel etiquetaCabecera = new JLabel(" ");
    private final JLabel etiquetaVolumen = new JLabel("Volumen acumulado: 0.00 L");
    private final JLabel etiquetaDetalle = new JLabel(" ");
    private final JLabel etiquetaPendiente = new JLabel(" ");
    private final JLabel etiquetaEnlace = new JLabel(" ");
    private final JLabel etiquetaMensaje = new JLabel("Listo.");
    private final JPanel tarjeta = new JPanel(new BorderLayout(0, 8));
    private final JButton botonAveria = new JButton("Simular avería KO");
    private final JButton botonSinRespuesta = new JButton("Motor sin respuesta");

    private String estado = "FUGA";
    private boolean parpadeo;

    WSEngineFrame(WM_WS_E motor) {
        super("WM_WS_E - Motor de estación");
        this.motor = motor;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                motor.detener();
                dispose();
                System.exit(0);
            }
        });

        getContentPane().setBackground(FONDO);
        setLayout(new BorderLayout(0, 10));
        add(cabecera(), BorderLayout.NORTH);
        add(panelTarjeta(), BorderLayout.CENTER);
        add(controles(), BorderLayout.SOUTH);

        Rectangle pantalla = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(720, Math.max(560, pantalla.width - 80)),
                Math.min(520, Math.max(400, pantalla.height - 80)));
        setMinimumSize(new Dimension(520, 380));
        setLocationRelativeTo(null);

        refrescar();
        new Timer(500, e -> {
            parpadeo = !parpadeo;
            refrescar();
        }).start();
    }

    private JComponent cabecera() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel titulo = new JLabel("WM_WS_E - Motor de estación");
        titulo.setFont(new Font("SansSerif", Font.BOLD, 22));
        titulo.setForeground(TEXTO);

        etiquetaCabecera.setFont(new Font("SansSerif", Font.PLAIN, 12));
        etiquetaCabecera.setForeground(ATENUADO);

        JPanel izquierda = new JPanel();
        izquierda.setOpaque(false);
        izquierda.setLayout(new BoxLayout(izquierda, BoxLayout.Y_AXIS));
        izquierda.add(titulo);
        izquierda.add(Box.createVerticalStrut(3));
        izquierda.add(etiquetaCabecera);

        panel.add(izquierda, BorderLayout.WEST);
        return panel;
    }

    private JComponent panelTarjeta() {
        tarjeta.setOpaque(true);
        tarjeta.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(0, 0, 0, 90), 1, true),
                new EmptyBorder(22, 24, 22, 24)));

        etiquetaEstado.setFont(new Font("SansSerif", Font.BOLD, 34));
        etiquetaEstado.setForeground(Color.WHITE);
        etiquetaVolumen.setFont(new Font("SansSerif", Font.BOLD, 15));
        etiquetaVolumen.setForeground(Color.WHITE);
        etiquetaDetalle.setFont(new Font("SansSerif", Font.PLAIN, 13));
        etiquetaDetalle.setForeground(Color.WHITE);
        etiquetaPendiente.setFont(new Font("SansSerif", Font.PLAIN, 12));
        etiquetaPendiente.setForeground(Color.WHITE);
        etiquetaEnlace.setFont(new Font("SansSerif", Font.BOLD, 13));
        etiquetaEnlace.setForeground(Color.WHITE);

        JPanel centro = new JPanel(new GridLayout(0, 1, 0, 4));
        centro.setOpaque(false);
        centro.add(etiquetaEstado);
        centro.add(etiquetaVolumen);
        centro.add(etiquetaDetalle);
        centro.add(etiquetaPendiente);

        JPanel enlaces = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        enlaces.setOpaque(false);
        enlaces.add(etiquetaEnlace);

        tarjeta.add(centro, BorderLayout.CENTER);
        tarjeta.add(enlaces, BorderLayout.SOUTH);

        JPanel envoltorio = new JPanel(new BorderLayout());
        envoltorio.setBackground(FONDO);
        envoltorio.setBorder(new EmptyBorder(14, 18, 0, 18));
        envoltorio.add(tarjeta, BorderLayout.CENTER);
        return envoltorio;
    }

    private JComponent controles() {
        JPanel panel = new JPanel(new BorderLayout(0, 6));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(10, 18, 12, 18));

        botonAveria.addActionListener(e -> {
            motor.alternarAveria();
            refrescar();
        });
        botonSinRespuesta.addActionListener(e -> {
            motor.alternarSinRespuesta();
            refrescar();
        });

        JPanel botones = new JPanel(new GridLayout(1, 2, 8, 0));
        botones.setOpaque(false);
        botones.add(botonAveria);
        botones.add(botonSinRespuesta);

        etiquetaMensaje.setForeground(ATENUADO);
        etiquetaMensaje.setFont(new Font("SansSerif", Font.PLAIN, 12));

        panel.add(botones, BorderLayout.NORTH);
        panel.add(etiquetaMensaje, BorderLayout.SOUTH);
        return panel;
    }

    private void refrescar() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refrescar);
            return;
        }

        WM_WS_E.VistaMotor vista = motor.vistaMotor();
        estado = vista.estado == null ? "FUGA" : vista.estado.toUpperCase(Locale.ROOT);

        etiquetaCabecera.setText("WS " + (vista.idWs > 0 ? vista.idWs : "?")
                + (vista.ubicacion == null || vista.ubicacion.isEmpty() ? "" : "  ·  " + vista.ubicacion)
                + "  ·  Caudal nominal " + fmt(vista.caudalNominal) + " L/min");
        etiquetaEstado.setText(etiquetaEstado(estado));
        etiquetaVolumen.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", vista.litrosTotales));

        if ("REGANDO".equals(estado)) {
            etiquetaDetalle.setText(String.format(Locale.ROOT,
                    "Servicio: %.2f L  ·  Operario: %s  ·  Tiempo: %d s",
                    vista.litrosServicio, vista.idOperario > 0 ? String.valueOf(vista.idOperario) : "-",
                    vista.segundos));
        } else if ("FUGA".equals(estado)) {
            if (vista.averia) etiquetaDetalle.setText("Avería KO simulada; el monitor responderá KO.");
            else if (vista.sinRespuesta) etiquetaDetalle.setText("Motor sin respuesta; el monitor detectará el timeout.");
            else etiquetaDetalle.setText("Monitor desconectado; la estación queda averiada.");
        } else if ("FUERA_SERVICIO".equals(estado)) {
            etiquetaDetalle.setText("Estación bloqueada (Out of Service).");
        } else {
            etiquetaDetalle.setText("Disponible para riego.");
        }

        etiquetaPendiente.setText(vista.idSolicitudPendiente == null ? " "
                : "Informe final pendiente de confirmar: " + vista.idSolicitudPendiente);
        etiquetaEnlace.setText("MONITOR: " + (vista.monitorConectado ? "conectado" : "desconectado")
                + "   ·   KO: " + (vista.averia ? "SÍ" : "no")
                + "   ·   Sin respuesta: " + (vista.sinRespuesta ? "SÍ" : "no"));
        etiquetaEnlace.setForeground(vista.monitorConectado && !vista.averia && !vista.sinRespuesta
                ? new Color(0xC8E6C9) : new Color(0xFFCDD2));

        botonAveria.setText(vista.averia ? "Resolver avería KO" : "Simular avería KO");
        botonSinRespuesta.setText(vista.sinRespuesta ? "Restablecer respuesta" : "Motor sin respuesta");
        etiquetaMensaje.setText("Volumen persistido en disco; no se pierde al reiniciar el motor.");

        switch (estado) {
            case "ACTIVADA": tarjeta.setBackground(VERDE); break;
            case "REGANDO": tarjeta.setBackground(parpadeo ? RIEGO_B : RIEGO_A); break;
            case "FUGA": tarjeta.setBackground(ROJO); break;
            case "FUERA_SERVICIO": tarjeta.setBackground(NARANJA); break;
            default: tarjeta.setBackground(ROJO); break;
        }
    }

    private static String etiquetaEstado(String estado) {
        switch (estado) {
            case "ACTIVADA": return "Available";
            case "REGANDO": return "Watering";
            case "FUGA": return "Leak";
            case "FUERA_SERVICIO": return "Out of Service";
            case "DESCONECTADA": return "Disconnected";
            default: return estado;
        }
    }

    private static String fmt(double valor) {
        return String.format(Locale.ROOT, "%.2f", valor);
    }
}

```


## WM_WS/src/WSMonitorFrame.java

```java
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.awt.Rectangle;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Locale;

/**
 * Interfaz gráfica de WM_WS_M (monitor de la estación).
 *
 * Muestra el estado que la central verá para esta WS y permite iniciar un riego
 * local, bloquear, activar o parar la estación sin usar la consola.
 */
final class WSMonitorFrame extends JFrame {

    private static final Color VERDE = new Color(0x2E7D32);
    private static final Color RIEGO_A = new Color(0x1B5E20);
    private static final Color RIEGO_B = new Color(0x4CAF50);
    private static final Color ROJO = new Color(0xC62828);
    private static final Color NARANJA = new Color(0xEF6C00);
    private static final Color GRIS = new Color(0x616161);

    private static final Color FONDO = new Color(0x1E1E1E);
    private static final Color FONDO_PANEL = new Color(0x252526);
    private static final Color TEXTO = new Color(0xF2F2F2);
    private static final Color ATENUADO = new Color(0xBBBBBB);

    private final WM_WS_M monitor;

    private final JLabel etiquetaEstado = new JLabel("Disconnected");
    private final JLabel etiquetaUbicacion = new JLabel(" ");
    private final JLabel etiquetaVolumen = new JLabel("Volumen acumulado: 0.00 L");
    private final JLabel etiquetaDetalle = new JLabel(" ");
    private final JLabel etiquetaCentral = new JLabel("CENTRAL: sin conexión");
    private final JLabel etiquetaMotor = new JLabel("MOTOR: desconectado");
    private final JLabel etiquetaMensaje = new JLabel("Listo.");
    private final JPanel tarjeta = new JPanel(new BorderLayout(0, 8));

    private String estado = "DESCONECTADA";
    private boolean parpadeo;

    WSMonitorFrame(WM_WS_M monitor) {
        super("WM_WS_M - Monitor de estación");
        this.monitor = monitor;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                monitor.detener();
                dispose();
                System.exit(0);
            }
        });

        getContentPane().setBackground(FONDO);
        setLayout(new BorderLayout(0, 10));
        add(cabecera(), BorderLayout.NORTH);
        add(panelTarjeta(), BorderLayout.CENTER);
        add(controles(), BorderLayout.SOUTH);

        Rectangle pantalla = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(720, Math.max(560, pantalla.width - 80)),
                Math.min(520, Math.max(400, pantalla.height - 80)));
        setMinimumSize(new Dimension(520, 380));
        setLocationRelativeTo(null);

        refrescar();
        new Timer(1000, e -> refrescar()).start();
        new Timer(500, e -> {
            parpadeo = !parpadeo;
            pintarEstado();
        }).start();
    }

    private JComponent cabecera() {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel titulo = new JLabel("WM_WS_M - Monitor de estación");
        titulo.setFont(new Font("SansSerif", Font.BOLD, 22));
        titulo.setForeground(TEXTO);

        etiquetaUbicacion.setFont(new Font("SansSerif", Font.PLAIN, 12));
        etiquetaUbicacion.setForeground(ATENUADO);

        JPanel izquierda = new JPanel();
        izquierda.setOpaque(false);
        izquierda.setLayout(new BoxLayout(izquierda, BoxLayout.Y_AXIS));
        izquierda.add(titulo);
        izquierda.add(Box.createVerticalStrut(3));
        izquierda.add(etiquetaUbicacion);

        panel.add(izquierda, BorderLayout.WEST);
        return panel;
    }

    private JComponent panelTarjeta() {
        tarjeta.setOpaque(true);
        tarjeta.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(0, 0, 0, 90), 1, true),
                new EmptyBorder(22, 24, 22, 24)));

        etiquetaEstado.setFont(new Font("SansSerif", Font.BOLD, 34));
        etiquetaEstado.setForeground(Color.WHITE);
        etiquetaVolumen.setFont(new Font("SansSerif", Font.BOLD, 15));
        etiquetaVolumen.setForeground(Color.WHITE);
        etiquetaDetalle.setFont(new Font("SansSerif", Font.PLAIN, 13));
        etiquetaDetalle.setForeground(Color.WHITE);

        etiquetaCentral.setFont(new Font("SansSerif", Font.BOLD, 13));
        etiquetaCentral.setForeground(Color.WHITE);
        etiquetaMotor.setFont(new Font("SansSerif", Font.BOLD, 13));
        etiquetaMotor.setForeground(Color.WHITE);

        JPanel centro = new JPanel(new GridLayout(0, 1, 0, 4));
        centro.setOpaque(false);
        centro.add(etiquetaEstado);
        centro.add(etiquetaVolumen);
        centro.add(etiquetaDetalle);

        JPanel enlaces = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        enlaces.setOpaque(false);
        enlaces.add(etiquetaCentral);
        enlaces.add(Box.createHorizontalStrut(18));
        enlaces.add(etiquetaMotor);

        tarjeta.add(centro, BorderLayout.CENTER);
        tarjeta.add(enlaces, BorderLayout.SOUTH);

        JPanel envoltorio = new JPanel(new BorderLayout());
        envoltorio.setBackground(FONDO);
        envoltorio.setBorder(new EmptyBorder(14, 18, 0, 18));
        envoltorio.add(tarjeta, BorderLayout.CENTER);
        return envoltorio;
    }

    private JComponent controles() {
        JPanel panel = new JPanel(new BorderLayout(0, 6));
        panel.setBackground(FONDO_PANEL);
        panel.setBorder(new EmptyBorder(10, 18, 12, 18));

        JButton regar = new JButton("Iniciar riego local");
        regar.addActionListener(e -> iniciarRiegoLocal());
        JButton bloquear = new JButton("Bloquear WS");
        bloquear.addActionListener(e -> enviarComando("BLOCK_WS", "bloqueo"));
        JButton activar = new JButton("Activar WS");
        activar.addActionListener(e -> enviarComando("ACTIVATE_WS", "activación"));
        JButton parar = new JButton("Parar riego");
        parar.addActionListener(e -> enviarComando("STOP_IRRIGATION", "parada"));

        JPanel botones = new JPanel(new GridLayout(1, 4, 8, 0));
        botones.setOpaque(false);
        botones.add(regar);
        botones.add(bloquear);
        botones.add(activar);
        botones.add(parar);

        etiquetaMensaje.setForeground(ATENUADO);
        etiquetaMensaje.setFont(new Font("SansSerif", Font.PLAIN, 12));

        panel.add(botones, BorderLayout.NORTH);
        panel.add(etiquetaMensaje, BorderLayout.SOUTH);
        return panel;
    }

    private void refrescar() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refrescar);
            return;
        }

        WM_WS_M.VistaMonitor vista = monitor.vistaMonitor();
        estado = estadoEfectivo(vista);

        etiquetaUbicacion.setText("WS " + vista.idWs + "  ·  " + vista.ubicacion
                + "  ·  Caudal nominal " + fmt(vista.caudalNominal) + " L/min");
        etiquetaEstado.setText(etiquetaEstado(estado));
        etiquetaVolumen.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", vista.litros));
        etiquetaCentral.setText(vista.centralConectada ? "CENTRAL: conectada" : "CENTRAL: sin conexión");
        etiquetaCentral.setForeground(vista.centralConectada ? new Color(0x81C784) : new Color(0xE57373));
        etiquetaMotor.setText("MOTOR: " + (!vista.motorConectado ? "desconectado"
                : vista.motorSinRespuesta ? "sin respuesta" : "OK"));
        etiquetaMotor.setForeground(vista.motorConectado && !vista.motorSinRespuesta
                ? new Color(0x81C784) : new Color(0xE57373));

        if ("REGANDO".equalsIgnoreCase(vista.estadoMotor)) {
            etiquetaDetalle.setText(String.format(Locale.ROOT,
                    "Caudal: %.2f L/min  ·  Operario: %s  ·  Tiempo: %d s",
                    vista.caudalLMin, vista.idOperario > 0 ? String.valueOf(vista.idOperario) : "-",
                    vista.segundos));
        } else if ("FUERA_SERVICIO".equalsIgnoreCase(vista.estadoMotor)) {
            etiquetaDetalle.setText("Estación bloqueada (Out of Service).");
        } else if ("FUGA".equalsIgnoreCase(estado)) {
            etiquetaDetalle.setText("Motor averiado o sin respuesta; la central verá Leak.");
        } else if ("DESCONECTADA".equalsIgnoreCase(estado)) {
            etiquetaDetalle.setText("Sin enlace con WM_Central; la central verá Disconnected.");
        } else {
            etiquetaDetalle.setText("Disponible para riego.");
        }
        pintarEstado();
    }

    private String estadoEfectivo(WM_WS_M.VistaMonitor vista) {
        if (!vista.centralConectada) return "DESCONECTADA";
        if (!vista.motorConectado || vista.motorSinRespuesta
                || "FUGA".equalsIgnoreCase(vista.estadoMotor)) return "FUGA";
        return vista.estadoMotor == null ? "FUGA" : vista.estadoMotor.toUpperCase(Locale.ROOT);
    }

    private void pintarEstado() {
        switch (estado) {
            case "ACTIVADA": tarjeta.setBackground(VERDE); break;
            case "REGANDO": tarjeta.setBackground(parpadeo ? RIEGO_B : RIEGO_A); break;
            case "FUGA": tarjeta.setBackground(ROJO); break;
            case "FUERA_SERVICIO": tarjeta.setBackground(NARANJA); break;
            default: tarjeta.setBackground(GRIS); break;
        }
    }

    private void iniciarRiegoLocal() {
        String valor = JOptionPane.showInputDialog(this, "Duración del riego en segundos (1-86400):",
                "Riego local", JOptionPane.QUESTION_MESSAGE);
        if (valor == null) return;
        int duracion;
        try {
            duracion = Integer.parseInt(valor.trim());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "Duración no válida.", "WM_WS_M", JOptionPane.WARNING_MESSAGE);
            return;
        }
        String error = monitor.solicitarRiegoLocal(duracion);
        if (error != null) {
            JOptionPane.showMessageDialog(this, error, "WM_WS_M", JOptionPane.WARNING_MESSAGE);
        } else {
            etiquetaMensaje.setText("Solicitud de riego local enviada al motor.");
        }
        refrescar();
    }

    private void enviarComando(String tipo, String descripcion) {
        String error = monitor.enviarComandoMonitor(tipo);
        if (error != null) {
            JOptionPane.showMessageDialog(this, error, "WM_WS_M", JOptionPane.WARNING_MESSAGE);
        } else {
            etiquetaMensaje.setText("Orden de " + descripcion + " enviada al motor.");
        }
        refrescar();
    }

    private static String etiquetaEstado(String estado) {
        switch (estado) {
            case "ACTIVADA": return "Available";
            case "REGANDO": return "Watering";
            case "FUGA": return "Leak";
            case "FUERA_SERVICIO": return "Out of Service";
            case "DESCONECTADA": return "Disconnected";
            default: return estado;
        }
    }

    private static String fmt(double valor) {
        return String.format(Locale.ROOT, "%.2f", valor);
    }
}

```
