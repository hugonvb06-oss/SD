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
