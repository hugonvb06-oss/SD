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

    private static final Color AVAILABLE = new Color(0x2E7D32);
    private static final Color WATERING_A = new Color(0x1B5E20);
    private static final Color WATERING_B = new Color(0x4CAF50);
    private static final Color LEAK = new Color(0xC62828);
    private static final Color OUT_OF_SERVICE = new Color(0xEF6C00);
    private static final Color DISCONNECTED = new Color(0x616161);

    private static final Color BACKGROUND = new Color(0x1E1E1E);
    private static final Color PANEL_BACKGROUND = new Color(0x252526);
    private static final Color TEXT = new Color(0xF2F2F2);
    private static final Color MUTED = new Color(0xBBBBBB);

    private final WM_WS_M monitor;

    private final JLabel stateLabel = new JLabel("Disconnected");
    private final JLabel locationLabel = new JLabel(" ");
    private final JLabel volumeLabel = new JLabel("Volumen acumulado: 0.00 L");
    private final JLabel detailLabel = new JLabel(" ");
    private final JLabel centralLabel = new JLabel("CENTRAL: sin conexión");
    private final JLabel engineLabel = new JLabel("MOTOR: desconectado");
    private final JLabel statusLabel = new JLabel("Listo.");
    private final JPanel card = new JPanel(new BorderLayout(0, 8));

    private String state = "DESCONECTADA";
    private boolean blinkOn;

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

        getContentPane().setBackground(BACKGROUND);
        setLayout(new BorderLayout(0, 10));
        add(header(), BorderLayout.NORTH);
        add(cardPanel(), BorderLayout.CENTER);
        add(controls(), BorderLayout.SOUTH);

        Rectangle screen = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(720, Math.max(560, screen.width - 80)),
                Math.min(520, Math.max(400, screen.height - 80)));
        setMinimumSize(new Dimension(520, 380));
        setLocationRelativeTo(null);

        refresh();
        new Timer(1000, e -> refresh()).start();
        new Timer(500, e -> {
            blinkOn = !blinkOn;
            paintState();
        }).start();
    }

    private JComponent header() {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel title = new JLabel("WM_WS_M - Monitor de estación");
        title.setFont(new Font("SansSerif", Font.BOLD, 22));
        title.setForeground(TEXT);

        locationLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
        locationLabel.setForeground(MUTED);

        JPanel left = new JPanel();
        left.setOpaque(false);
        left.setLayout(new BoxLayout(left, BoxLayout.Y_AXIS));
        left.add(title);
        left.add(Box.createVerticalStrut(3));
        left.add(locationLabel);

        panel.add(left, BorderLayout.WEST);
        return panel;
    }

    private JComponent cardPanel() {
        card.setOpaque(true);
        card.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(0, 0, 0, 90), 1, true),
                new EmptyBorder(22, 24, 22, 24)));

        stateLabel.setFont(new Font("SansSerif", Font.BOLD, 34));
        stateLabel.setForeground(Color.WHITE);
        volumeLabel.setFont(new Font("SansSerif", Font.BOLD, 15));
        volumeLabel.setForeground(Color.WHITE);
        detailLabel.setFont(new Font("SansSerif", Font.PLAIN, 13));
        detailLabel.setForeground(Color.WHITE);

        centralLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        centralLabel.setForeground(Color.WHITE);
        engineLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        engineLabel.setForeground(Color.WHITE);

        JPanel centre = new JPanel(new GridLayout(0, 1, 0, 4));
        centre.setOpaque(false);
        centre.add(stateLabel);
        centre.add(volumeLabel);
        centre.add(detailLabel);

        JPanel links = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        links.setOpaque(false);
        links.add(centralLabel);
        links.add(Box.createHorizontalStrut(18));
        links.add(engineLabel);

        card.add(centre, BorderLayout.CENTER);
        card.add(links, BorderLayout.SOUTH);

        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.setBackground(BACKGROUND);
        wrapper.setBorder(new EmptyBorder(14, 18, 0, 18));
        wrapper.add(card, BorderLayout.CENTER);
        return wrapper;
    }

    private JComponent controls() {
        JPanel panel = new JPanel(new BorderLayout(0, 6));
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(10, 18, 12, 18));

        JButton irrigate = new JButton("Iniciar riego local");
        irrigate.addActionListener(e -> startLocalIrrigation());
        JButton block = new JButton("Bloquear WS");
        block.addActionListener(e -> sendCommand("BLOCK_WS", "bloqueo"));
        JButton activate = new JButton("Activar WS");
        activate.addActionListener(e -> sendCommand("ACTIVATE_WS", "activación"));
        JButton stop = new JButton("Parar riego");
        stop.addActionListener(e -> sendCommand("STOP_IRRIGATION", "parada"));

        JPanel buttons = new JPanel(new GridLayout(1, 4, 8, 0));
        buttons.setOpaque(false);
        buttons.add(irrigate);
        buttons.add(block);
        buttons.add(activate);
        buttons.add(stop);

        statusLabel.setForeground(MUTED);
        statusLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));

        panel.add(buttons, BorderLayout.NORTH);
        panel.add(statusLabel, BorderLayout.SOUTH);
        return panel;
    }

    private void refresh() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refresh);
            return;
        }

        WM_WS_M.MonitorView view = monitor.vistaMonitor();
        state = effectiveState(view);

        locationLabel.setText("WS " + view.wsId + "  ·  " + view.location
                + "  ·  Caudal nominal " + fmt(view.nominalFlow) + " L/min");
        stateLabel.setText(displayState(state));
        volumeLabel.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", view.volumeLitres));
        centralLabel.setText(view.centralOnline ? "CENTRAL: conectada" : "CENTRAL: sin conexión");
        centralLabel.setForeground(view.centralOnline ? new Color(0x81C784) : new Color(0xE57373));
        engineLabel.setText("MOTOR: " + (!view.engineConnected ? "desconectado"
                : view.engineNoResponse ? "sin respuesta" : "OK"));
        engineLabel.setForeground(view.engineConnected && !view.engineNoResponse
                ? new Color(0x81C784) : new Color(0xE57373));

        if ("REGANDO".equalsIgnoreCase(view.engineState)) {
            detailLabel.setText(String.format(Locale.ROOT,
                    "Caudal: %.2f L/min  ·  Operario: %s  ·  Tiempo: %d s",
                    view.flowLMin, view.operatorId > 0 ? String.valueOf(view.operatorId) : "-",
                    view.elapsedSeconds));
        } else if ("FUERA_SERVICIO".equalsIgnoreCase(view.engineState)) {
            detailLabel.setText("Estación bloqueada (Out of Service).");
        } else if ("FUGA".equalsIgnoreCase(state)) {
            detailLabel.setText("Motor averiado o sin respuesta; la central verá Leak.");
        } else if ("DESCONECTADA".equalsIgnoreCase(state)) {
            detailLabel.setText("Sin enlace con WM_Central; la central verá Disconnected.");
        } else {
            detailLabel.setText("Disponible para riego.");
        }
        paintState();
    }

    private String effectiveState(WM_WS_M.MonitorView view) {
        if (!view.centralOnline) return "DESCONECTADA";
        if (!view.engineConnected || view.engineNoResponse
                || "FUGA".equalsIgnoreCase(view.engineState)) return "FUGA";
        return view.engineState == null ? "FUGA" : view.engineState.toUpperCase(Locale.ROOT);
    }

    private void paintState() {
        switch (state) {
            case "ACTIVADA": card.setBackground(AVAILABLE); break;
            case "REGANDO": card.setBackground(blinkOn ? WATERING_B : WATERING_A); break;
            case "FUGA": card.setBackground(LEAK); break;
            case "FUERA_SERVICIO": card.setBackground(OUT_OF_SERVICE); break;
            default: card.setBackground(DISCONNECTED); break;
        }
    }

    private void startLocalIrrigation() {
        String value = JOptionPane.showInputDialog(this, "Duración del riego en segundos (1-86400):",
                "Riego local", JOptionPane.QUESTION_MESSAGE);
        if (value == null) return;
        int duration;
        try {
            duration = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "Duración no válida.", "WM_WS_M", JOptionPane.WARNING_MESSAGE);
            return;
        }
        String error = monitor.solicitarRiegoLocal(duration);
        if (error != null) {
            JOptionPane.showMessageDialog(this, error, "WM_WS_M", JOptionPane.WARNING_MESSAGE);
        } else {
            statusLabel.setText("Solicitud de riego local enviada al motor.");
        }
        refresh();
    }

    private void sendCommand(String type, String description) {
        String error = monitor.enviarComandoMonitor(type);
        if (error != null) {
            JOptionPane.showMessageDialog(this, error, "WM_WS_M", JOptionPane.WARNING_MESSAGE);
        } else {
            statusLabel.setText("Orden de " + description + " enviada al motor.");
        }
        refresh();
    }

    private static String displayState(String state) {
        switch (state) {
            case "ACTIVADA": return "Available";
            case "REGANDO": return "Watering";
            case "FUGA": return "Leak";
            case "FUERA_SERVICIO": return "Out of Service";
            case "DESCONECTADA": return "Disconnected";
            default: return state;
        }
    }

    private static String fmt(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }
}
