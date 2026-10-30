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

    private static final Color AVAILABLE = new Color(0x2E7D32);
    private static final Color WATERING_A = new Color(0x1B5E20);
    private static final Color WATERING_B = new Color(0x4CAF50);
    private static final Color LEAK = new Color(0xC62828);
    private static final Color OUT_OF_SERVICE = new Color(0xEF6C00);

    private static final Color BACKGROUND = new Color(0x1E1E1E);
    private static final Color PANEL_BACKGROUND = new Color(0x252526);
    private static final Color TEXT = new Color(0xF2F2F2);
    private static final Color MUTED = new Color(0xBBBBBB);

    private final WM_WS_E engine;

    private final JLabel stateLabel = new JLabel("Leak");
    private final JLabel headerLabel = new JLabel(" ");
    private final JLabel volumeLabel = new JLabel("Volumen acumulado: 0.00 L");
    private final JLabel detailLabel = new JLabel(" ");
    private final JLabel pendingLabel = new JLabel(" ");
    private final JLabel linkLabel = new JLabel(" ");
    private final JLabel statusLabel = new JLabel("Listo.");
    private final JPanel card = new JPanel(new BorderLayout(0, 8));
    private final JButton koButton = new JButton("Simular avería KO");
    private final JButton noResponseButton = new JButton("Motor sin respuesta");

    private String state = "FUGA";
    private boolean blinkOn;

    WSEngineFrame(WM_WS_E engine) {
        super("WM_WS_E - Motor de estación");
        this.engine = engine;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                engine.detener();
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
        new Timer(500, e -> {
            blinkOn = !blinkOn;
            refresh();
        }).start();
    }

    private JComponent header() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel title = new JLabel("WM_WS_E - Motor de estación");
        title.setFont(new Font("SansSerif", Font.BOLD, 22));
        title.setForeground(TEXT);

        headerLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
        headerLabel.setForeground(MUTED);

        JPanel left = new JPanel();
        left.setOpaque(false);
        left.setLayout(new BoxLayout(left, BoxLayout.Y_AXIS));
        left.add(title);
        left.add(Box.createVerticalStrut(3));
        left.add(headerLabel);

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
        pendingLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
        pendingLabel.setForeground(Color.WHITE);
        linkLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        linkLabel.setForeground(Color.WHITE);

        JPanel centre = new JPanel(new GridLayout(0, 1, 0, 4));
        centre.setOpaque(false);
        centre.add(stateLabel);
        centre.add(volumeLabel);
        centre.add(detailLabel);
        centre.add(pendingLabel);

        JPanel links = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        links.setOpaque(false);
        links.add(linkLabel);

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

        koButton.addActionListener(e -> {
            engine.alternarKo();
            refresh();
        });
        noResponseButton.addActionListener(e -> {
            engine.alternarSinRespuesta();
            refresh();
        });

        JPanel buttons = new JPanel(new GridLayout(1, 2, 8, 0));
        buttons.setOpaque(false);
        buttons.add(koButton);
        buttons.add(noResponseButton);

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

        WM_WS_E.EngineView view = engine.vistaMotor();
        state = view.state == null ? "FUGA" : view.state.toUpperCase(Locale.ROOT);

        headerLabel.setText("WS " + (view.wsId > 0 ? view.wsId : "?")
                + (view.location == null || view.location.isEmpty() ? "" : "  ·  " + view.location)
                + "  ·  Caudal nominal " + fmt(view.nominalFlow) + " L/min");
        stateLabel.setText(displayState(state));
        volumeLabel.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", view.totalLitres));

        if ("REGANDO".equals(state)) {
            detailLabel.setText(String.format(Locale.ROOT,
                    "Servicio: %.2f L  ·  Operario: %s  ·  Tiempo: %d s",
                    view.serviceLitres, view.operatorId > 0 ? String.valueOf(view.operatorId) : "-",
                    view.elapsedSeconds));
        } else if ("FUGA".equals(state)) {
            if (view.ko) detailLabel.setText("Avería KO simulada; el monitor responderá KO.");
            else if (view.noResponse) detailLabel.setText("Motor sin respuesta; el monitor detectará el timeout.");
            else detailLabel.setText("Monitor desconectado; la estación queda averiada.");
        } else if ("FUERA_SERVICIO".equals(state)) {
            detailLabel.setText("Estación bloqueada (Out of Service).");
        } else {
            detailLabel.setText("Disponible para riego.");
        }

        pendingLabel.setText(view.pendingRequestId == null ? " "
                : "Informe final pendiente de confirmar: " + view.pendingRequestId);
        linkLabel.setText("MONITOR: " + (view.monitorConnected ? "conectado" : "desconectado")
                + "   ·   KO: " + (view.ko ? "SÍ" : "no")
                + "   ·   Sin respuesta: " + (view.noResponse ? "SÍ" : "no"));
        linkLabel.setForeground(view.monitorConnected && !view.ko && !view.noResponse
                ? new Color(0xC8E6C9) : new Color(0xFFCDD2));

        koButton.setText(view.ko ? "Resolver avería KO" : "Simular avería KO");
        noResponseButton.setText(view.noResponse ? "Restablecer respuesta" : "Motor sin respuesta");
        statusLabel.setText("Volumen persistido en disco; no se pierde al reiniciar el motor.");

        switch (state) {
            case "ACTIVADA": card.setBackground(AVAILABLE); break;
            case "REGANDO": card.setBackground(blinkOn ? WATERING_B : WATERING_A); break;
            case "FUGA": card.setBackground(LEAK); break;
            case "FUERA_SERVICIO": card.setBackground(OUT_OF_SERVICE); break;
            default: card.setBackground(LEAK); break;
        }
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
