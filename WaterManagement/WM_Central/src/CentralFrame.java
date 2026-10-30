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

    private static final Color AVAILABLE = new Color(0x2E7D32);
    private static final Color WATERING_A = new Color(0x1B5E20);
    private static final Color WATERING_B = new Color(0x4CAF50);
    private static final Color LEAK = new Color(0xC62828);
    private static final Color OUT_OF_SERVICE = new Color(0xEF6C00);
    private static final Color DISCONNECTED = new Color(0x616161);
    private static final Color UNKNOWN = new Color(0x424242);

    private static final Color BACKGROUND = new Color(0x1E1E1E);
    private static final Color PANEL_BACKGROUND = new Color(0x252526);
    private static final Color TEXT = new Color(0xF2F2F2);
    private static final Color MUTED = new Color(0xBBBBBB);

    private final CentralService central;
    private final JPanel stationsPanel = new JPanel();
    private final Map<Integer, StationCard> cards = new LinkedHashMap<>();
    private final JLabel summaryLabel = new JLabel(" ");
    private final JLabel statusLabel = new JLabel("Listo.");
    private final JLabel emptyLabel = new JLabel("No hay estaciones WS registradas.", SwingConstants.CENTER);

    private final JTextField operatorIdField = new JTextField(6);
    private final JTextField operatorNameField = new JTextField(14);
    private final JTextField irrigationWsField = new JTextField("0", 6);
    private final JTextField irrigationDurationField = new JTextField("10", 6);
    private final JTextField controlWsField = new JTextField("0", 6);

    private boolean blinkOn;
    private boolean emptyShown;

    CentralFrame(CentralService central, int socketPort, String kafkaHost, int kafkaPort, String dbPath) {
        super("WM_Central - Panel de estaciones");
        this.central = central;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                int option = JOptionPane.showConfirmDialog(CentralFrame.this,
                        "¿Cerrar WM_Central? Se detendrá el servicio de la central.",
                        "Cerrar WM_Central", JOptionPane.YES_NO_OPTION);
                if (option == JOptionPane.YES_OPTION) {
                    dispose();
                    System.exit(0);
                }
            }
        });

        getContentPane().setBackground(BACKGROUND);
        setLayout(new BorderLayout());

        add(header(socketPort, kafkaHost, kafkaPort, dbPath), BorderLayout.NORTH);
        add(centerPanel(), BorderLayout.CENTER);
        add(adminPanel(), BorderLayout.EAST);
        add(southPanel(), BorderLayout.SOUTH);

        emptyLabel.setForeground(MUTED);
        emptyLabel.setFont(new Font("SansSerif", Font.PLAIN, 15));

        Rectangle screen = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(1180, Math.max(900, screen.width - 40)),
                Math.min(760, Math.max(520, screen.height - 40)));
        setMinimumSize(new Dimension(860, 500));
        setLocationRelativeTo(null);

        refresh();
        new Timer(1000, e -> refresh()).start();
        new Timer(500, e -> {
            blinkOn = !blinkOn;
            for (StationCard card : cards.values()) card.paintState(blinkOn);
        }).start();
    }

    // ------------------------------------------------------------------ Layout

    private JComponent header(int socketPort, String kafkaHost, int kafkaPort, String dbPath) {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel title = new JLabel("WM_Central");
        title.setFont(new Font("SansSerif", Font.BOLD, 24));
        title.setForeground(TEXT);

        JLabel subtitle = new JLabel("Socket " + socketPort + "  ·  Kafka " + kafkaHost + ":" + kafkaPort
                + "  ·  SQLite " + dbPath);
        subtitle.setFont(new Font("SansSerif", Font.PLAIN, 12));
        subtitle.setForeground(MUTED);

        JPanel left = new JPanel();
        left.setOpaque(false);
        left.setLayout(new BoxLayout(left, BoxLayout.Y_AXIS));
        left.add(title);
        left.add(Box.createVerticalStrut(3));
        left.add(subtitle);

        summaryLabel.setFont(new Font("SansSerif", Font.BOLD, 14));
        summaryLabel.setForeground(TEXT);
        summaryLabel.setHorizontalAlignment(SwingConstants.RIGHT);

        panel.add(left, BorderLayout.WEST);
        panel.add(summaryLabel, BorderLayout.EAST);
        return panel;
    }

    private JComponent centerPanel() {
        stationsPanel.setLayout(new GridLayout(0, 3, 14, 14));
        stationsPanel.setBackground(BACKGROUND);
        stationsPanel.setBorder(new EmptyBorder(14, 14, 14, 14));

        JScrollPane scroll = new JScrollPane(stationsPanel);
        scroll.setBorder(BorderFactory.createEmptyBorder());
        scroll.getViewport().setBackground(BACKGROUND);
        scroll.getVerticalScrollBar().setUnitIncrement(18);
        return scroll;
    }

    private JComponent adminPanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(12, 12, 12, 12));
        panel.setPreferredSize(new Dimension(340, 0));

        JButton registerButton = new JButton("Registrar operario");
        registerButton.addActionListener(e -> registerOperator());
        JButton listOperatorsButton = new JButton("Ver operarios");
        listOperatorsButton.addActionListener(e -> showOperators());

        JPanel operatorForm = form(
                label("ID:"), operatorIdField,
                label("Nombre:"), operatorNameField);
        JPanel operatorButtons = buttons(registerButton, listOperatorsButton);
        panel.add(section("Operarios", operatorForm, operatorButtons));
        panel.add(Box.createVerticalStrut(10));

        JButton irrigateButton = new JButton("Iniciar riego");
        irrigateButton.addActionListener(e -> startIrrigation());
        JPanel irrigationForm = form(
                label("WS (0 = todas):"), irrigationWsField,
                label("Duración (s):"), irrigationDurationField);
        panel.add(section("Riego administrativo", irrigationForm, buttons(irrigateButton)));
        panel.add(Box.createVerticalStrut(10));

        JButton blockButton = new JButton("Bloquear");
        blockButton.addActionListener(e -> sendControl("BLOCK_WS", "bloqueo"));
        JButton activateButton = new JButton("Activar");
        activateButton.addActionListener(e -> sendControl("ACTIVATE_WS", "activación"));
        JButton stopButton = new JButton("Parar riego");
        stopButton.addActionListener(e -> sendControl("STOP_IRRIGATION", "parada"));
        JPanel controlForm = form(label("WS (0 = todas):"), controlWsField);
        panel.add(section("Control de estaciones", controlForm,
                buttons(blockButton, activateButton, stopButton)));
        panel.add(Box.createVerticalStrut(10));

        JButton refreshButton = new JButton("Actualizar panel");
        refreshButton.addActionListener(e -> {
            refreshNow();
            status("Panel actualizado.");
        });
        panel.add(section("Vista", buttons(refreshButton)));

        panel.add(Box.createVerticalGlue());
        JLabel hint = new JLabel("<html><div style='width:280px'>Las órdenes se entregan a la "
                + "estación en la siguiente comprobación de estado (1 s).</div></html>");
        hint.setForeground(MUTED);
        hint.setFont(new Font("SansSerif", Font.PLAIN, 11));
        hint.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.add(hint);
        return panel;
    }

    private JComponent southPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(2, 10, 4, 10));
        panel.add(legend(), BorderLayout.NORTH);

        statusLabel.setForeground(MUTED);
        statusLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
        statusLabel.setBorder(new EmptyBorder(0, 6, 4, 6));
        panel.add(statusLabel, BorderLayout.SOUTH);
        return panel;
    }

    private JComponent legend() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 4));
        panel.setOpaque(false);
        panel.add(chip("Available", AVAILABLE));
        panel.add(chip("Watering", WATERING_A));
        panel.add(chip("Leak", LEAK));
        panel.add(chip("Out of Service", OUT_OF_SERVICE));
        panel.add(chip("Disconnected", DISCONNECTED));
        return panel;
    }

    private JComponent chip(String text, Color color) {
        JLabel label = new JLabel(" " + text + " ");
        label.setOpaque(true);
        label.setBackground(color);
        label.setForeground(Color.WHITE);
        label.setFont(new Font("SansSerif", Font.BOLD, 11));
        label.setBorder(BorderFactory.createEmptyBorder(2, 6, 2, 6));
        return label;
    }

    private JPanel section(String title, JComponent... contents) {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(0x505050)), title,
                javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.TOP,
                new Font("SansSerif", Font.BOLD, 12), TEXT));
        for (JComponent content : contents) {
            content.setAlignmentX(Component.LEFT_ALIGNMENT);
            panel.add(content);
            panel.add(Box.createVerticalStrut(5));
        }
        panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, panel.getPreferredSize().height));
        return panel;
    }

    private JPanel form(JComponent... pairs) {
        JPanel panel = new JPanel(new GridLayout(0, 2, 6, 4));
        panel.setOpaque(false);
        for (JComponent component : pairs) panel.add(component);
        return panel;
    }

    private JPanel buttons(JButton... buttons) {
        JPanel panel = new JPanel(new GridLayout(1, 0, 6, 0));
        panel.setOpaque(false);
        for (JButton button : buttons) panel.add(button);
        return panel;
    }

    private JLabel label(String text) {
        JLabel label = new JLabel(text);
        label.setForeground(TEXT);
        label.setFont(new Font("SansSerif", Font.PLAIN, 12));
        return label;
    }

    // ------------------------------------------------------------------ Refresco

    private void refreshNow() {
        refresh();
    }

    private void refresh() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refresh);
            return;
        }

        List<CentralService.StationView> stations = central.vistaEstaciones();
        Set<Integer> seen = new HashSet<>();
        int online = 0;
        int watering = 0;

        for (CentralService.StationView station : stations) {
            seen.add(station.wsId);
            if (station.online) online++;
            if ("REGANDO".equalsIgnoreCase(station.state)) watering++;

            StationCard card = cards.get(station.wsId);
            if (card == null) {
                card = new StationCard();
                cards.put(station.wsId, card);
                stationsPanel.add(card);
            }
            card.apply(station);
            card.paintState(blinkOn);
        }

        for (Integer id : new ArrayList<>(cards.keySet())) {
            if (!seen.contains(id)) stationsPanel.remove(cards.remove(id));
        }

        if (stations.isEmpty()) {
            if (!emptyShown) {
                stationsPanel.add(emptyLabel);
                emptyShown = true;
            }
        } else if (emptyShown) {
            stationsPanel.remove(emptyLabel);
            emptyShown = false;
        }

        summaryLabel.setText(stations.size() + " estaciones  ·  " + online + " en línea  ·  "
                + watering + " regando");
        stationsPanel.revalidate();
        stationsPanel.repaint();
    }

    // ------------------------------------------------------------------ Acciones

    private void registerOperator() {
        int id = readInteger(operatorIdField, "ID de operario", true);
        if (id < 0) return;
        String name = operatorNameField.getText().trim();
        if (name.isEmpty()) {
            error("Introduce el nombre del operario.");
            return;
        }
        boolean created = central.registrarOperario(id, name);
        if (created) {
            operatorNameField.setText("");
            status("Operario " + id + " registrado.");
        } else {
            error("No se pudo registrar el operario (¿ID duplicado?).");
        }
    }

    private void showOperators() {
        List<Map<String, Object>> operators = central.listarOperarios();
        DefaultTableModel model = new DefaultTableModel(new Object[]{"ID", "Nombre"}, 0) {
            @Override public boolean isCellEditable(int row, int column) { return false; }
        };
        for (Map<String, Object> operator : operators) {
            model.addRow(new Object[]{operator.get("operatorId"), operator.get("name")});
        }
        JTable table = new JTable(model);
        table.setRowHeight(24);
        JScrollPane scroll = new JScrollPane(table);
        scroll.setPreferredSize(new Dimension(340, Math.max(180, Math.min(420, 40 + operators.size() * 26))));

        JDialog dialog = new JDialog(this, "Operarios registrados", true);
        dialog.setLayout(new BorderLayout());
        dialog.add(scroll, BorderLayout.CENTER);
        JButton close = new JButton("Cerrar");
        close.addActionListener(e -> dialog.dispose());
        JPanel footer = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        footer.add(close);
        dialog.add(footer, BorderLayout.SOUTH);
        dialog.pack();
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private void startIrrigation() {
        int wsId = readInteger(irrigationWsField, "ID de WS", false);
        if (wsId < 0) return;
        int duration = readInteger(irrigationDurationField, "Duración", true);
        if (duration < 0) return;
        if (duration > 86400) {
            error("La duración máxima es 86400 segundos.");
            return;
        }
        boolean queued = central.adminRiego(wsId, duration);
        if (queued) status("Riego encolado para " + (wsId == 0 ? "todas las estaciones conectadas" : "WS " + wsId)
                + " (" + duration + " s).");
        else error("No se pudo iniciar el riego: la estación no está conectada y disponible.");
        refreshNow();
    }

    private void sendControl(String type, String description) {
        int wsId = readInteger(controlWsField, "ID de WS", false);
        if (wsId < 0) return;
        boolean queued = central.adminOrden(type, wsId);
        if (queued) status("Orden de " + description + " encolada para "
                + (wsId == 0 ? "todas las estaciones conectadas" : "WS " + wsId) + ".");
        else error("No hay estaciones conectadas con estado reciente para esa orden.");
        refreshNow();
    }

    private int readInteger(JTextField field, String name, boolean positive) {
        try {
            int value = Integer.parseInt(field.getText().trim());
            if (positive ? value > 0 : value >= 0) return value;
        } catch (NumberFormatException ignored) {
            // Se informa abajo con un mensaje uniforme.
        }
        error(name + ": introduce un entero " + (positive ? "positivo." : "mayor o igual que cero."));
        return -1;
    }

    private void status(String message) {
        statusLabel.setText(message);
    }

    private void error(String message) {
        statusLabel.setText(message);
        JOptionPane.showMessageDialog(this, message, "WM_Central", JOptionPane.WARNING_MESSAGE);
    }

    // ------------------------------------------------------------------ Tarjeta

    private static final class StationCard extends JPanel {
        private final JLabel idLabel = new JLabel();
        private final JLabel locationLabel = new JLabel();
        private final JLabel stateLabel = new JLabel();
        private final JLabel volumeLabel = new JLabel();
        private final JLabel detail1 = new JLabel(" ");
        private final JLabel detail2 = new JLabel(" ");
        private final JLabel detail3 = new JLabel(" ");
        private String state = "DESCONECTADA";
        private String requestId;

        StationCard() {
            super(new BorderLayout(0, 7));
            setOpaque(true);
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(new Color(0, 0, 0, 90), 1, true),
                    new EmptyBorder(14, 16, 14, 16)));

            idLabel.setFont(new Font("SansSerif", Font.BOLD, 16));
            locationLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
            stateLabel.setFont(new Font("SansSerif", Font.BOLD, 23));
            volumeLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
            Font detailFont = new Font("SansSerif", Font.PLAIN, 12);
            detail1.setFont(detailFont);
            detail2.setFont(detailFont);
            detail3.setFont(detailFont);

            for (JLabel label : new JLabel[]{idLabel, locationLabel, stateLabel, volumeLabel,
                    detail1, detail2, detail3}) {
                label.setForeground(Color.WHITE);
            }
            locationLabel.setHorizontalAlignment(SwingConstants.RIGHT);

            JPanel top = new JPanel(new BorderLayout(8, 0));
            top.setOpaque(false);
            top.add(idLabel, BorderLayout.WEST);
            top.add(locationLabel, BorderLayout.CENTER);

            JPanel middle = new JPanel(new BorderLayout(0, 2));
            middle.setOpaque(false);
            middle.add(stateLabel, BorderLayout.NORTH);
            middle.add(volumeLabel, BorderLayout.SOUTH);

            JPanel details = new JPanel(new GridLayout(0, 1, 0, 1));
            details.setOpaque(false);
            details.add(detail1);
            details.add(detail2);
            details.add(detail3);

            add(top, BorderLayout.NORTH);
            add(middle, BorderLayout.CENTER);
            add(details, BorderLayout.SOUTH);
        }

        void apply(CentralService.StationView station) {
            idLabel.setText("WS " + station.wsId);
            locationLabel.setText(station.location == null ? "" : station.location);
            locationLabel.setToolTipText(station.location);
            state = station.state == null ? "DESCONECTADA" : station.state.toUpperCase(Locale.ROOT);
            stateLabel.setText(displayState(state));
            volumeLabel.setText(String.format(Locale.ROOT, "Volumen acumulado: %.2f L", station.volumeLitres));

            switch (state) {
                case "REGANDO":
                    detail1.setText(String.format(Locale.ROOT, "Caudal: %.2f L/min", station.flowLMin));
                    detail2.setText(String.format(Locale.ROOT, "Servicio: %.2f L  ·  Tiempo: %d s",
                            station.serviceLitres, station.elapsedSeconds));
                    detail3.setText("Operario: " + (station.operatorId > 0 ? station.operatorId : "-"));
                    break;
                case "ACTIVADA":
                    detail1.setText("Disponible para riego");
                    detail2.setText(" ");
                    detail3.setText(" ");
                    break;
                case "FUGA":
                    detail1.setText("Fuga/avería detectada");
                    detail2.setText("Riego detenido");
                    detail3.setText(" ");
                    break;
                case "FUERA_SERVICIO":
                    detail1.setText("Estación bloqueada");
                    detail2.setText("No acepta nuevos riegos");
                    detail3.setText(" ");
                    break;
                default:
                    detail1.setText("Sin conexión con el monitor");
                    detail2.setText(" ");
                    detail3.setText(" ");
                    break;
            }
            if (station.pendingControl != null) {
                detail3.setText("Orden pendiente: " + station.pendingControl);
            }
            requestId = station.requestId;
            setToolTipText(station.requestId == null ? null : "requestId: " + station.requestId);
        }

        void paintState(boolean blinkOn) {
            switch (state) {
                case "ACTIVADA": setBackground(AVAILABLE); break;
                case "REGANDO": setBackground(blinkOn ? WATERING_B : WATERING_A); break;
                case "FUGA": setBackground(LEAK); break;
                case "FUERA_SERVICIO": setBackground(OUT_OF_SERVICE); break;
                case "DESCONECTADA": setBackground(DISCONNECTED); break;
                default: setBackground(UNKNOWN); break;
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
    }
}
