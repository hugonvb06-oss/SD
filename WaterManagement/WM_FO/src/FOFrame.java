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
 * avisando cuando dejan de llegar los latidos de la central.
 */
final class FOFrame extends JFrame {

    private static final long PAUSE_BETWEEN_FILE_SERVICES_MS = 4000L;
    private static final long DEFAULT_LIST_TIMEOUT_SECONDS = 15L;
    private static final long DEFAULT_MIN_SERVICE_TIMEOUT_SECONDS = 120L;
    private static final long EXTRA_SERVICE_TIMEOUT_SECONDS = 120L;

    private static final Color BACKGROUND = new Color(0x1E1E1E);
    private static final Color PANEL_BACKGROUND = new Color(0x252526);
    private static final Color TEXT = new Color(0xF2F2F2);
    private static final Color MUTED = new Color(0xBBBBBB);
    private static final Color GREEN = new Color(0x66BB6A);
    private static final Color RED = new Color(0xEF5350);
    private static final Color ORANGE = new Color(0xFFA726);

    private final String ipBroker;
    private final int puertoBroker;
    private final int operatorId;

    private volatile KafkaOperatorClient kafka;
    private volatile boolean connected;
    private volatile boolean busy;
    private volatile boolean connecting;
    private volatile boolean closing;

    private final JLabel kafkaBadge = new JLabel("Kafka: desconectado");
    private final JLabel centralBadge = new JLabel("Central: sin conexión");
    private final JTextArea logArea = new JTextArea(10, 80);
    private final DefaultTableModel stationsModel = new DefaultTableModel(
            new Object[]{"ID WS", "Ubicación", "Estado"}, 0) {
        @Override public boolean isCellEditable(int row, int column) { return false; }
    };

    private final JSpinner wsSpinner = new JSpinner(new SpinnerNumberModel(1, 1, Integer.MAX_VALUE, 1));
    private final JSpinner durationSpinner = new JSpinner(new SpinnerNumberModel(10, 1, 86400, 1));
    private final JTextField fileField = new JTextField();
    private final JButton manualButton = new JButton("Solicitar riego");
    private final JButton fileButton = new JButton("Procesar fichero");
    private final JButton listButton = new JButton("Consultar WS disponibles");
    private final JButton reconnectButton = new JButton("Reconectar");

    FOFrame(String ipBroker, int puertoBroker, int operatorId) {
        super("WM_FO - Operario " + operatorId);
        this.ipBroker = ipBroker;
        this.puertoBroker = puertoBroker;
        this.operatorId = operatorId;

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) {
                closeClient();
                dispose();
                System.exit(0);
            }
        });

        getContentPane().setBackground(BACKGROUND);
        setLayout(new BorderLayout());
        add(header(), BorderLayout.NORTH);
        add(tabs(), BorderLayout.CENTER);
        add(southPanel(), BorderLayout.SOUTH);

        manualButton.addActionListener(e -> requestManual());
        fileButton.addActionListener(e -> processFile());
        listButton.addActionListener(e -> listAvailable());
        reconnectButton.addActionListener(e -> connect());

        Rectangle screen = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        setSize(Math.min(1000, Math.max(760, screen.width - 60)),
                Math.min(720, Math.max(500, screen.height - 60)));
        setMinimumSize(new Dimension(720, 480));
        setLocationRelativeTo(null);

        updateControls();
        connect();
        new Timer(1000, e -> updateCentralStatus()).start();
    }

    // ------------------------------------------------------------------ Layout

    private JComponent header() {
        JPanel panel = new JPanel(new BorderLayout(16, 0));
        panel.setBackground(PANEL_BACKGROUND);
        panel.setBorder(new EmptyBorder(12, 18, 12, 18));

        JLabel title = new JLabel("WM_FO - Operario de campo");
        title.setFont(new Font("SansSerif", Font.BOLD, 22));
        title.setForeground(TEXT);

        JLabel subtitle = new JLabel("Operario " + operatorId + "  ·  Kafka "
                + formatBootstrap(ipBroker, puertoBroker));
        subtitle.setFont(new Font("SansSerif", Font.PLAIN, 12));
        subtitle.setForeground(MUTED);

        JPanel left = new JPanel();
        left.setOpaque(false);
        left.setLayout(new BoxLayout(left, BoxLayout.Y_AXIS));
        left.add(title);
        left.add(Box.createVerticalStrut(3));
        left.add(subtitle);

        kafkaBadge.setFont(new Font("SansSerif", Font.BOLD, 12));
        kafkaBadge.setForeground(MUTED);
        centralBadge.setFont(new Font("SansSerif", Font.BOLD, 12));
        centralBadge.setForeground(MUTED);
        reconnectButton.setEnabled(false);

        JPanel badges = new JPanel();
        badges.setOpaque(false);
        badges.setLayout(new BoxLayout(badges, BoxLayout.Y_AXIS));
        kafkaBadge.setAlignmentX(Component.RIGHT_ALIGNMENT);
        centralBadge.setAlignmentX(Component.RIGHT_ALIGNMENT);
        reconnectButton.setAlignmentX(Component.RIGHT_ALIGNMENT);
        badges.add(kafkaBadge);
        badges.add(Box.createVerticalStrut(3));
        badges.add(centralBadge);
        badges.add(Box.createVerticalStrut(5));
        badges.add(reconnectButton);

        panel.add(left, BorderLayout.WEST);
        panel.add(badges, BorderLayout.EAST);
        return panel;
    }

    private JComponent tabs() {
        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("Riego manual", manualTab());
        tabs.addTab("Servicios desde fichero", fileTab());
        tabs.addTab("WS disponibles", availableTab());
        return tabs;
    }

    private JComponent manualTab() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(BACKGROUND);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        panel.add(row(label("Estación (WS):"), wsSpinner));
        panel.add(row(label("Duración del riego (s):"), durationSpinner));
        panel.add(Box.createVerticalStrut(6));
        panel.add(note("La solicitud se envía a CENTRAL por Kafka. La ventana mostrará los "
                + "eventos RECEIVED, CHECKING, AUTHORIZED, STARTED, PROGRESS y COMPLETED."));
        panel.add(Box.createVerticalStrut(10));
        panel.add(leftRow(manualButton));
        return panel;
    }

    private JComponent fileTab() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(BACKGROUND);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        fileField.setPreferredSize(new Dimension(430, 26));
        JButton browse = new JButton("Examinar...");
        browse.addActionListener(e -> chooseFile());
        panel.add(row(label("Fichero:"), fileField, browse));
        panel.add(Box.createVerticalStrut(6));
        panel.add(note("Formato: una línea por servicio con wsId;duraciónSegundos. "
                + "Se admiten líneas vacías y comentarios que empiecen por #. "
                + "Los servicios se envían en orden con una pausa de 4 segundos."));
        panel.add(Box.createVerticalStrut(10));
        panel.add(leftRow(fileButton));
        return panel;
    }

    private JComponent availableTab() {
        JPanel panel = new JPanel(new BorderLayout(0, 10));
        panel.setBackground(BACKGROUND);
        panel.setBorder(new EmptyBorder(18, 18, 18, 18));

        JTable table = new JTable(stationsModel);
        table.setRowHeight(24);
        JScrollPane scroll = new JScrollPane(table);
        scroll.getViewport().setBackground(Color.WHITE);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        top.setOpaque(false);
        top.add(listButton);

        panel.add(top, BorderLayout.NORTH);
        panel.add(scroll, BorderLayout.CENTER);
        return panel;
    }

    private JComponent southPanel() {
        JPanel south = new JPanel(new BorderLayout(0, 4));
        south.setBackground(PANEL_BACKGROUND);
        south.setBorder(new EmptyBorder(6, 10, 6, 10));

        logArea.setEditable(false);
        logArea.setBackground(new Color(0x111111));
        logArea.setForeground(new Color(0xDCDCDC));
        logArea.setFont(new Font("Consolas", Font.PLAIN, 12));
        logArea.setLineWrap(true);
        logArea.setWrapStyleWord(true);
        JScrollPane scroll = new JScrollPane(logArea);
        scroll.setPreferredSize(new Dimension(0, 220));
        scroll.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(0x505050)), "Registro de eventos",
                javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.TOP,
                new Font("SansSerif", Font.BOLD, 12), TEXT));

        JLabel hint = new JLabel("Consejo: mantén abierta esta ventana para no perder eventos de CENTRAL.");
        hint.setForeground(MUTED);
        hint.setFont(new Font("SansSerif", Font.PLAIN, 11));

        south.add(scroll, BorderLayout.CENTER);
        south.add(hint, BorderLayout.SOUTH);
        return south;
    }

    private JPanel row(JComponent... components) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        for (JComponent component : components) panel.add(component);
        return panel;
    }

    private JPanel leftRow(JComponent component) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        panel.setOpaque(false);
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.add(component);
        return panel;
    }

    private JLabel label(String text) {
        JLabel label = new JLabel(text);
        label.setForeground(TEXT);
        label.setFont(new Font("SansSerif", Font.PLAIN, 13));
        return label;
    }

    private JLabel note(String text) {
        JLabel label = new JLabel("<html><div style='width:620px'>" + text + "</div></html>");
        label.setForeground(MUTED);
        label.setFont(new Font("SansSerif", Font.PLAIN, 12));
        label.setAlignmentX(Component.LEFT_ALIGNMENT);
        return label;
    }

    // ------------------------------------------------------------------ Kafka

    private synchronized void connect() {
        if (connecting || closing) return;
        connecting = true;
        busy = true;
        updateControls();
        setKafkaStatus("Kafka: conectando...", MUTED);

        new Thread(() -> {
            KafkaOperatorClient previous = kafka;
            kafka = null;
            connected = false;
            if (previous != null) {
                try { previous.close(); } catch (Exception ignored) { }
            }
            KafkaOperatorClient client = null;
            try {
                client = new KafkaOperatorClient(
                        formatBootstrap(ipBroker, puertoBroker), operatorId);
                client.start();
                if (closing) {
                    client.close();
                    return;
                }
                kafka = client;
                connected = true;
                log("Kafka conectado. Topics: " + KafkaOperatorClient.TOPIC_SOLICITUDES
                        + " / " + KafkaOperatorClient.TOPIC_RESPUESTAS);
                SwingUtilities.invokeLater(() -> setKafkaStatus("Kafka: conectado", GREEN));
            } catch (Exception e) {
                connected = false;
                if (client != null) {
                    try { client.close(); } catch (Exception ignored) { }
                }
                log("No se pudo iniciar la comunicación Kafka: " + e.getMessage());
                SwingUtilities.invokeLater(() -> setKafkaStatus("Kafka: sin conexión", RED));
            } finally {
                connecting = false;
                busy = false;
                SwingUtilities.invokeLater(this::updateControls);
            }
        }, "wm-fo-gui-connect").start();
    }

    private void closeClient() {
        closing = true;
        KafkaOperatorClient client = kafka;
        kafka = null;
        connected = false;
        if (client != null) {
            try { client.close(); } catch (Exception ignored) { }
        }
    }

    private void updateControls() {
        boolean ready = connected && !busy;
        manualButton.setEnabled(ready);
        fileButton.setEnabled(ready);
        listButton.setEnabled(ready);
        reconnectButton.setEnabled(!busy);
    }

    private void setKafkaStatus(String text, Color color) {
        kafkaBadge.setText(text);
        kafkaBadge.setForeground(color);
    }

    private void updateCentralStatus() {
        boolean alive = connected && !closing && kafka != null && kafka.isCentralAlive();
        if (!connected) {
            centralBadge.setText("Central: sin conexión");
            centralBadge.setForeground(MUTED);
        } else if (alive) {
            centralBadge.setText("Central: CONECTADA");
            centralBadge.setForeground(GREEN);
        } else {
            centralBadge.setText("Central: SIN LATIDO");
            centralBadge.setForeground(RED);
        }
    }

    // ------------------------------------------------------------------ Acciones

    private void requestManual() {
        if (!readyForOperation()) return;
        int wsId = ((Number) wsSpinner.getValue()).intValue();
        int duration = ((Number) durationSpinner.getValue()).intValue();
        beginOperation();
        new Thread(() -> {
            try {
                ejecutarServicio(wsId, duration, "Solicitud manual");
            } finally {
                SwingUtilities.invokeLater(this::endOperation);
            }
        }, "wm-fo-gui-manual").start();
    }

    private void processFile() {
        if (!readyForOperation()) return;
        String pathText = fileField.getText().trim();
        if (pathText.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Indica la ruta del fichero de servicios.",
                    "WM_FO", JOptionPane.WARNING_MESSAGE);
            return;
        }

        final List<Servicio> services;
        try {
            services = readServices(Paths.get(pathText));
        } catch (IOException | IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, "No se ha podido cargar el fichero:\n" + e.getMessage(),
                    "WM_FO", JOptionPane.ERROR_MESSAGE);
            return;
        }
        if (services.isEmpty()) {
            JOptionPane.showMessageDialog(this, "El fichero no contiene servicios.",
                    "WM_FO", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        beginOperation();
        new Thread(() -> {
            try {
                log("Se han validado " + services.size() + " servicios. Comienza el envío secuencial.");
                for (int i = 0; i < services.size(); i++) {
                    Servicio service = services.get(i);
                    log("Servicio " + (i + 1) + "/" + services.size() + " (línea " + service.line
                            + "): WS=" + service.wsId + ", duración=" + service.durationSeconds + " s");
                    Result result = ejecutarServicio(service.wsId, service.durationSeconds,
                            "Servicio de fichero, línea " + service.line);
                    if (result == Result.DESCONOCIDO) {
                        log("Se detiene el procesamiento: no se conoce la conclusión del servicio.");
                        return;
                    }
                    if (i < services.size() - 1) {
                        log("Esperando 4 segundos antes del siguiente servicio...");
                        try {
                            Thread.sleep(PAUSE_BETWEEN_FILE_SERVICES_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log("Procesamiento del fichero interrumpido.");
                            return;
                        }
                    }
                }
                log("Procesamiento del fichero terminado.");
            } finally {
                SwingUtilities.invokeLater(this::endOperation);
            }
        }, "wm-fo-gui-file").start();
    }

    private void listAvailable() {
        if (!readyForOperation()) return;
        beginOperation();
        new Thread(() -> {
            try {
                consultarDisponibles();
            } finally {
                SwingUtilities.invokeLater(this::endOperation);
            }
        }, "wm-fo-gui-list").start();
    }

    private Result ejecutarServicio(int wsId, int durationSeconds, String origin) {
        KafkaOperatorClient client = kafka;
        if (client == null) {
            log("Kafka no está conectado.");
            return Result.DESCONOCIDO;
        }

        final String requestId;
        try {
            log(origin + ". Operario=" + operatorId + ", WS=" + wsId
                    + ", duración=" + durationSeconds + " s");
            requestId = client.solicitarRiego(wsId, durationSeconds);
            log("Solicitud publicada en Kafka. Request ID: " + requestId);
        } catch (Exception e) {
            log("No se pudo publicar la solicitud: " + e.getMessage());
            return Result.DESCONOCIDO;
        }

        try {
            long timeout = serviceTimeoutSeconds(durationSeconds);
            long deadline = System.nanoTime() + Duration.ofSeconds(timeout).toNanos();
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    log("Timeout: han pasado " + timeout
                            + " s sin conclusión. El resultado es desconocido; el riego no se cancela.");
                    return Result.DESCONOCIDO;
                }
                Map<String, Object> event = client.esperarEvento(requestId,
                        Duration.ofNanos(Math.min(remaining, Duration.ofSeconds(1).toNanos())));
                if (event == null) continue;

                log(describe(event));
                String status = text(event.get("status"));
                if (isSuccess(status)) {
                    log("Riego finalizado correctamente.");
                    return Result.EXITO;
                }
                if (isFailure(status)) {
                    log("El servicio ha concluido sin éxito (" + status + ").");
                    return Result.FRACASO;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Espera interrumpida; el resultado del servicio es desconocido.");
            return Result.DESCONOCIDO;
        } catch (Exception e) {
            log("Se ha perdido la capacidad de recibir el resultado: " + e.getMessage());
            return Result.DESCONOCIDO;
        } finally {
            client.finalizarSolicitud(requestId);
        }
    }

    private void consultarDisponibles() {
        KafkaOperatorClient client = kafka;
        if (client == null) {
            log("Kafka no está conectado.");
            return;
        }

        final String requestId;
        try {
            requestId = client.solicitarListadoWsDisponibles();
            log("Consultando estaciones disponibles... Request ID: " + requestId);
        } catch (Exception e) {
            log("No se pudo solicitar el listado a CENTRAL: " + e.getMessage());
            return;
        }

        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(DEFAULT_LIST_TIMEOUT_SECONDS).toNanos();
            while (System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                Map<String, Object> event = client.esperarEvento(requestId,
                        Duration.ofNanos(Math.min(remaining, Duration.ofSeconds(1).toNanos())));
                if (event == null) continue;

                if ("AVAILABLE_WS_LIST".equalsIgnoreCase(text(event.get("type")))
                        || event.get("stations") instanceof List<?>) {
                    final List<Map<String, Object>> stations = toStations(event.get("stations"));
                    SwingUtilities.invokeLater(() -> showStations(stations));
                    return;
                }
                log(describe(event));
                if (isFailure(text(event.get("status")))) {
                    log("CENTRAL no ha podido proporcionar el listado.");
                    return;
                }
            }
            log("CENTRAL no respondió al listado en " + DEFAULT_LIST_TIMEOUT_SECONDS + " segundos.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Consulta de WS interrumpida.");
        } catch (Exception e) {
            log("No se pudo recibir el listado: " + e.getMessage());
        } finally {
            client.finalizarSolicitud(requestId);
        }
    }

    private void showStations(List<Map<String, Object>> stations) {
        stationsModel.setRowCount(0);
        for (Map<String, Object> station : stations) {
            String state = text(station.get("state"));
            Object available = station.get("available");
            boolean isAvailable = Boolean.TRUE.equals(available)
                    || "ACTIVADA".equalsIgnoreCase(state) || "AVAILABLE".equalsIgnoreCase(state);
            if (available != null && !Boolean.TRUE.equals(available)) continue;
            if (available == null && state != null && !isAvailable) continue;

            String id = text(station.containsKey("wsId") ? station.get("wsId") : station.get("id"));
            String location = text(station.get("location"));
            stationsModel.addRow(new Object[]{id == null ? "?" : id,
                    location == null ? "Sin ubicación" : location,
                    state == null ? "ACTIVADA" : state});
            log("WS disponible: " + id + " | " + location + " | " + state);
        }
        if (stationsModel.getRowCount() == 0) {
            log("No hay estaciones disponibles en este momento.");
        }
    }

    private void chooseFile() {
        JFileChooser chooser = new JFileChooser();
        if (!fileField.getText().trim().isEmpty()) {
            chooser.setSelectedFile(Paths.get(fileField.getText().trim()).toFile());
        }
        if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            fileField.setText(chooser.getSelectedFile().getAbsolutePath());
        }
    }

    // ------------------------------------------------------------------ Apoyo

    private boolean readyForOperation() {
        if (!connected || kafka == null) {
            JOptionPane.showMessageDialog(this, "Kafka no está conectado. Pulsa 'Reconectar'.",
                    "WM_FO", JOptionPane.WARNING_MESSAGE);
            return false;
        }
        if (busy) return false;
        return true;
    }

    private void beginOperation() {
        busy = true;
        updateControls();
    }

    private void endOperation() {
        busy = false;
        updateControls();
    }

    private void log(String message) {
        final String line = "[" + LocalTime.now().withNano(0) + "] " + message;
        SwingUtilities.invokeLater(() -> {
            logArea.append(line + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    private static String describe(Map<String, Object> event) {
        StringBuilder line = new StringBuilder("[CENTRAL] ");
        String status = text(event.get("status"));
        line.append(status == null || status.isEmpty() ? "Evento recibido" : status);
        String message = text(event.get("message"));
        if (message != null && !message.isEmpty()) line.append(" - ").append(message);

        Object flow = event.get("flowLMin");
        Object volume = event.get("volumeLitres");
        Object serviceVolume = event.get("serviceVolumeLitres");
        Object elapsed = event.get("elapsedSeconds");
        if (flow != null || volume != null || serviceVolume != null || elapsed != null) {
            line.append(" | Telemetría");
            if (flow != null) line.append(": caudal ").append(decimal(flow)).append(" L/min");
            if (volume != null) line.append(", acumulado ").append(decimal(volume)).append(" L");
            if (serviceVolume != null) line.append(", servicio ").append(decimal(serviceVolume)).append(" L");
            if (elapsed != null) line.append(", tiempo ").append(text(elapsed)).append(" s");
        }
        return line.toString();
    }

    private List<Servicio> readServices(Path path) throws IOException {
        List<Servicio> services = new ArrayList<>();
        int lineNumber = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String content = line.trim();
                if (content.isEmpty() || content.startsWith("#")) continue;

                String[] fields = content.split(";", -1);
                if (fields.length != 2) {
                    throw new IllegalArgumentException("Línea " + lineNumber
                            + ": se esperaba el formato wsId;duracionSegundos.");
                }
                int wsId;
                int duration;
                try {
                    wsId = Integer.parseInt(fields[0].trim());
                    duration = Integer.parseInt(fields[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Línea " + lineNumber
                            + ": el ID de WS y la duración deben ser números enteros.");
                }
                if (wsId <= 0) {
                    throw new IllegalArgumentException("Línea " + lineNumber + ": el ID de WS debe ser positivo.");
                }
                if (duration <= 0 || duration > 86400) {
                    throw new IllegalArgumentException("Línea " + lineNumber
                            + ": la duración debe estar entre 1 y 86400 segundos.");
                }
                services.add(new Servicio(wsId, duration, lineNumber));
            }
        }
        return services;
    }

    private static long serviceTimeoutSeconds(int durationSeconds) {
        long fallback = Math.max(DEFAULT_MIN_SERVICE_TIMEOUT_SECONDS,
                (long) durationSeconds + EXTRA_SERVICE_TIMEOUT_SECONDS);
        String configured = System.getenv("WM_FO_TIMEOUT_SECONDS");
        if (configured == null || configured.trim().isEmpty()) return fallback;
        try {
            long value = Long.parseLong(configured.trim());
            return value > 0 ? value : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static String formatBootstrap(String host, int port) {
        if (host.contains(":") && !host.startsWith("[")) return "[" + host + "]:" + port;
        return host + ":" + port;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> toStations(Object value) {
        List<Map<String, Object>> stations = new ArrayList<>();
        if (!(value instanceof List<?>)) return stations;
        for (Object item : (List<?>) value) {
            if (!(item instanceof Map<?, ?>)) continue;
            Map<String, Object> station = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                station.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            stations.add(station);
        }
        return stations;
    }

    private static boolean isSuccess(String status) {
        String value = status == null ? "" : status.trim().toUpperCase(Locale.ROOT);
        return value.equals("COMPLETED") || value.equals("COMPLETADO")
                || value.equals("FINALIZADO") || value.equals("SUCCESS")
                || value.equals("EXITO") || value.equals("ÉXITO");
    }

    private static boolean isFailure(String status) {
        String value = status == null ? "" : status.trim().toUpperCase(Locale.ROOT);
        return value.equals("REJECTED") || value.equals("RECHAZADO")
                || value.equals("FAILED") || value.equals("FALLO")
                || value.equals("ERROR") || value.equals("CANCELLED")
                || value.equals("CANCELADO") || value.equals("FRACASO")
                || value.equals("DENEGADO") || value.equals("RECHAZADA");
    }

    private static String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String decimal(Object value) {
        try {
            double number = value instanceof Number
                    ? ((Number) value).doubleValue()
                    : Double.parseDouble(String.valueOf(value));
            return String.format(Locale.ROOT, "%.2f", number);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private enum Result { EXITO, FRACASO, DESCONOCIDO }

    private static final class Servicio {
        final int wsId;
        final int durationSeconds;
        final int line;

        Servicio(int wsId, int durationSeconds, int line) {
            this.wsId = wsId;
            this.durationSeconds = durationSeconds;
            this.line = line;
        }
    }
}
