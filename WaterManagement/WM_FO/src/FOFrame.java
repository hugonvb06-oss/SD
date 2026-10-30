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
