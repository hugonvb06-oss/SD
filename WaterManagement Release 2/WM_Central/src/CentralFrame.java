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
