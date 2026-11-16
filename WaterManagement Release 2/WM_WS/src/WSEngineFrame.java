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
