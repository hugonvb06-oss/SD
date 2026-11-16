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
