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
