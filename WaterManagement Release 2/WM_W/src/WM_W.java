import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WM_W - Weather Control Office (WaterManagement Release 2).
 *
 * Responsabilidades:
 *  - Mantener una lista de ciudades modificable durante la ejecucion.
 *  - Consultar OpenWeather Current Weather Data cada cuatro segundos por ciudad.
 *  - Considerar adversas las condiciones con temperatura < 0 C o lluvia.
 *  - Notificar a API_Central cuando una ciudad entra en alerta y cuando se recupera.
 *
 * Java 11. Configuracion mediante variables de entorno; no contiene API keys ni
 * direcciones privadas codificadas en el fuente.
 */
public final class WM_W {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private WM_W() { }

    public static void main(String[] args) {
        String apiKey = env("WM_OPENWEATHER_API_KEY", env("OPENWEATHER_API_KEY", "")).trim();
        if (apiKey.isEmpty()) {
            System.err.println("Falta WM_OPENWEATHER_API_KEY. Configure su API key de OpenWeather antes de arrancar WM_W.");
            return;
        }

        long intervalMs;
        try {
            intervalMs = Long.parseLong(env("WM_WEATHER_INTERVAL_MS", "4000").trim());
            if (intervalMs < 1000L) {
                throw new NumberFormatException("El intervalo minimo admitido es 1000 ms.");
            }
        } catch (NumberFormatException e) {
            System.err.println("WM_WEATHER_INTERVAL_MS debe ser un entero >= 1000 (milisegundos). Se esperaba 4000 por defecto.");
            return;
        }

        Path citiesFile = Paths.get(env("WM_W_CITIES_FILE", "config/cities.json")).toAbsolutePath().normalize();
        String centralBaseUrl = env("WM_API_CENTRAL_URL", "http://localhost:8080").trim();
        String centralAlertPath = env("WM_API_CENTRAL_ALERT_PATH", "/api/alerta").trim();

        try {
            OpenWeatherClient openWeather = new OpenWeatherClient(apiKey);
            CityRepository repository = new CityRepository(citiesFile);
            List<WeatherLocation> initialLocations = repository.load();

            // Si el fichero contiene ciudades sin coordenadas, se resuelven al arrancar.
            List<WeatherLocation> resolved = new ArrayList<>();
            boolean changed = false;
            for (WeatherLocation location : initialLocations) {
                if (location == null || location.getQuery() == null || location.getQuery().trim().isEmpty()) {
                    System.err.println("Se ignora una ciudad vacia o mal formada en " + citiesFile);
                    changed = true;
                    continue;
                }
                if (!location.hasCoordinates()) {
                    try {
                        location = openWeather.resolveCity(location.getQuery());
                        changed = true;
                    } catch (Exception e) {
                        System.err.println("No se pudo resolver '" + location.getQuery() + "': " + e.getMessage());
                        continue;
                    }
                }
                resolved.add(location);
            }
            if (changed) repository.save(resolved);

            CentralNotifier notifier = new CentralNotifier(centralBaseUrl, centralAlertPath);
            WeatherMonitor monitor = new WeatherMonitor(openWeather, notifier, intervalMs);
            monitor.replaceLocations(resolved);

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "wm-w-weather-poller");
                thread.setDaemon(true);
                return thread;
            });
            scheduler.scheduleAtFixedRate(monitor::pollAll, 0L, intervalMs, TimeUnit.MILLISECONDS);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdownNow();
                System.out.println("WM_W detenido.");
            }, "wm-w-shutdown"));

            System.out.println("====================================================");
            System.out.println("WM_W - WEATHER CONTROL OFFICE");
            System.out.println("OpenWeather Current Weather Data: habilitado");
            System.out.println("Intervalo de consulta: " + intervalMs + " ms");
            System.out.println("API_Central: " + centralBaseUrl + centralAlertPath);
            System.out.println("Fichero de ciudades: " + citiesFile);
            System.out.println("Ciudades cargadas: " + resolved.size());
            System.out.println("====================================================");
            runMenu(openWeather, repository, monitor, scheduler);
        } catch (IOException e) {
            System.err.println("No se pudo iniciar WM_W: " + e.getMessage());
        }
    }

    private static void runMenu(OpenWeatherClient openWeather, CityRepository repository,
                                WeatherMonitor monitor, ScheduledExecutorService scheduler) {
        try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name())) {
            boolean running = true;
            while (running) {
                printMenu();
                System.out.print("Opcion: ");
                if (!scanner.hasNextLine()) break;
                String option = scanner.nextLine().trim();
                try {
                    switch (option) {
                        case "1":
                            showLocations(monitor.getLocations());
                            break;
                        case "2":
                            addLocation(scanner, openWeather, repository, monitor);
                            break;
                        case "3":
                            modifyLocation(scanner, openWeather, repository, monitor);
                            break;
                        case "4":
                            removeLocation(scanner, repository, monitor);
                            break;
                        case "5":
                            System.out.println("Intervalo configurado: " + env("WM_WEATHER_INTERVAL_MS", "4000") + " ms");
                            System.out.println("API_Central: " + env("WM_API_CENTRAL_URL", "http://localhost:8080")
                                    + env("WM_API_CENTRAL_ALERT_PATH", "/api/alerta"));
                            System.out.println("Las consultas y notificaciones siguen ejecutandose en segundo plano.");
                            break;
                        case "0":
                            running = false;
                            break;
                        default:
                            System.out.println("Opcion no valida.");
                    }
                } catch (Exception e) {
                    System.err.println("No se pudo completar la operacion: " + e.getMessage());
                }
            }
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("--------------- MENU WM_W ----------------");
        System.out.println("1. Listar ciudades monitorizadas");
        System.out.println("2. Anadir una ciudad");
        System.out.println("3. Modificar una ciudad");
        System.out.println("4. Eliminar una ciudad");
        System.out.println("5. Mostrar configuracion");
        System.out.println("0. Salir");
        System.out.println("-------------------------------------------");
    }

    private static void showLocations(List<WeatherLocation> locations) {
        if (locations.isEmpty()) {
            System.out.println("No hay ciudades configuradas. Utilice la opcion 2 para anadir una.");
            return;
        }
        for (int i = 0; i < locations.size(); i++) {
            WeatherLocation location = locations.get(i);
            System.out.printf(Locale.ROOT, "%d. %s | lat=%.5f | lon=%.5f%n",
                    i + 1, location.displayName(), location.getLatitude(), location.getLongitude());
        }
    }

    private static void addLocation(Scanner scanner, OpenWeatherClient openWeather,
                                    CityRepository repository, WeatherMonitor monitor) throws IOException, InterruptedException {
        System.out.print("Ciudad (recomendado: Ciudad, codigo de pais; ej. Alicante, ES): ");
        if (!scanner.hasNextLine()) return;
        String query = scanner.nextLine().trim();
        if (query.isEmpty()) {
            System.out.println("La ciudad no puede estar vacia.");
            return;
        }
        WeatherLocation location = openWeather.resolveCity(query);
        List<WeatherLocation> locations = new ArrayList<>(monitor.getLocations());
        if (containsQuery(locations, location.getQuery())) {
            System.out.println("Esa ciudad ya esta monitorizada.");
            return;
        }
        locations.add(location);
        repository.save(locations);
        monitor.replaceLocations(locations);
        System.out.println("Ciudad anadida: " + location.displayName() + ". Se consultara en el siguiente ciclo.");
    }

    private static void modifyLocation(Scanner scanner, OpenWeatherClient openWeather,
                                       CityRepository repository, WeatherMonitor monitor) throws IOException, InterruptedException {
        List<WeatherLocation> locations = new ArrayList<>(monitor.getLocations());
        if (locations.isEmpty()) {
            System.out.println("No hay ciudades que modificar.");
            return;
        }
        showLocations(locations);
        System.out.print("Numero de ciudad que desea modificar: ");
        if (!scanner.hasNextLine()) return;
        int index = parseIndex(scanner.nextLine(), locations.size());
        if (index < 0) {
            System.out.println("Numero no valido.");
            return;
        }
        System.out.print("Nueva ciudad (ej. Valencia, ES): ");
        if (!scanner.hasNextLine()) return;
        String query = scanner.nextLine().trim();
        if (query.isEmpty()) {
            System.out.println("La ciudad no puede estar vacia.");
            return;
        }
        WeatherLocation replacement = openWeather.resolveCity(query);
        for (int i = 0; i < locations.size(); i++) {
            if (i != index && sameQuery(locations.get(i).getQuery(), replacement.getQuery())) {
                System.out.println("La ciudad indicada ya esta en la lista.");
                return;
            }
        }
        locations.set(index, replacement);
        repository.save(locations);
        monitor.replaceLocations(locations);
        System.out.println("Ciudad modificada: " + replacement.displayName());
    }

    private static void removeLocation(Scanner scanner, CityRepository repository,
                                       WeatherMonitor monitor) throws IOException {
        List<WeatherLocation> locations = new ArrayList<>(monitor.getLocations());
        if (locations.isEmpty()) {
            System.out.println("No hay ciudades que eliminar.");
            return;
        }
        showLocations(locations);
        System.out.print("Numero de ciudad que desea eliminar: ");
        if (!scanner.hasNextLine()) return;
        int index = parseIndex(scanner.nextLine(), locations.size());
        if (index < 0) {
            System.out.println("Numero no valido.");
            return;
        }
        WeatherLocation removed = locations.remove(index);
        repository.save(locations);
        monitor.replaceLocations(locations);
        System.out.println("Ciudad eliminada: " + removed.displayName());
    }

    private static int parseIndex(String value, int size) {
        try {
            int parsed = Integer.parseInt(value.trim()) - 1;
            return parsed >= 0 && parsed < size ? parsed : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static boolean containsQuery(List<WeatherLocation> locations, String query) {
        for (WeatherLocation location : locations) {
            if (sameQuery(location.getQuery(), query)) return true;
        }
        return false;
    }

    private static boolean sameQuery(String first, String second) {
        return first != null && second != null
                && first.trim().equalsIgnoreCase(second.trim());
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

    /** Persistencia JSON de la lista de ciudades y sus coordenadas resueltas. */
    private static final class CityRepository {
        private final Path file;

        private CityRepository(Path file) {
            this.file = file;
        }

        private synchronized List<WeatherLocation> load() throws IOException {
            if (!Files.exists(file)) {
                Path parent = file.getParent();
                if (parent != null) Files.createDirectories(parent);
                JSON.writeValue(file.toFile(), Collections.emptyList());
                return new ArrayList<>();
            }
            if (Files.size(file) == 0L) return new ArrayList<>();
            List<WeatherLocation> result = JSON.readValue(file.toFile(), new TypeReference<List<WeatherLocation>>() { });
            return result == null ? new ArrayList<>() : new ArrayList<>(result);
        }

        private synchronized void save(List<WeatherLocation> locations) throws IOException {
            Path parent = file.getParent();
            if (parent != null) Files.createDirectories(parent);
            Path temporary = file.resolveSibling(file.getFileName().toString() + ".tmp");
            JSON.writeValue(temporary.toFile(), locations);
            try {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    /** Cliente HTTP de OpenWeather: geocoding al modificar ciudades y Current Weather al monitorizar. */
    private static final class OpenWeatherClient {
        private static final String GEO_URL = "https://api.openweathermap.org/geo/1.0/direct";
        private static final String WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather";
        private final String apiKey;
        private final HttpClient client;

        private OpenWeatherClient(String apiKey) {
            this.apiKey = apiKey;
            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(6))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
        }

        private WeatherLocation resolveCity(String query) throws IOException, InterruptedException {
            String url = GEO_URL + "?q=" + encode(query) + "&limit=1&appid=" + encode(apiKey);
            JsonNode results = getJson(url, "Geocoding OpenWeather");
            if (!results.isArray() || results.size() == 0) {
                throw new IOException("OpenWeather no encuentra la ciudad '" + query
                        + "'. Pruebe con 'Ciudad, codigo de pais', por ejemplo 'Alicante, ES'.");
            }
            JsonNode selected = results.get(0);
            double lat = requiredDouble(selected, "lat");
            double lon = requiredDouble(selected, "lon");
            String name = text(selected, "name", query);
            String country = text(selected, "country", "");
            String state = text(selected, "state", "");
            return new WeatherLocation(query.trim(), name, country, state, lat, lon);
        }

        private WeatherReading currentWeather(WeatherLocation location) throws IOException, InterruptedException {
            String url = WEATHER_URL + "?lat=" + location.getLatitude()
                    + "&lon=" + location.getLongitude()
                    + "&appid=" + encode(apiKey)
                    + "&units=metric&lang=es";
            JsonNode root = getJson(url, "Current Weather Data");
            JsonNode main = root.path("main");
            if (!main.has("temp") || !main.get("temp").isNumber()) {
                throw new IOException("La respuesta de OpenWeather no contiene main.temp numerico.");
            }
            double temperature = main.get("temp").asDouble();
            if (!Double.isFinite(temperature)) {
                throw new IOException("OpenWeather ha devuelto una temperatura no valida.");
            }

            JsonNode weather = root.path("weather");
            String weatherMain = weather.isArray() && weather.size() > 0
                    ? text(weather.get(0), "main", "Unknown") : "Unknown";
            String description = weather.isArray() && weather.size() > 0
                    ? text(weather.get(0), "description", "") : "";
            double rainMm = root.path("rain").path("1h").asDouble(0.0);
            String mainLower = weatherMain.toLowerCase(Locale.ROOT);
            boolean rain = rainMm > 0.0 || mainLower.equals("rain")
                    || mainLower.equals("drizzle") || mainLower.equals("thunderstorm");
            boolean freezing = temperature < 0.0;
            boolean adverse = freezing || rain;
            String reason;
            if (freezing && rain) reason = "FREEZING_AND_RAIN";
            else if (freezing) reason = "FREEZING";
            else if (rain) reason = "RAIN";
            else reason = "NONE";

            return new WeatherReading(temperature, weatherMain, description, rain, rainMm,
                    freezing, adverse, reason, Instant.now().toString());
        }

        private JsonNode getJson(String url, String operation) throws IOException, InterruptedException {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .header("User-Agent", "WaterManagement-WM_W/2.0")
                    .GET().build();
            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                String body = response.body() == null ? "" : response.body();
                if (body.length() > 300) body = body.substring(0, 300);
                throw new IOException(operation + " respondio HTTP " + response.statusCode() + ": " + body);
            }
            try {
                return JSON.readTree(response.body());
            } catch (Exception e) {
                throw new IOException(operation + " devolvio JSON no valido.", e);
            }
        }

        private static String encode(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        }

        private static double requiredDouble(JsonNode node, String field) throws IOException {
            JsonNode value = node.get(field);
            if (value == null || !value.isNumber() || !Double.isFinite(value.asDouble())) {
                throw new IOException("Respuesta de geocoding sin coordenada valida: " + field);
            }
            return value.asDouble();
        }

        private static String text(JsonNode node, String field, String fallback) {
            JsonNode value = node.get(field);
            return value == null || value.isNull() ? fallback : value.asText(fallback);
        }
    }

    /** Envia los cambios de estado meteorologico al endpoint REST de API_Central. */
    private static final class CentralNotifier {
        private final String endpoint;
        private final HttpClient client;

        private CentralNotifier(String baseUrl, String alertPath) {
            String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
            String path = alertPath.startsWith("/") ? alertPath : "/" + alertPath;
            this.endpoint = base + path;
            this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        }

        private boolean notify(WeatherLocation location, WeatherReading reading, boolean active) {
            // Contrato real de API_Central (ApiCentral.java): PUT /api/alerta
            // { ciudad, alerta, motivo, temperatura, lluvia }
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("ciudad", location.getQuery());
            event.put("alerta", active);
            event.put("motivo", active ? reading.reason : "NONE");
            event.put("temperatura", reading.temperatureC);
            event.put("lluvia", reading.rain);

            try {
                String body = JSON.writeValueAsString(event);
                HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                        .timeout(Duration.ofSeconds(7))
                        .header("Content-Type", "application/json; charset=utf-8")
                        .header("Accept", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build();
                HttpResponse<String> response = client.send(request,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    System.out.println("[API_Central] " + (active ? "Alerta enviada" : "Cancelacion enviada")
                            + " para " + location.displayName() + " (HTTP " + response.statusCode() + ").");
                    return true;
                }
                System.err.println("[API_Central] HTTP " + response.statusCode() + " al notificar "
                        + location.displayName() + ". Se reintentara en el siguiente ciclo.");
            } catch (Exception e) {
                System.err.println("[API_Central] No se pudo notificar " + location.displayName()
                        + ": " + e.getMessage() + ". Se reintentara en el siguiente ciclo.");
            }
            return false;
        }
    }

    /** Consulta las ciudades y notifica unicamente cambios de estado para no inundar API_Central. */
    private static final class WeatherMonitor {
        private final OpenWeatherClient openWeather;
        private final CentralNotifier notifier;
        private final long intervalMs;
        private final CopyOnWriteArrayList<WeatherLocation> locations = new CopyOnWriteArrayList<>();
        private final Map<String, Boolean> lastNotifiedAlert = new ConcurrentHashMap<>();

        private WeatherMonitor(OpenWeatherClient openWeather, CentralNotifier notifier, long intervalMs) {
            this.openWeather = openWeather;
            this.notifier = notifier;
            this.intervalMs = intervalMs;
        }

        private void replaceLocations(List<WeatherLocation> newLocations) {
            locations.clear();
            locations.addAll(newLocations);
            List<String> activeKeys = new ArrayList<>();
            for (WeatherLocation location : newLocations) activeKeys.add(key(location));
            lastNotifiedAlert.keySet().retainAll(activeKeys);
        }

        private List<WeatherLocation> getLocations() {
            return new ArrayList<>(locations);
        }

        private void pollAll() {
            List<WeatherLocation> snapshot = getLocations();
            if (snapshot.isEmpty()) return;
            long started = System.currentTimeMillis();
            for (WeatherLocation location : snapshot) {
                try {
                    WeatherReading reading = openWeather.currentWeather(location);
                    System.out.printf(Locale.ROOT, "[WM_W] %s | %.1f C | %s | lluvia=%s | alerta=%s%n",
                            location.displayName(), reading.temperatureC, reading.weatherMain,
                            reading.rain ? "SI" : "NO", reading.adverse ? "SI" : "NO");
                    updateAlertState(location, reading);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    System.err.println("[WM_W] Error consultando " + location.displayName()
                            + ": " + e.getMessage() + ". Se conserva el ultimo estado conocido.");
                }
            }
            long elapsed = System.currentTimeMillis() - started;
            if (elapsed > intervalMs) {
                System.err.println("[WM_W] Aviso: el ciclo de " + snapshot.size() + " ciudad(es) ha tardado "
                        + elapsed + " ms; el siguiente ciclo empezara cuando termine el actual.");
            }
        }

        private void updateAlertState(WeatherLocation location, WeatherReading reading) {
            String key = key(location);
            Boolean previous = lastNotifiedAlert.get(key);
            if (reading.adverse) {
                if (previous == null || !previous) {
                    if (notifier.notify(location, reading, true)) lastNotifiedAlert.put(key, true);
                }
            } else if (previous == null || previous) {
                // En el primer ciclo se envia tambien el estado favorable para que
                // API_Central pueda reconciliar alertas antiguas tras un reinicio.
                if (notifier.notify(location, reading, false)) lastNotifiedAlert.put(key, false);
            }
        }

        private static String key(WeatherLocation location) {
            return location.getQuery().trim().toLowerCase(Locale.ROOT);
        }
    }

    /** DTO persistido en config/cities.json. */
    public static final class WeatherLocation {
        private String query;
        private String name;
        private String country;
        private String state;
        private double latitude;
        private double longitude;
        private boolean coordinatesSet;

        public WeatherLocation() { }

        private WeatherLocation(String query, String name, String country, String state,
                                double latitude, double longitude) {
            this.query = query;
            this.name = name;
            this.country = country;
            this.state = state;
            this.latitude = latitude;
            this.longitude = longitude;
            this.coordinatesSet = Double.isFinite(latitude) && Double.isFinite(longitude);
        }

        public String getQuery() { return query; }
        public void setQuery(String query) { this.query = query; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getCountry() { return country; }
        public void setCountry(String country) { this.country = country; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; this.coordinatesSet = true; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; this.coordinatesSet = true; }
        public boolean isCoordinatesSet() { return coordinatesSet; }
        public void setCoordinatesSet(boolean coordinatesSet) { this.coordinatesSet = coordinatesSet; }
        public boolean hasCoordinates() {
            return coordinatesSet && Double.isFinite(latitude) && Double.isFinite(longitude)
                    && latitude >= -90.0 && latitude <= 90.0 && longitude >= -180.0 && longitude <= 180.0;
        }
        public String displayName() {
            StringBuilder result = new StringBuilder(name == null || name.trim().isEmpty() ? query : name);
            if (state != null && !state.trim().isEmpty()) result.append(", ").append(state);
            if (country != null && !country.trim().isEmpty()) result.append(", ").append(country);
            return result.toString();
        }
    }

    private static final class WeatherReading {
        private final double temperatureC;
        private final String weatherMain;
        private final String description;
        private final boolean rain;
        private final double rainMmLastHour;
        private final boolean freezing;
        private final boolean adverse;
        private final String reason;
        private final String observedAt;

        private WeatherReading(double temperatureC, String weatherMain, String description,
                               boolean rain, double rainMmLastHour, boolean freezing,
                               boolean adverse, String reason, String observedAt) {
            this.temperatureC = temperatureC;
            this.weatherMain = weatherMain;
            this.description = description;
            this.rain = rain;
            this.rainMmLastHour = rainMmLastHour;
            this.freezing = freezing;
            this.adverse = adverse;
            this.reason = reason;
            this.observedAt = observedAt;
        }
    }
}
