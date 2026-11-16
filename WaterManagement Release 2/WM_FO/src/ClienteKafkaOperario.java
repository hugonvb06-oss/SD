import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cliente Kafka de WM_FO.
 *
 * Contrato acordado con WM_Central (ver README.md):
 * - wm.riego.solicitudes: solicitudes de riego, listados y consultas de estado.
 * - wm.riego.respuestas: eventos de avance/finalización y respuestas a las consultas.
 *
 * Cada solicitud tiene un requestId UUID. El consumidor Kafka se ejecuta en un
 * hilo independiente; la interfaz nunca accede directamente al KafkaConsumer, que
 * no es thread-safe.
 *
 * Las solicitudes de riego se conservan en un fichero local mientras no se conozca
 * su conclusión, de modo que si el operario se cierra durante un servicio, al
 * recuperarse puede volver a preguntar su estado a CENTRAL (CONSULTAR_ESTADO) y
 * conocer el resultado sin perder ninguna transacción.
 */
public final class ClienteKafkaOperario implements AutoCloseable {

    public static final String TOPICO_SOLICITUDES = "wm.riego.solicitudes";
    public static final String TOPICO_RESPUESTAS = "wm.riego.respuestas";
    public static final String TOPICO_LATIDO = "wm.central.latido";

    private static final long VENTANA_CENTRAL_VIVA_MS = 8_000L;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TIPO_MAPA =
            new TypeReference<Map<String, Object>>() { };

    private static final int PARTICIONES_TEMA = 3;
    private static final short FACTOR_REPLICACION_TEMA = 1;
    private static final long TIMEOUT_ENVIO_SEGUNDOS = 35;
    private static final long TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS = 30;

    private final String servidoresKafka;
    private final int idOperario;
    private final String idSesion = UUID.randomUUID().toString();
    private final String idGrupoConsumidor;
    private final KafkaProducer<String, String> productor;

    private final AtomicBoolean activo = new AtomicBoolean(false);
    private final AtomicReference<Throwable> falloConsumidor = new AtomicReference<>();
    private final CountDownLatch consumidorListo = new CountDownLatch(1);
    private final Map<String, BlockingQueue<Map<String, Object>>> solicitudesPendientes =
            new ConcurrentHashMap<>();

    private volatile KafkaConsumer<String, String> consumidor;
    private volatile Thread hiloConsumidor;
    private volatile boolean iniciado;
    private volatile boolean cerrado;
    private volatile long ultimoLatidoMillis;
    private volatile long iniciadoEnMillis;

    public ClienteKafkaOperario(String servidoresKafka, int idOperario) {
        if (servidoresKafka == null || servidoresKafka.trim().isEmpty()) {
            throw new IllegalArgumentException("La dirección del broker no puede estar vacía.");
        }
        if (idOperario <= 0) {
            throw new IllegalArgumentException("El ID del operario debe ser un entero positivo.");
        }

        this.servidoresKafka = servidoresKafka.trim();
        this.idOperario = idOperario;
        this.idGrupoConsumidor = "wm-fo-" + idOperario + "-" + idSesion;
        this.productor = new KafkaProducer<>(crearConfiguracionProductor());
    }

    /**
     * Comprueba/crea los topics, arranca el consumidor y espera a que esté
     * asignado al topic de respuestas antes de permitir publicar peticiones.
     */
    public synchronized void iniciar() throws Exception {
        if (cerrado) {
            throw new IllegalStateException("Este cliente Kafka ya está cerrado.");
        }
        if (iniciado) {
            return;
        }

        if (gestionTopicsHabilitada()) asegurarTopics();
        activo.set(true);
        hiloConsumidor = new Thread(this::bucleConsumo, "wm-fo-kafka-consumidor-" + idOperario);
        hiloConsumidor.setDaemon(true);
        hiloConsumidor.start();

        boolean listo = consumidorListo.await(TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS, TimeUnit.SECONDS);
        Throwable fallo = falloConsumidor.get();
        if (!listo || fallo != null || !hiloConsumidor.isAlive()) {
            cerrar();
            if (fallo != null) {
                throw new IllegalStateException("El consumidor Kafka no pudo arrancar: "
                        + fallo.getMessage(), fallo);
            }
            throw new TimeoutException("Kafka no asignó el topic de respuestas en "
                    + TIMEOUT_LISTO_CONSUMIDOR_SEGUNDOS + " segundos. Comprueba el broker y los topics.");
        }

        iniciado = true;
        iniciadoEnMillis = System.currentTimeMillis();
        System.out.println("Kafka conectado. Consumidor listo para recibir respuestas.");
        System.out.println("Topics: " + TOPICO_SOLICITUDES + " / " + TOPICO_RESPUESTAS + " / " + TOPICO_LATIDO);
    }

    /**
     * Indica si WM_Central sigue viva según los latidos publicados por Kafka.
     * Permite avisar al operario aunque el broker de Kafka siga funcionando.
     */
    public boolean centralViva() {
        if (!iniciado || cerrado) return false;
        long referencia = Math.max(ultimoLatidoMillis, iniciadoEnMillis);
        return System.currentTimeMillis() - referencia <= VENTANA_CENTRAL_VIVA_MS;
    }

    public String solicitarRiego(int idWs, int duracionSegundos) throws Exception {
        if (idWs <= 0) {
            throw new IllegalArgumentException("El ID de la WS debe ser positivo.");
        }
        if (duracionSegundos <= 0 || duracionSegundos > 86400) {
            throw new IllegalArgumentException("La duración debe estar entre 1 y 86400 segundos.");
        }

        String idSolicitud = UUID.randomUUID().toString();
        Map<String, Object> carga = new HashMap<>();
        carga.put("type", "RIEGO_REQUEST");
        carga.put("requestId", idSolicitud);
        carga.put("operatorId", idOperario);
        carga.put("wsId", idWs);
        carga.put("durationSeconds", duracionSegundos);
        carga.put("createdAt", Instant.now().toString());

        publicarSolicitud(idSolicitud, carga);
        persistirPendiente(idSolicitud, idWs, duracionSegundos);
        return idSolicitud;
    }

    public String solicitarListadoWsDisponibles() throws Exception {
        String idSolicitud = UUID.randomUUID().toString();
        Map<String, Object> carga = new HashMap<>();
        carga.put("type", "LIST_AVAILABLE_WS");
        carga.put("requestId", idSolicitud);
        carga.put("operatorId", idOperario);
        carga.put("createdAt", Instant.now().toString());

        publicarSolicitud(idSolicitud, carga);
        return idSolicitud;
    }

    /**
     * Pregunta a CENTRAL el estado de una solicitud previa (p. ej. un riego que
     * quedó pendiente cuando el operario se cerró). La respuesta llega con el
     * mismo requestId, por lo que se correlaciona con {@link #esperarEvento}.
     */
    public void solicitarEstado(String idSolicitud) throws Exception {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = new ArrayBlockingQueue<>(1000);
        solicitudesPendientes.put(idSolicitud, cola);
        try {
            Map<String, Object> carga = new HashMap<>();
            carga.put("type", "CONSULTAR_ESTADO");
            carga.put("requestId", idSolicitud);
            carga.put("operatorId", idOperario);
            carga.put("createdAt", Instant.now().toString());
            String textoJson = JSON.writeValueAsString(carga);
            productor.send(new ProducerRecord<>(TOPICO_SOLICITUDES, Integer.toString(idOperario), textoJson))
                    .get(TIMEOUT_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
            System.out.println("Consulta de estado publicada en Kafka. ID: " + idSolicitud);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("Se interrumpió el envío de la consulta a Kafka.", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("No se pudo confirmar el envío de la consulta a Kafka: "
                    + causaRaiz(e).getMessage(), e);
        } catch (IOException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw e;
        }
    }

    private void publicarSolicitud(String idSolicitud, Map<String, Object> carga) throws Exception {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = new ArrayBlockingQueue<>(1000);
        solicitudesPendientes.put(idSolicitud, cola);

        try {
            String textoJson = JSON.writeValueAsString(carga);
            ProducerRecord<String, String> registro = new ProducerRecord<>(
                    TOPICO_SOLICITUDES,
                    Integer.toString(idOperario),
                    textoJson);

            productor.send(registro).get(TIMEOUT_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
            System.out.println("Solicitud publicada en Kafka. ID: " + idSolicitud);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("Se interrumpió el envío a Kafka.", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw new IOException("No se pudo confirmar el envío a Kafka: "
                    + causaRaiz(e).getMessage()
                    + ". El resultado del envío puede ser incierto si se agotó el tiempo de espera.", e);
        } catch (IOException e) {
            solicitudesPendientes.remove(idSolicitud);
            throw e;
        }
    }

    /** Devuelve null al agotar el timeout; el hilo consumidor permanece independiente. */
    public Map<String, Object> esperarEvento(String idSolicitud, Duration timeout)
            throws InterruptedException, IOException {
        comprobarListo();
        BlockingQueue<Map<String, Object>> cola = solicitudesPendientes.get(idSolicitud);
        if (cola == null) {
            throw new IllegalStateException("No existe una solicitud pendiente con ID " + idSolicitud);
        }

        Map<String, Object> evento = cola.poll(Math.max(1L, timeout.toMillis()), TimeUnit.MILLISECONDS);
        Throwable fallo = falloConsumidor.get();
        if (evento == null && fallo != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + fallo.getMessage(), fallo);
        }
        if (evento == null && (hiloConsumidor == null || !hiloConsumidor.isAlive())) {
            throw new IOException("El consumidor Kafka ya no está activo.");
        }
        return evento;
    }

    /** Elimina únicamente la cola en memoria; no altera el fichero de pendientes. */
    public void finalizarSolicitud(String idSolicitud) {
        if (idSolicitud != null) {
            solicitudesPendientes.remove(idSolicitud);
        }
    }

    /** Marca una solicitud de riego como resuelta (elimina su entrada del fichero). */
    public void resolverPendiente(String idSolicitud) {
        eliminarPendiente(idSolicitud);
    }

    private void bucleConsumo() {
        boolean posicionInicialFijada = false;
        try {
            // Si se pierde la conexión o Kafka reinicia, se crea un consumidor nuevo.
            // Se conserva el group.id de esta ejecución y se confirman offsets para
            // poder reanudar desde el último lote procesado después de una reconexión.
            while (activo.get()) {
                try (KafkaConsumer<String, String> kafkaConsumer =
                             new KafkaConsumer<>(crearConfiguracionConsumidor())) {
                    consumidor = kafkaConsumer;
                    kafkaConsumer.subscribe(Arrays.asList(TOPICO_RESPUESTAS, TOPICO_LATIDO));

                    while (activo.get()) {
                        ConsumerRecords<String, String> registros =
                                kafkaConsumer.poll(Duration.ofMillis(500));

                        // Este grupo es exclusivo de esta instancia de WM_FO. Al iniciar,
                        // salta mensajes antiguos y confirma esa posición antes de enviar
                        // solicitudes para no perder respuestas nuevas.
                        if (!posicionInicialFijada && !kafkaConsumer.assignment().isEmpty()) {
                            Collection<TopicPartition> asignacion = kafkaConsumer.assignment();
                            kafkaConsumer.seekToEnd(asignacion);
                            for (TopicPartition particion : asignacion) {
                                kafkaConsumer.position(particion); // resuelve el offset final
                            }
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                            posicionInicialFijada = true;
                            consumidorListo.countDown();
                            continue;
                        }

                        if (!registros.isEmpty()) {
                            for (ConsumerRecord<String, String> registro : registros) {
                                despachar(registro.topic(), registro.value());
                            }
                            // Confirmar solo después de colocar los eventos en sus colas.
                            // En caso de fallo podría haber una redelivery, que se tolera.
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                        }
                    }
                } catch (WakeupException e) {
                    if (!activo.get()) {
                        break;
                    }
                    System.err.println("Se interrumpió el consumidor Kafka; se recreará la conexión.");
                    dormirReintento();
                } catch (Exception e) {
                    if (!activo.get()) {
                        break;
                    }
                    System.err.println("Error del consumidor Kafka: " + e.getMessage()
                            + ". Se reintentará automáticamente.");
                    dormirReintento();
                } finally {
                    consumidor = null;
                }
            }
        } catch (Throwable t) {
            if (activo.get()) {
                falloConsumidor.set(t);
                System.err.println("El consumidor Kafka se ha detenido de forma inesperada: " + t.getMessage());
            }
        } finally {
            consumidorListo.countDown();
        }
    }

    private void despachar(String tema, String textoJson) {
        if (TOPICO_LATIDO.equals(tema)) {
            ultimoLatidoMillis = System.currentTimeMillis();
            return;
        }
        try {
            Map<String, Object> evento = JSON.readValue(textoJson, TIPO_MAPA);
            String idSolicitud = texto(evento.get("requestId"));
            if (idSolicitud == null || idSolicitud.trim().isEmpty()) {
                System.err.println("Evento Kafka ignorado: no contiene requestId.");
                return;
            }

            Object valorOperario = evento.get("operatorId");
            if (valorOperario != null && entero(valorOperario, -1) != idOperario) {
                return;
            }

            BlockingQueue<Map<String, Object>> cola = solicitudesPendientes.get(idSolicitud);
            if (cola != null && !cola.offer(evento)) {
                System.err.println("Se ha llenado la cola local de eventos de " + idSolicitud
                        + "; se ha descartado un evento para no bloquear el consumidor Kafka.");
            }
            // Los eventos de otras solicitudes/operarios se descartan aquí: no bloquean
            // ni se muestran como notificaciones propias de esta instancia de WM_FO.
        } catch (Exception e) {
            System.err.println("Se ha ignorado un mensaje de respuesta Kafka mal formado: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ Persistencia

    private Path rutaFicheroPendientes() {
        String directorio = System.getenv("WM_FO_STATE_DIR");
        if (directorio == null || directorio.trim().isEmpty()) directorio = "data";
        return Paths.get(directorio, "fo_" + idOperario + ".pendientes");
    }

    /** Carga las solicitudes de riego sin resolver de ejecuciones anteriores. */
    public List<Pendiente> cargarPendientes() {
        List<Pendiente> pendientes = new ArrayList<>();
        Path ruta = rutaFicheroPendientes();
        if (!Files.isRegularFile(ruta)) return pendientes;
        try (BufferedReader lector = Files.newBufferedReader(ruta, StandardCharsets.UTF_8)) {
            String linea;
            while ((linea = lector.readLine()) != null) {
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) continue;
                String[] campos = contenido.split(";", -1);
                if (campos.length < 2) continue;
                try {
                    String idSolicitud = campos[0].trim();
                    int idWs = Integer.parseInt(campos[1].trim());
                    int duracion = campos.length >= 3 ? Integer.parseInt(campos[2].trim()) : 0;
                    if (!idSolicitud.isEmpty() && idWs > 0) {
                        pendientes.add(new Pendiente(idSolicitud, idWs, duracion));
                    }
                } catch (NumberFormatException ignorada) {
                    // Línea corrupta: se omite sin detener la recuperación.
                }
            }
        } catch (IOException e) {
            System.err.println("No se pudo leer el fichero de pendientes " + ruta + ": " + e.getMessage());
        }
        return pendientes;
    }

    private synchronized void persistirPendiente(String idSolicitud, int idWs, int duracion) {
        try {
            Path ruta = rutaFicheroPendientes();
            if (ruta.getParent() != null) Files.createDirectories(ruta.getParent());
            try (BufferedWriter escritor = Files.newBufferedWriter(ruta, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
                escritor.write(idSolicitud + ";" + idWs + ";" + duracion);
                escritor.newLine();
            }
        } catch (IOException e) {
            System.err.println("No se pudo persistir la solicitud pendiente: " + e.getMessage());
        }
    }

    private synchronized void eliminarPendiente(String idSolicitud) {
        if (idSolicitud == null) return;
        Path ruta = rutaFicheroPendientes();
        if (!Files.isRegularFile(ruta)) return;
        try {
            List<String> lineas = Files.readAllLines(ruta, StandardCharsets.UTF_8);
            List<String> restantes = new ArrayList<>();
            for (String linea : lineas) {
                String contenido = linea.trim();
                if (contenido.isEmpty() || contenido.startsWith("#")) {
                    restantes.add(linea);
                    continue;
                }
                String[] campos = contenido.split(";", -1);
                if (campos.length > 0 && idSolicitud.equals(campos[0].trim())) continue;
                restantes.add(linea);
            }
            Files.write(ruta, restantes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("No se pudo actualizar el fichero de pendientes: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ Kafka (apoyo)

    private boolean gestionTopicsHabilitada() {
        String configurado = System.getenv("WM_FO_MANAGE_TOPICS");
        return configurado == null || Boolean.parseBoolean(configurado.trim());
    }

    private void asegurarTopics() throws Exception {
        Properties propiedades = new Properties();
        propiedades.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        propiedades.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        propiedades.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

        try (AdminClient admin = AdminClient.create(propiedades)) {
            List<String> temas = Arrays.asList(TOPICO_SOLICITUDES, TOPICO_RESPUESTAS, TOPICO_LATIDO);
            for (String tema : temas) {
                boolean verificado = false;
                for (int intento = 1; intento <= 3 && !verificado; intento++) {
                    try {
                        Set<String> existentes = admin.listTopics().names().get(10, TimeUnit.SECONDS);
                        if (existentes.contains(tema)) {
                            verificado = true;
                            continue;
                        }
                        NewTopic nuevoTema = new NewTopic(tema, PARTICIONES_TEMA, FACTOR_REPLICACION_TEMA);
                        admin.createTopics(Collections.singleton(nuevoTema)).all().get(15, TimeUnit.SECONDS);
                        verificado = true;
                        System.out.println("Topic Kafka creado: " + tema);
                    } catch (ExecutionException e) {
                        Throwable causa = causaRaiz(e);
                        if (causa instanceof TopicExistsException) {
                            verificado = true; // otro proceso lo pudo crear al mismo tiempo
                        } else if (intento == 3) {
                            throw new IOException("No se pudo crear/verificar el topic '" + tema
                                    + "'. Comprueba la disponibilidad del broker y los permisos de administración.", causa);
                        } else {
                            dormirBackoff(intento);
                        }
                    } catch (TimeoutException e) {
                        if (intento == 3) {
                            throw new IOException("Timeout al crear/verificar el topic '" + tema + "'.", e);
                        }
                        dormirBackoff(intento);
                    }
                }
                if (!verificado) {
                    throw new IOException("No se pudo verificar el topic Kafka '" + tema + "'.");
                }
            }
        }
    }

    private Properties crearConfiguracionProductor() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "wm-fo-productor-" + idOperario + "-" + idSesion);
        return p;
    }

    private Properties crearConfiguracionConsumidor() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidoresKafka);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, idGrupoConsumidor);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "wm-fo-consumidor-" + idOperario + "-" + idSesion);
        return p;
    }

    private void comprobarListo() throws IOException {
        if (!iniciado || cerrado) {
            throw new IOException("El cliente Kafka de WM_FO no está iniciado o ya está cerrado.");
        }
        if (falloConsumidor.get() != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + falloConsumidor.get().getMessage(),
                    falloConsumidor.get());
        }
    }

    // ------------------------------------------------------------ Utilidades

    private static Throwable causaRaiz(Throwable t) {
        Throwable actual = t;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual;
    }

    private static String texto(Object valor) {
        return valor == null ? null : String.valueOf(valor);
    }

    private static int entero(Object valor, int porDefecto) {
        if (valor instanceof Number) {
            return ((Number) valor).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(valor));
        } catch (Exception ignorada) {
            return porDefecto;
        }
    }

    private static void dormirReintento() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void dormirBackoff(int intento) throws InterruptedException {
        Thread.sleep(Math.min(1000L * intento, 3000L));
    }

    @Override
    public void close() {
        cerrar();
    }

    public synchronized void cerrar() {
        if (cerrado) {
            return;
        }
        cerrado = true;
        activo.set(false);
        KafkaConsumer<String, String> c = consumidor;
        if (c != null) {
            c.wakeup();
        }
        Thread t = hiloConsumidor;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            productor.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            System.err.println("Aviso cerrando el productor Kafka: " + e.getMessage());
        }
        solicitudesPendientes.clear();
        iniciado = false;
    }

    /** Solicitud de riego conservada entre ejecuciones mientras no se resuelva. */
    public static final class Pendiente {
        public final String idSolicitud;
        public final int idWs;
        public final int duracionSegundos;

        Pendiente(String idSolicitud, int idWs, int duracionSegundos) {
            this.idSolicitud = idSolicitud;
            this.idWs = idWs;
            this.duracionSegundos = duracionSegundos;
        }
    }
}
