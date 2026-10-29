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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
 * - wm.riego.solicitudes: solicitudes de riego y peticiones de listado.
 * - wm.riego.respuestas: eventos de avance/finalización y respuesta del listado.
 *
 * Cada solicitud tiene un requestId UUID. El consumidor Kafka se ejecuta en un
 * hilo independiente; la UI nunca accede directamente al KafkaConsumer, que no
 * es thread-safe.
 */
public final class KafkaOperatorClient implements AutoCloseable {

    public static final String TOPIC_SOLICITUDES = "wm.riego.solicitudes";
    public static final String TOPIC_RESPUESTAS = "wm.riego.respuestas";
    public static final String TOPIC_LATIDO = "wm.central.latido";

    private static final long CENTRAL_ALIVE_WINDOW_MS = 8_000L;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE =
            new TypeReference<Map<String, Object>>() { };

    private static final int TOPIC_PARTITIONS = 3;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static final long SEND_TIMEOUT_SECONDS = 35;
    private static final long CONSUMER_READY_TIMEOUT_SECONDS = 30;

    private final String bootstrapServers;
    private final int operatorId;
    private final String sessionId = UUID.randomUUID().toString();
    private final String consumerGroupId;
    private final KafkaProducer<String, String> producer;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    private final CountDownLatch consumerReady = new CountDownLatch(1);
    private final Map<String, BlockingQueue<Map<String, Object>>> pendingRequests =
            new ConcurrentHashMap<>();

    private volatile KafkaConsumer<String, String> consumer;
    private volatile Thread consumerThread;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile long lastHeartbeatMillis;
    private volatile long startedAtMillis;

    public KafkaOperatorClient(String bootstrapServers, int operatorId) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("La dirección del broker no puede estar vacía.");
        }
        if (operatorId <= 0) {
            throw new IllegalArgumentException("El ID del operario debe ser un entero positivo.");
        }

        this.bootstrapServers = bootstrapServers.trim();
        this.operatorId = operatorId;
        this.consumerGroupId = "wm-fo-" + operatorId + "-" + sessionId;
        this.producer = new KafkaProducer<>(crearConfiguracionProductor());
    }

    /**
     * Comprueba/crea los topics, arranca el consumidor y espera a que esté
     * asignado al topic de respuestas antes de permitir publicar peticiones.
     */
    public synchronized void start() throws Exception {
        if (closed) {
            throw new IllegalStateException("Este cliente Kafka ya está cerrado.");
        }
        if (started) {
            return;
        }

        if (topicManagementEnabled()) ensureTopicsExist();
        running.set(true);
        consumerThread = new Thread(this::consumeLoop, "wm-fo-kafka-consumer-" + operatorId);
        consumerThread.setDaemon(true);
        consumerThread.start();

        boolean ready = consumerReady.await(CONSUMER_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Throwable failure = consumerFailure.get();
        if (!ready || failure != null || !consumerThread.isAlive()) {
            close();
            if (failure != null) {
                throw new IllegalStateException("El consumidor Kafka no pudo arrancar: "
                        + failure.getMessage(), failure);
            }
            throw new TimeoutException("Kafka no asignó el topic de respuestas en "
                    + CONSUMER_READY_TIMEOUT_SECONDS + " segundos. Comprueba el broker y los topics.");
        }

        started = true;
        startedAtMillis = System.currentTimeMillis();
        System.out.println("Kafka conectado. Consumidor listo para recibir respuestas.");
        System.out.println("Topics: " + TOPIC_SOLICITUDES + " / " + TOPIC_RESPUESTAS + " / " + TOPIC_LATIDO);
    }

    /**
     * Indica si WM_Central sigue viva según los latidos publicados por Kafka.
     * Permite avisar al operario aunque el broker de Kafka siga funcionando.
     */
    public boolean isCentralAlive() {
        if (!started || closed) return false;
        long reference = Math.max(lastHeartbeatMillis, startedAtMillis);
        return System.currentTimeMillis() - reference <= CENTRAL_ALIVE_WINDOW_MS;
    }

    public String solicitarRiego(int wsId, int durationSeconds) throws Exception {
        if (wsId <= 0) {
            throw new IllegalArgumentException("El ID de la WS debe ser positivo.");
        }
        if (durationSeconds <= 0 || durationSeconds > 86400) {
            throw new IllegalArgumentException("La duración debe estar entre 1 y 86400 segundos.");
        }

        String requestId = UUID.randomUUID().toString();
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", "RIEGO_REQUEST");
        payload.put("requestId", requestId);
        payload.put("operatorId", operatorId);
        payload.put("wsId", wsId);
        payload.put("durationSeconds", durationSeconds);
        payload.put("createdAt", Instant.now().toString());

        publishRequest(requestId, payload);
        return requestId;
    }

    public String solicitarListadoWsDisponibles() throws Exception {
        String requestId = UUID.randomUUID().toString();
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", "LIST_AVAILABLE_WS");
        payload.put("requestId", requestId);
        payload.put("operatorId", operatorId);
        payload.put("createdAt", Instant.now().toString());

        publishRequest(requestId, payload);
        return requestId;
    }

    private void publishRequest(String requestId, Map<String, Object> payload) throws Exception {
        checkReady();
        BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(1000);
        pendingRequests.put(requestId, queue);

        try {
            String json = JSON.writeValueAsString(payload);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_SOLICITUDES,
                    Integer.toString(operatorId),
                    json);

            producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            System.out.println("Solicitud publicada en Kafka. ID: " + requestId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingRequests.remove(requestId);
            throw new IOException("Se interrumpió el envío a Kafka.", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            pendingRequests.remove(requestId);
            throw new IOException("No se pudo confirmar el envío a Kafka: "
                    + causaRaiz(e).getMessage()
                    + ". El resultado del envío puede ser incierto si se agotó el tiempo de espera.", e);
        } catch (IOException e) {
            pendingRequests.remove(requestId);
            throw e;
        }
    }

    /** Devuelve null al agotar el timeout; el hilo consumidor permanece independiente. */
    public Map<String, Object> esperarEvento(String requestId, Duration timeout)
            throws InterruptedException, IOException {
        checkReady();
        BlockingQueue<Map<String, Object>> queue = pendingRequests.get(requestId);
        if (queue == null) {
            throw new IllegalStateException("No existe una solicitud pendiente con ID " + requestId);
        }

        Map<String, Object> event = queue.poll(Math.max(1L, timeout.toMillis()), TimeUnit.MILLISECONDS);
        Throwable failure = consumerFailure.get();
        if (event == null && failure != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + failure.getMessage(), failure);
        }
        if (event == null && (consumerThread == null || !consumerThread.isAlive())) {
            throw new IOException("El consumidor Kafka ya no está activo.");
        }
        return event;
    }

    public void finalizarSolicitud(String requestId) {
        if (requestId != null) {
            pendingRequests.remove(requestId);
        }
    }

    private void consumeLoop() {
        boolean initialPositionSet = false;
        try {
            // Si se pierde la conexión o Kafka reinicia, se crea un consumidor nuevo.
            // Se conserva el group.id de esta ejecución y se confirman offsets para
            // poder reanudar desde el último lote procesado después de una reconexión.
            while (running.get()) {
                try (KafkaConsumer<String, String> kafkaConsumer =
                             new KafkaConsumer<>(crearConfiguracionConsumidor())) {
                    consumer = kafkaConsumer;
                    kafkaConsumer.subscribe(Arrays.asList(TOPIC_RESPUESTAS, TOPIC_LATIDO));

                    while (running.get()) {
                        ConsumerRecords<String, String> records =
                                kafkaConsumer.poll(Duration.ofMillis(500));

                        // Este grupo es exclusivo de esta instancia de WM_FO. Al iniciar,
                        // salta mensajes antiguos y confirma esa posición antes de enviar
                        // solicitudes para no perder respuestas nuevas.
                        if (!initialPositionSet && !kafkaConsumer.assignment().isEmpty()) {
                            Collection<TopicPartition> assignment = kafkaConsumer.assignment();
                            kafkaConsumer.seekToEnd(assignment);
                            for (TopicPartition partition : assignment) {
                                kafkaConsumer.position(partition); // resuelve el offset final
                            }
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                            initialPositionSet = true;
                            consumerReady.countDown();
                            continue;
                        }

                        if (!records.isEmpty()) {
                            for (ConsumerRecord<String, String> record : records) {
                                dispatch(record.topic(), record.value());
                            }
                            // Confirmar solo después de colocar los eventos en sus colas.
                            // En caso de fallo podría haber una redelivery, que se tolera.
                            kafkaConsumer.commitSync(Duration.ofSeconds(10));
                        }
                    }
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    System.err.println("Se interrumpió el consumidor Kafka; se recreará la conexión.");
                    dormirReintento();
                } catch (Exception e) {
                    if (!running.get()) {
                        break;
                    }
                    System.err.println("Error del consumidor Kafka: " + e.getMessage()
                            + ". Se reintentará automáticamente.");
                    dormirReintento();
                } finally {
                    consumer = null;
                }
            }
        } catch (Throwable t) {
            if (running.get()) {
                consumerFailure.set(t);
                System.err.println("El consumidor Kafka se ha detenido de forma inesperada: " + t.getMessage());
            }
        } finally {
            consumerReady.countDown();
        }
    }

    private void dispatch(String topic, String json) {
        if (TOPIC_LATIDO.equals(topic)) {
            lastHeartbeatMillis = System.currentTimeMillis();
            return;
        }
        try {
            Map<String, Object> event = JSON.readValue(json, MAP_TYPE);
            String requestId = asString(event.get("requestId"));
            if (requestId == null || requestId.trim().isEmpty()) {
                System.err.println("Evento Kafka ignorado: no contiene requestId.");
                return;
            }

            Object operatorValue = event.get("operatorId");
            if (operatorValue != null && asInt(operatorValue, -1) != operatorId) {
                return;
            }

            BlockingQueue<Map<String, Object>> queue = pendingRequests.get(requestId);
            if (queue != null && !queue.offer(event)) {
                System.err.println("Se ha llenado la cola local de eventos de " + requestId
                        + "; se ha descartado un evento para no bloquear el consumidor Kafka.");
            }
            // Los eventos de otras solicitudes/operarios se descartan aquí: no bloquean
            // ni se muestran como notificaciones propias de esta instancia de WM_FO.
        } catch (Exception e) {
            System.err.println("Se ha ignorado un mensaje de respuesta Kafka mal formado: " + e.getMessage());
        }
    }

    private boolean topicManagementEnabled() {
        String configured = System.getenv("WM_FO_MANAGE_TOPICS");
        return configured == null || Boolean.parseBoolean(configured.trim());
    }

    private void ensureTopicsExist() throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

        try (AdminClient admin = AdminClient.create(properties)) {
            List<String> topics = Arrays.asList(TOPIC_SOLICITUDES, TOPIC_RESPUESTAS, TOPIC_LATIDO);
            for (String topic : topics) {
                boolean verified = false;
                for (int attempt = 1; attempt <= 3 && !verified; attempt++) {
                    try {
                        Set<String> existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
                        if (existing.contains(topic)) {
                            verified = true;
                            continue;
                        }
                        NewTopic newTopic = new NewTopic(topic, TOPIC_PARTITIONS, TOPIC_REPLICATION_FACTOR);
                        admin.createTopics(Collections.singleton(newTopic)).all().get(15, TimeUnit.SECONDS);
                        verified = true;
                        System.out.println("Topic Kafka creado: " + topic);
                    } catch (ExecutionException e) {
                        Throwable cause = causaRaiz(e);
                        if (cause instanceof TopicExistsException) {
                            verified = true; // otro proceso lo pudo crear al mismo tiempo
                        } else if (attempt == 3) {
                            throw new IOException("No se pudo crear/verificar el topic '" + topic
                                    + "'. Comprueba la disponibilidad del broker y los permisos de administración.", cause);
                        } else {
                            dormirBackoff(attempt);
                        }
                    } catch (TimeoutException e) {
                        if (attempt == 3) {
                            throw new IOException("Timeout al crear/verificar el topic '" + topic + "'.", e);
                        }
                        dormirBackoff(attempt);
                    }
                }
                if (!verified) {
                    throw new IOException("No se pudo verificar el topic Kafka '" + topic + "'.");
                }
            }
        }
    }

    private Properties crearConfiguracionProductor() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "wm-fo-producer-" + operatorId + "-" + sessionId);
        return p;
    }

    private Properties crearConfiguracionConsumidor() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "wm-fo-consumer-" + operatorId + "-" + sessionId);
        return p;
    }

    private void checkReady() throws IOException {
        if (!started || closed) {
            throw new IOException("El cliente Kafka de WM_FO no está iniciado o ya está cerrado.");
        }
        if (consumerFailure.get() != null) {
            throw new IOException("El consumidor Kafka ha fallado: " + consumerFailure.get().getMessage(),
                    consumerFailure.get());
        }
    }

    private static Throwable causaRaiz(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static int asInt(Object value, int fallback) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static void dormirReintento() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void dormirBackoff(int attempt) throws InterruptedException {
        Thread.sleep(Math.min(1000L * attempt, 3000L));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        running.set(false);
        KafkaConsumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        Thread t = consumerThread;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            System.err.println("Aviso cerrando el productor Kafka: " + e.getMessage());
        }
        pendingRequests.clear();
        started = false;
    }
}
