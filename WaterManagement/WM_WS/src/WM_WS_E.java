import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.time.Duration;
import java.net.Socket;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.concurrent.TimeUnit;

public class WM_WS_E {

    private static String ID = "";
    private static String ubicacion = "";
    private static int caudal = 0;
    private static long tiempoRiego = 0;
    private static KafkaConsumer<String, String> kafka;
    static private BlockingQueue<String> colaMensajesMonitor;
    static private BlockingQueue<String> colaMensajesPantalla;
    static final String nombreCentral = "central";

    private static String ipMonitorGlobal = "";
    private static int puertoMonitorGlobal = 0;

    public static String averia(Socket Monitor) {
        String datosMonitor;
        try {
            while (true) {
                datosMonitor = colaMensajesMonitor.poll(100, TimeUnit.MILLISECONDS);
                if (datosMonitor != null && datosMonitor.contains("\u0004")) {
                    System.out.println("--> AVISO: Señal de apagado recibida del Monitor.");
                    return "error";
                }

                ArrayList<String> mensajes = leerKafka(kafka);
                for (String mensaje : mensajes) {
                    if (mensaje.contains("ACTIVAR") && mensaje.contains(nombreCentral)) {
                        System.out.println("Avería resuelta vía Kafka. Volviendo a estado espera.");
                        if (!escribeSocket(Monitor, "#STATUS#ESPERA#")) return "desconectado";
                        return "espera";
                    }
                }

                if (!escribeSocket(Monitor, "#STATUS#AVERIA#")) {
                    System.out.println("Fallo de comunicación con Monitor durante avería.");
                    return "desconectado";
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Error en estado avería: " + e.toString());
        }
        return "error";
    }

    public static String bloqueo(Socket Monitor) {
        String datosMonitor;
        try {
            while (true) {
                datosMonitor = colaMensajesMonitor.poll(100, TimeUnit.MILLISECONDS);
                if (datosMonitor != null && datosMonitor.contains("\u0004")) {
                    System.out.println("--> AVISO: Señal de apagado recibida del Monitor.");
                    return "error";
                }

                ArrayList<String> mensajes = leerKafka(kafka);
                for (String mensaje : mensajes) {
                    if (mensaje.contains("ACTIVAR") && mensaje.contains(nombreCentral)) {
                        System.out.println("Estación desbloqueada vía Kafka. Volviendo a estado espera.");
                        if (!escribeSocket(Monitor, "#STATUS#ESPERA#")) return "desconectado";
                        return "espera";
                    }
                }

                if (!escribeSocket(Monitor, "#STATUS#BLOQUEO#")) {
                    System.out.println("Fallo de comunicación con Monitor durante bloqueo.");
                    return "desconectado";
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Error en estado bloqueo: " + e.toString());
        }
        return "error";
    }

    static public String leeSocket(Socket p_sk, String Datos) {
        try {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream(aux);
            String p_Datos = flujo.readUTF();

            while (!validaLrc(p_Datos)) {
                escribeSocket(p_sk, "\u0015");
                p_Datos = flujo.readUTF();
            }
            while (p_Datos.equals("\u0015") && !Datos.equals("")) {
                escribeSocket(p_sk, Datos);
                p_Datos = flujo.readUTF();
            }
            return p_Datos;
        } catch (Exception e) {
            System.out.println("Error de lectura en Socket: " + e.getMessage());
        }
        return null;
    }

    public static KafkaConsumer<String, String> crearKafka(String ip, int puerto) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + puerto);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-ws-" + ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("ordenes-estacion"));
        return consumer;
    }

    public static ArrayList<String> leerKafka(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        ArrayList<String> mensajes = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            if (record.value() != null && record.value().contains("\"wsId\":" + ID)) {
                mensajes.add(record.value());
            }
        }
        return mensajes;
    }

    static public boolean escribeSocket(Socket p_sk, String p_Datos) {
        try {
            String mensajeFinal = "";
            if (!p_Datos.equals("\u0006") && !p_Datos.equals("\u0015")) {
                mensajeFinal = lrc("\u0002" + p_Datos + "\u0003");
            } else {
                mensajeFinal = p_Datos;
            }
            OutputStream aux = p_sk.getOutputStream();
            DataOutputStream flujo = new DataOutputStream(aux);
            flujo.writeUTF(mensajeFinal);
            return true;
        } catch (Exception e) {
            System.out.println("Error al escribir en socket con Monitor: " + e.getMessage());
            return false;
        }
    }

    // Bucle de reintento físico de conexión con el Monitor
    static public Socket reconectarSocketMonitor() {
        Socket nuevoSocket = null;
        while (nuevoSocket == null) {
            try {
                System.out.println("Reintentando conexión con el Monitor (" + ipMonitorGlobal + ":" + puertoMonitorGlobal + ")...");
                nuevoSocket = new Socket(ipMonitorGlobal, puertoMonitorGlobal);
                ValidarId(nuevoSocket);
                System.out.println("Reconexión con el Monitor completada con éxito.");
            } catch (Exception e) {
                System.out.println("Monitor aún no disponible. Reintentando en 2 segundos...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return nuevoSocket;
    }

    static public void ValidarId(Socket p_sk) {
        String validar = "";
        while (validar == null || !validar.contains("ID")) {
            validar = leeSocket(p_sk, validar);
        }

        ID = validar.split("#ID#")[1].split("#")[0];
        ubicacion = validar.split("#UBICACION#")[1].split("#")[0];
        caudal = Integer.parseInt(validar.split("#CAUDAL#")[1].split("#")[0]);
        escribeSocket(p_sk, "\u0006");
    }

    public static String lrc(String p_datos) {
        byte[] bytes = p_datos.getBytes();
        int lrc = 0;
        for (byte b : bytes) {
            lrc ^= b;
        }
        return p_datos + Integer.toHexString(lrc);
    }

    public static boolean validaLrc(String p_datos) {
        if (p_datos == null) return false;
        if (p_datos.equals("\u0015") || p_datos.equals("\u0006")) return true;
        if (p_datos.length() < 2) return false;

        int longitud = p_datos.length();
        String cuerpo = p_datos.substring(0, longitud - 1);
        char lrcRecibido = p_datos.charAt(longitud - 1);

        byte[] bytes = cuerpo.getBytes();
        int lrcCalculado = 0;
        for (byte b : bytes) {
            lrcCalculado ^= b;
        }
        return lrcCalculado == (int) lrcRecibido;
    }

    static public String espera(Socket Monitor) {
        String datosMonitor;
        String datosPantalla;
        try {
            while (true) {
                ArrayList<String> mensajes = leerKafka(kafka);
                datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);
                datosPantalla = colaMensajesPantalla.poll();

                if (datosPantalla != null && datosPantalla.equals("KO")) {
                    escribeSocket(Monitor, "#STATUS#AVERIA#");
                    System.out.println(">>> TRANSICIÓN A AVERÍA desde pantalla.");
                    return "averia";
                } else if (datosMonitor != null && datosMonitor.contains("\u0004")) {
                    System.out.println("--> Señal de apagado recibida.");
                    return "error";
                }

                for (String mensaje : mensajes) {
                    if (mensaje.contains("BLOQUEO") && mensaje.contains(nombreCentral)) {
                        System.out.println("Orden de bloqueo recibida desde Kafka.");
                        if (!escribeSocket(Monitor, "#STATUS#BLOQUEO#")) return "desconectado";
                        return "bloqueo";
                    } else if (mensaje.contains("RIEGO_REQUEST") || mensaje.contains("RIEGO_ACTIVO")) {
                        System.out.println("Orden de riego activa desde Kafka.");
                        if (mensaje.contains("#TIEMPO#")) {
                            tiempoRiego = Long.parseLong(mensaje.split("#TIEMPO#")[1].split("#")[0]);
                        } else {
                            tiempoRiego = 0;
                        }
                        if (!escribeSocket(Monitor, "#STATUS#RIEGO#")) return "desconectado";
                        return "riego";
                    }
                }

                if (datosPantalla != null && datosPantalla.equals("RIEGO_ACTIVO")) {
                    if (!escribeSocket(Monitor, "#STATUS#RIEGO#")) return "desconectado";
                    System.out.println(">>> TRANSICIÓN A RIEGO desde pantalla.");
                    return "riego";
                }

                if (!escribeSocket(Monitor, "#STATUS#ESPERA#")) {
                    System.out.println("Desconectado del monitor.");
                    return "desconectado";
                }
            }
        } catch (Exception e) {
            System.out.println("Error en espera: " + e.toString());
        }
        return "error";
    }

    static public String regando(Socket Monitor) {
        long tiempoInicio = System.currentTimeMillis();
        try {
            while (true) {
                String datosPantalla = colaMensajesPantalla.poll();
                String datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);

                ArrayList<String> mensajes = leerKafka(kafka);

                if (datosPantalla != null && datosPantalla.equals("KO")) {
                    escribeSocket(Monitor, "#STATUS#AVERIA#");
                    System.out.println(">>> TRANSICIÓN A AVERÍA durante el riego.");
                    tiempoRiego = 0;
                    return "averia";
                }

                for (String mensaje : mensajes) {
                    if (mensaje.contains("BLOQUEO") && mensaje.contains(nombreCentral)) {
                        escribeSocket(Monitor, "#STATUS#BLOQUEO#");
                        System.out.println("Riego interrumpido por orden de BLOQUEO.");
                        tiempoRiego = 0;
                        return "bloqueo";
                    } else if (mensaje.contains("DETENER") && mensaje.contains(nombreCentral)) {
                        System.out.println("Riego detenido por orden de CENTRAL.");
                        escribeSocket(Monitor, "#STATUS#ESPERA#");
                        tiempoRiego = 0;
                        return "espera";
                    }
                }

                if (datosMonitor != null && datosMonitor.contains("\u0004")) {
                    System.out.println("--> Señal de apagado recibida.");
                    return "error";
                }

                if (datosPantalla != null && datosPantalla.equals("RIEGO_FINALIZADO")) {
                    escribeSocket(Monitor, "#STATUS#ESPERA#");
                    System.out.println(">>> RIEGO FINALIZADO desde pantalla.");
                    tiempoRiego = 0;
                    return "espera";
                }

                if (System.currentTimeMillis() - tiempoInicio >= tiempoRiego && tiempoRiego > 0) {
                    escribeSocket(Monitor, "#STATUS#ESPERA#");
                    System.out.println(">>> RIEGO FINALIZADO: Tiempo completado.");
                    tiempoRiego = 0;
                    return "espera";
                }

                float acumulado = (System.currentTimeMillis() - tiempoInicio) * (float) caudal / 60000;
                if (!escribeSocket(Monitor, "#STATUS#RIEGO#CAUDAL#" + caudal + "#ACUMULADO#" + acumulado)) {
                    tiempoRiego = 0;
                    System.out.println("Desconectado del monitor durante riego.");
                    return "desconectado";
                }
            }
        } catch (Exception e) {
            System.out.println("Error en estado regando: " + e.toString());
        }
        return "error";
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Indique los parámetros: <ipBroker> <puertoBroker> <ipMonitor> <puertoMonitor>");
            System.exit(1);
        }

        String ipBroker = args[0];
        String puertoBroker = args[1];
        ipMonitorGlobal = args[2];
        puertoMonitorGlobal = Integer.parseInt(args[3]);

        try {
            kafka = crearKafka(ipBroker, Integer.parseInt(puertoBroker));

            Socket skMonitor = new Socket(ipMonitorGlobal, puertoMonitorGlobal);
            ValidarId(skMonitor);

            colaMensajesMonitor = new ArrayBlockingQueue<>(100);
            HiloWS hiloMonitor = new HiloWS(skMonitor, colaMensajesMonitor);
            hiloMonitor.start();
            colaMensajesMonitor.put("EsperaContinua");

            colaMensajesPantalla = new ArrayBlockingQueue<>(100);
            HiloWS hiloPantalla = new HiloWS(null, colaMensajesPantalla);
            hiloPantalla.start();
            colaMensajesPantalla.put("EsperaPantalla");

            String datos = "espera";

            while (true) {
                if (datos.equals("espera")) {
                    datos = espera(skMonitor);
                } else if (datos.equals("averia")) {
                    datos = averia(skMonitor);
                } else if (datos.equals("bloqueo")) {
                    datos = bloqueo(skMonitor);
                } else if (datos.equals("riego")) {
                    datos = regando(skMonitor);
                } else if (datos.equals("desconectado")) {
                    // Cierre del socket e hilo antiguo roto
                    try { skMonitor.close(); } catch (Exception e) {}
                    if (hiloMonitor.isAlive()) hiloMonitor.interrupt();

                    // Creación de nuevo socket e hilo limpio
                    skMonitor = reconectarSocketMonitor();
                    colaMensajesMonitor = new ArrayBlockingQueue<>(100);
                    hiloMonitor = new HiloWS(skMonitor, colaMensajesMonitor);
                    hiloMonitor.start();

                    datos = "espera";
                } else {
                    System.out.println("Error en la supervisión y operación del sistema.");
                    break;
                }
            }

            kafka.close();
            skMonitor.close();
            while (hiloMonitor.isAlive()) hiloMonitor.interrupt();
            while (hiloPantalla.isAlive()) hiloPantalla.interrupt();

        } catch (Exception e) {
            System.out.println("Error en el componente Engine: " + e.toString());
        }
    }
}