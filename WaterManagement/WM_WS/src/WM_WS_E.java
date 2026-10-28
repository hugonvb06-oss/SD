import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.concurrent.TimeUnit;
public class WM_WS_E {
	private static int  particion = 0;
	private static String ID = "";
	private static String ubicacion = "";
	private static int caudal=0;
	private static long tiempoRiego = 0;
	private static KafkaConsumer<String, String> kafka;
	static private BlockingQueue<String> colaMensajesMonitor;
    static private BlockingQueue<String> colaMensajesPantalla;
	static final String operador = "operatorId";
	static final String nombreCentral = "central";
	public static String averia(Socket Monitor){
		String datosMonitor;
		try{
			while (true) {
				// Mantenemos la comprobación del socket con el monitor por si hay cierre o error crítico
				datosMonitor = colaMensajesMonitor.poll(100, TimeUnit.MILLISECONDS);
				if(datosMonitor != null && datosMonitor.contains("\u0004")) {
					System.out.println("--> AVISO: Señal de apagado/desconexión recibida del Monitor (\u0004). Finalizando proceso...");
					return "error";
				}

				// CAMBIO KAFKA: Consultamos Kafka a la espera del comando ACTIVAR enviado por CENTRAL
				ArrayList<String> mensajes = leerKafka(kafka);
				for (String mensaje : mensajes) {
					if (mensaje.contains("ACTIVAR")&&mensaje.contains(nombreCentral)) {
						System.out.println("Avería resuelta vía Kafka. Volviendo a estado espera.");
						escribeSocket(Monitor, "#STATUS#ESPERA#");
						return "espera";
					}
				}
				if(datosMonitor!=null){
					escribeSocket(Monitor, "#STATUS#AVERIA#");
				}else{
					System.out.println("Desconectado del monitor");
					return "desconectado";
				}
			}
		}catch(InterruptedException e){
			System.out.println("Error: " + e.toString());
		}
		
		return "error";
	}
	public static String bloqueo(Socket Monitor){
		String datosMonitor;
		try{
			while (true) {
				datosMonitor = colaMensajesMonitor.poll(100, TimeUnit.MILLISECONDS);
				if(datosMonitor != null && datosMonitor.contains("\u0004")) {
					System.out.println("--> AVISO: Señal de apagado/desconexión recibida del Monitor (\u0004). Finalizando proceso...");
					return "error";
				}

				// CAMBIO KAFKA: Leer mensajes de Kafka esperando la orden ACTIVAR
				ArrayList<String> mensajes = leerKafka(kafka);
				for (String mensaje : mensajes) {
					if (mensaje.contains("ACTIVAR")&&mensaje.contains(nombreCentral)) {
						System.out.println("Estación desbloqueada vía Kafka. Volviendo a estado espera.");
						escribeSocket(Monitor, "#STATUS#ESPERA#");
						return "espera";
					}
				}
				if(datosMonitor!=null){
					escribeSocket(Monitor, "#STATUS#BLOQUEO#");
				}else{
					System.out.println("Desconectado del monitor");
					return "desconectado";
				}
			}
		}catch(InterruptedException e){
			System.out.println("Error: " + e.toString());
		}
		
		return "error";
	}
	static public String leeSocket (Socket p_sk, String Datos)
	{
		try
		{
			String p_Datos;
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = new String();
			p_Datos = flujo.readUTF();
			while(!validaLrc(p_Datos)){
				escribeSocket(p_sk, "\u0015");
				p_Datos = flujo.readUTF();
			}
			while(p_Datos.equals("\u0015")&&Datos!=""){
				escribeSocket(p_sk, Datos);
				p_Datos = flujo.readUTF();
			}
			
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return Datos;
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
				// Procesar el mensaje correctamente
				mensajes.add(record.value());


			
			}
		}
		return mensajes;
	}
    // Método 2: leer los mensajes del objeto Kafka
    
	static public void escribeSocket (Socket p_sk, String p_Datos)
	{
		String mensajeFinal = "";
		try
		{
			if (!p_Datos.equals("\u0006") && !p_Datos.equals("\u0015")) {
            	mensajeFinal = lrc("\u0002" + p_Datos + "\u0003");
        	}
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(mensajeFinal);      
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return;
	}
	static public String reconexion(Socket p_sk)
	{
		colaMensajesMonitor.take();
		escribeSocket(p_sk, "\u0006");
		return "espera";
	}
	static public void ValidarId(Socket p_sk)
	{
		String validar ="";
		while (validar==null||!validar.contains("ID")) {
			validar =leeSocket(p_sk, validar);
		}
		
		
		ID = validar.split("#ID#")[1].split("#")[0];
		ubicacion = validar.split("#UBICACION#")[1].split("#")[0];
		caudal = Integer.parseInt(validar.split("#CAUDAL#")[1].split("#")[0]);
		escribeSocket(p_sk, "\u0006");
	}
	public static String lrc(String p_datos)
	{
        byte[] bytes = p_datos.getBytes();
        int lrc = 0;
        for (byte b : bytes) {
            lrc ^= b;
        }
        return p_datos+Integer.toHexString(lrc);
    }
	public static boolean validaLrc(String p_datos)
	{
		
        if (p_datos == null) return false;
		if(p_datos.equals("\u0015")||p_datos.equals("\u0006")) return true;
    	if(p_datos.length()<2)return false;
		int longitud = p_datos.length();
		String cuerpo = p_datos.substring(0, longitud - 1);
		char lrcRecibido = p_datos.charAt(longitud - 1);

		// Recalcular el LRC del cuerpo
		byte[] bytes = cuerpo.getBytes();
		int lrcCalculado = 0;
		for (byte b : bytes) {
			lrcCalculado ^= b;
		}

    return lrcCalculado == (int) lrcRecibido;
    }
	static public String espera(Socket Monitor)
	{
		String datosMonitor;
		String datosPantalla;
		try
		{
			while(true)
			{
				ArrayList<String> mensajes = leerKafka(kafka);
				datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);
				datosPantalla = colaMensajesPantalla.poll();

				// Verificaciones de pantalla y de avería en socket del monitor
				if(datosPantalla != null && datosPantalla.equals("KO")){
					escribeSocket(Monitor, "#STATUS#AVERIA#");
					System.out.println(">>> TRANSICIÓN A AVERÍA: Falla reportada desde la Pantalla local.");
					return "averia";
				}else if(datosMonitor != null && datosMonitor.contains("\u0004")) {
					System.out.println("--> AVISO: Señal de apagado/desconexión recibida del Monitor (\u0004). Finalizando proceso...");
					return "error";
				}

				// CAMBIO KAFKA: Procesar los cambios de estado (BLOQUEO o RIEGO) provenientes de la cola de KAFKA
				for (String mensaje : mensajes) {
					if (mensaje.contains("BLOQUEO")&&mensaje.contains(nombreCentral)) {
						System.out.println("Orden de bloqueo recibida desde Kafka.");
						escribeSocket(Monitor, "#STATUS#BLOQUEO#");
						return "bloqueo";
					} else if (mensaje.contains("RIEGO_REQUEST") || mensaje.contains("RIEGO_ACTIVO")) {
						System.out.println("Orden de riego activa desde Kafka.");
						if(mensaje.contains("#TIEMPO#")){
							tiempoRiego = Long.parseLong(mensaje.split("#TIEMPO#")[1].split("#")[0]);
						}else{
							tiempoRiego = 0;
						}
						System.out.println(">>> TRANSICIÓN A RIEGO: Iniciado manualmente desde la Pantalla local.");
						escribeSocket(Monitor, "#STATUS#RIEGO#");
						return "riego";
					}
				}

				// Si también se solicita iniciar desde la interfaz local (pantalla)
				if(datosPantalla != null && datosPantalla.equals("RIEGO_ACTIVO")){
					escribeSocket(Monitor, "#STATUS#RIEGO#");
					System.out.println(">>> TRANSICIÓN A RIEGO: Iniciado manualmente desde la Pantalla local.");
					return "riego";
				}
				if(datosMonitor!=null){
					escribeSocket(Monitor, "#STATUS#ESPERA#");
				}else{
					System.out.println("Desconectado del monitor");
					return "desconectado";
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return "error";
	}
	static public String regando(Socket Monitor)
	{
		long tiempoInicio = System.currentTimeMillis();
		long tiempoEspera = 0;
		try
		{
			while(true)
			{
				String datosPantalla = colaMensajesPantalla.poll();
				String datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);

				// CAMBIO KAFKA: Consultar los mensajes recibidos por Kafka durante el riego
				ArrayList<String> mensajes = leerKafka(kafka);

				if(datosPantalla != null && datosPantalla.equals("KO")){
					escribeSocket(Monitor, "#STATUS#AVERIA#");
					System.out.println(">>> TRANSICIÓN A AVERÍA: Falla reportada desde la Pantalla local.");
					tiempoEspera = System.currentTimeMillis();
					averia(Monitor);
					tiempoEspera = System.currentTimeMillis() - tiempoEspera;
				}

				// CAMBIO KAFKA: Si CENTRAL manda un BLOQUEO o DETENER por Kafka durante el riego
				for (String mensaje : mensajes) {
					if (mensaje.contains("BLOQUEO")&&mensaje.contains(nombreCentral)) {
						escribeSocket(Monitor, "#STATUS#BLOQUEO#");
						System.out.println("Riego interrumpido por orden de BLOQUEO desde Kafka.");
						tiempoRiego = 0;
						return "bloqueo";
					} else if (mensaje.contains("DETENER")&&mensaje.contains(nombreCentral)) {
						System.out.println("Riego detenido por orden de CENTRAL desde Kafka.");
						escribeSocket(Monitor, "#STATUS#ESPERA#");
						tiempoRiego = 0;
						return "espera";
					}
				}

				if(datosMonitor != null && datosMonitor.contains("\u0004")) {
					System.out.println("--> AVISO: Señal de apagado/desconexión recibida del Monitor (\u0004). Finalizando proceso...");
					return "error";
				}

				if(datosPantalla != null && datosPantalla.equals("RIEGO_FINALIZADO")){
					escribeSocket(Monitor, "#STATUS#ESPERA#");
					System.out.println(">>> RIEGO FINALIZADO: Interrumpido desde la Pantalla local. Volviendo a ESPERA.");
					tiempoRiego = 0;
					return "espera";
				}

				if (System.currentTimeMillis() - tiempoInicio - tiempoEspera >= tiempoRiego && tiempoRiego > 0) {
					escribeSocket(Monitor, "#STATUS#ESPERA#");
					System.out.println(">>> RIEGO FINALIZADO: Tiempo completado con éxito. Volviendo a ESPERA."); // AÑADIR ESTO
					tiempoRiego = 0;
					return "espera";
				}	
				if(datosMonitor!=null){
					escribeSocket(Monitor, "#STATUS#RIEGO#CAUDAL#"+caudal+"#ACUMULADO#"+(System.currentTimeMillis() - tiempoInicio)*(float)caudal/60000);
				}else{
					tiempoRiego = 0;
					System.out.println("Desconectado del monitor");
					return "desconectado";
				}
				
			}
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return "error";
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
        String ipBroker="";
        String ipMonitor="";
		String puertoBroker="";
		String puertoMonitor = "";
		try
		{
			 
			if (args.length < 4) {
				System.out.println("Debe indicar el puerto de escucha del servidor");
				System.out.println("$./Servidor puerto_servidor");
				System.exit (1);
			}
			ipBroker = args[0];
			puertoBroker = args[1];
			kafka = crearKafka(ipBroker, Integer.parseInt(puertoBroker));
		    
		    ipMonitor = args[2];
            puertoMonitor = args[3];
			Socket skMonitor = new Socket(ipMonitor, Integer.parseInt(puertoMonitor));
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
			long tiempoRiego = 0;
			while(true){
                if(datos.equals("espera")){
                    datos = espera(skMonitor);
                }else if(datos.equals("averia")) {
                    datos = averia(skMonitor);
                } else if(datos.equals("bloqueo")) {
                    datos = bloqueo(skMonitor);
                } else if(datos.equals("riego")) {
                    datos = regando(skMonitor);
                }else if(datos.equals("desconectado")){
					datos = reconexion(skMonitor);
				}else{
                    System.out.println("Error en la supervisión y operación del sistema.");
					break;
                }
            }
			kafka.close();
			skMonitor.close();
			while(hiloMonitor.isAlive()) hiloMonitor.interrupt();
            while(hiloPantalla.isAlive()) hiloPantalla.interrupt();
			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
			
		}
	}

}
