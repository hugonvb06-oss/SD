import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
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
	public static String averia(){
		String datosMonitor;

		while (true) {
			datosMonitor = colaMensajesMonitor.take();
			if(datosMonitor.contains("ACTIVAR")){
				return "espera";
			}
		}
		
	}
	public static String bloqueo(){
		String datosMonitor;

		while (true) {
			datosMonitor = colaMensajesMonitor.take();
			if(datosMonitor.contains("ACTIVAR")){
				return "espera";
			}
		}
		
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
				p_Datos = fujo.readUTF();
			}
			while(p_Datos.equals("\u0015")){
				escribeSocket(p_sk, datos);
				p_Datos = fujo.readUTF();
			}
			
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}
	
	public static KafkaConsumer<String, String> crearKafka(String ip, int puerto) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + puerto);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-java-" + ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.assign(Collections.singletonList(new TopicPartition("mi-topico", particion)));
		return consumer;
	}

public static ArrayList<String> leerKafka(KafkaConsumer<String, String> consumer) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

    ArrayList<String> mensajes = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
        if (ID.equals(record.key())) {
            // Procesar el mensaje correctamente
            mensajes.add(record.value());

            // Confirmar solo después de procesarlo
            consumer.commitSync();
        
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
	static public void ValidarId(Socket p_sk)
	{
		String validar ="";
		while (validar==null||!validar.contains("ID")) {
			validar =leeSocket(p_sk, validar);
		}
		
		
		ID = validar.split("#ID#")[1].split("#")[0];
		ubicacion = validar.split("#UBICACION#")[1].split("#")[0];
		caudal = validar.split("#CAUDAL#")[1].split("#")[0];
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
		boolean espera = true;
		String datosMonitor;
		String datosPantalla;
		try
		{
			while(espera)
			{
				ArrayList<String> mensajes = leerKafka(kafka);
				datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);
				datosPantalla = colaMensajesPantalla.poll();
				if(datosPantalla != null && datosPantalla.equals("KO")){
					escribeSocket(Monitor, datosPantalla);
					return "averia";
				}else if(datosMonitor != null && datosMonitor.contains("BLOQUEO")){
					return "bloqueo";
				}else if(datosPantalla != null && datosPantalla.equals("RIEGO_ACTIVO")){
					escribeSocket(Monitor, datosPantalla);
					return "riego";
				}else if(datosMonitor != null && datosMonitor.contains("RIEGO_ACTIVO")){
					return "riego";
				}
				for (String mensaje : mensajes) {
					if (mensaje.contains("RIEGO_ACTIVO")) {
						espera = true;
						System.out.println("Riego activo");
						if(mensaje.contains("#TIEMPO#")){
							tiempoRiego = Long.parseLong(mensaje.split("#TIEMPO#")[1].split("#")[0]);
						}
						
						escribeSocket(Monitor, mensaje);
						return "riego";
					}
				}
				escribeSocket(Monitor, "#ESTADO#ESPERA#");
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
		String datos = "";
		long tiempoInicio = System.currentTimeMillis();
		try
		{
			while(true)
			{
				String datosPantalla = colaMensajesPantalla.poll();
				String datosMonitor = colaMensajesMonitor.poll(1, TimeUnit.SECONDS);
				if(datosPantalla != null && datosPantalla.equals("KO")){
					escribeSocket(Monitor, datosPantalla);
					tiempoRiego = 0;
					return "averia";
				}
				if(datosMonitor != null && datosMonitor.contains("BLOQUEO")){
					tiempoRiego = 0;
					return "bloqueo";
				}
				if(datosPantalla != null && datosPantalla.equals("RIEGO_FINALIZADO")){
					escribeSocket(Monitor, datosPantalla);
					tiempoRiego = 0;
					return "espera";
				}
				if (System.currentTimeMillis() - tiempoInicio >= tiempoRiego&&tiempoRiego>0) {
					escribeSocket(Monitor, "#RIEGO_FINALIZADO#");
					tiempoRiego = 0;
					return "espera";
				}	
				escribeSocket(Monitor, "#ESTADO#RIEGO_ACTIVO#CAUDAL#"+caudal+"#ACUMULADO#"+(System.currentTimeMillis() - tiempoInicio)*Float.parseFloat(caudal)/60000);
	
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
                    datos = averia();
                    colaMensajesPantalla.put("EsperaPantalla");
                } else if(datos.equals("bloqueo")) {
                    datos = bloqueo();
                } else if(datos.equals("riego")) {
                    datos = regando(skMonitor);
                }else {
                    System.out.println("Error en la supervisión y operación del sistema.");
                }
            }
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
