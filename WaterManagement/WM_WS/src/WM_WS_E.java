import java.io.*;
import java.util.*;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
public class WM_WS_E {
	private static int  particion = 0;
	private static String ID = "";
	private static String ubicacion = "";
	private static String caudal = "";
	public static void averia(KafkaConsumer<String, String> kafka, Socket p_skMonitorSocket){
		escribeSocket(p_skMonitorSocket, "\u0005"+"#AVERIA#");
		String respuesta = "";
		respuesta = WM_WS_M.leeSocket(p_skMonitorSocket, respuesta);
		if (respuesta.equals("\u0006"))
			System.out.println("Averia enviada correctamente");
		else
			System.out.println("Error al enviar averia");
		
	}
	static public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = new String();
			p_Datos = flujo.readUTF();
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
		validar =leeSocket(p_sk, validar);
		while (!validaLrc(validar)){
			System.out.println("Error: LRC invalido");
			escribeSocket(p_sk, "\u0015");
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
        if (p_tramaRecibida == null || p_tramaRecibida.length() < 2) return false;

    	
		int longitud = p_tramaRecibida.length();
		String cuerpo = p_tramaRecibida.substring(0, longitud - 1);
		char lrcRecibido = p_tramaRecibida.charAt(longitud - 1);

		// Recalcular el LRC del cuerpo
		byte[] bytes = cuerpo.getBytes();
		int lrcCalculado = 0;
		for (byte b : bytes) {
			lrcCalculado ^= b;
		}

    return lrcCalculado == (int) lrcRecibido;
    }
	static public long EsperaRiego(KafkaConsumer<String, String> kafka, Socket Monitor)
	{
		boolean riegoActivo = false;
		long tiempoRiego = 0;
		try
		{
			while(!riegoActivo)
			{
				ArrayList<String> mensajes = leerKafka(kafka);
				for (String mensaje : mensajes) {
					if (mensaje.contains("RIEGO_ACTIVO")) {
						riegoActivo = true;
						System.out.println("Riego activo");
						tiempoRiego = Long.parseLong(mensaje.split("#TIEMPO#")[1].split("#")[0]);
						escribeSocket(Monitor, mensaje);
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
	  return tiempoRiego;
	}
	static public void regando(KafkaConsumer<String, String> kafka, Socket Monitor, long tiempoRiego)
	{
		String datos = "";
		boolean riegoActivo = true;
		long tiempoInicio = System.currentTimeMillis();
		try
		{
			while(riegoActivo)
			{
				ArrayList<String> mensajes = leerKafka(kafka);
				leeSocket(Monitor, datos);
				for (String mensaje : mensajes) {
					if (mensaje.contains("AVERIA")) {
						riegoActivo = false;
						escribeSocket(Monitor, "#KO#");
					}
				}
				if (System.currentTimeMillis() - tiempoInicio >= tiempoRiego&&tiempoRiego>0) {
					riegoActivo = false;
					escribeSocket(Monitor, "#riego_finalizado_correctamente#");
				}
				if(riegoActivo){
					escribeSocket(Monitor, "#ESTADO#riego_activo#CAUDAL#"+caudal+"#ACUMULADO#"+(System.currentTimeMillis() - tiempoInicio)*Float.parseFloat(caudal)/60000);
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
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
			KafkaConsumer<String, String> kafka = crearKafka(ipBroker, Integer.parseInt(puertoBroker));
		    
		    ipMonitor = args[2];
            puertoMonitor = args[3];
			Socket skMonitor = new Socket(ipMonitor, Integer.parseInt(puertoMonitor));
		    ValidarId(skMonitor);
			long tiempoRiego = 0;
			while(true)
			{
				tiempoRiego = EsperaRiego(kafka, skMonitor);
				regando(kafka, skMonitor, tiempoRiego);
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
