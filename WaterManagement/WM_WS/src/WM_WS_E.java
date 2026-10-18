
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
public class WM_WS_E {

	public static String ID = "";
	public static void averia(Socket p_sk, Socket p_skEngine)
	{
		WM_WS_M.escribeSocket(p_sk, "\u0005"+"#AVERIA#"+p_averia);
		String respuesta = "";
		respuesta = WM_WS_M.leeSocket(p_sk, respuesta);
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
	
	public static KafkaConsumer<String, String> crearKafka(String ip, int puerto, int particion) {

    Properties props = new Properties();

    props.put("bootstrap.servers", ip + ":" + puerto);
    props.put("group.id", "grupo-java-" + ID);
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
	
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    consumer.assign(Collections.singletonList(
            new TopicPartition("mi-topico", particion)));

    return consumer;
}

public static ArrayList<String> leerKafka(KafkaConsumer<String, String> consumer) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

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
		try
		{
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(p_Datos);      
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
		
		if (validar.contains("Invalido")){
			System.out.println("Error: No se pudo conectar con la central");
            System.exit(-1);
		}
		while (!validaLrc(validar)){
			System.out.println("Error: LRC invalido");
			escribeSocket(p_sk, "\u0015");
			validar =leeSocket(p_sk, validar);
		}
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
        byte[] bytes = p_datos.getBytes();
		
        int lrc = 0;
        for (byte b : bytes) {
            lrc ^= b;
        }
        return lrc == 0;
    }
	static public void EsperaRiego(KafkaConsumer<String, String> kafka, Socket Monitor)
	{
		boolean riegoActivo = false;
		try
		{
			while(!riegoActivo)
			{
				ArrayList<String> mensajes = leerKafka(kafka, ID);
				for (String mensaje : mensajes) {
					if (mensaje.contains("RIEGO_ACTIVO")) {
						riegoActivo = true;
						System.out.println("Riego activo");
						escribeSocket(Monitor, "\u0005"+"#riego#");
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
	  return;
	}
	static public void regando(KafkaConsumer<String, String> kafka, Socket Monitor, int tiempoRiego)
	{
		boolean riegoActivo = true;
		try
		{
			while(riegoActivo)
			{
				ArrayList<String> mensajes = leerKafka(kafka, ID);
				for (String mensaje : mensajes) {
					if (mensaje.contains("AVERIA")) {
						riegoActivo = false;
						System.out.println("Riego inactivo");
						escribeSocket(Monitor, "\u0005"+"#riego_inactivo#");
					}
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
			Socket skMonitor = new Socket(Integer.parseInt(puertoMonitor), ipMonitor);
		    ValidarId(skMonitor);
			while(true)
			{
				EsperaRiego(kafka, skMonitor);

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
