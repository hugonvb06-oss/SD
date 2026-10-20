import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class WM_WS_M {
	/* 
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
    static private String puertoServidor;
	static private String ipCentral;
    static private String puertoCentral; 
    static private String id;
    static private String ubicacion;
    static private String caudal;
    static private BlockingQueue<String> colaMensajesEngine;
    static private BlockingQueue<String> colaMensajesCentral;
	static public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = flujo.readUTF();
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
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
	
	
	public static Socket conectaSocket ( String p_ip, String p_puerto)
    {
        Socket p_sk = null;
        try
        {
            p_sk = new Socket(p_ip, Integer.parseInt(p_puerto));
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
            return null;
        }
      return p_sk;
    }
	
	public static boolean validaId( Socket p_sk)
    {
        String respuesta = "";
        escribeSocket(p_sk, "#ID#"+id+"#UBICACION#"+ubicacion);
        respuesta = leeSocket(p_sk, respuesta);
        if (respuesta.equals("\u0006"))
            return true;
        else
            return false;
    }
    public static boolean transmiteId(Socket p_sk)
    {
        String respuesta = "";
        escribeSocket(p_sk, "#ID#"+id+"#UBICACION#"+ubicacion+"#CAUDAL#"+caudal);
        respuesta = leeSocket(p_sk, respuesta);
        if (respuesta.equals("\u0006"))
            return true;
        else
            return false;
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
    public static String regando(Socket skEngine, Socket central)
    {
        String datosEngine = "";
        String datosCentral = "";
        float tiempoRiego = 0;
        boolean riegoActivo = true;
        try {
            while(riegoActivo)
            {
            
                // 1000 milisegundos equivalen a 1 segundo
                datosEngine = colaMensajesEngine.take();
                datosCentral = colaMensajesCentral.poll();
                if(datosEngine.contains("KO")) {
                    riegoActivo = false;
                    averia(central,skEngine);
                }else if(datosEngine.contains("RIEGO_FINALIZADO")) {
                    riegoActivo = false;
                    escribeSocket(central, "#ESTADO#RIEGO_FINALIZADO#");
                    return "espera";
                }else if(datosCentral != null && datosCentral.contains("BLOQUEAR")) {
                    riegoActivo = false;
                    return "bloqueo";
                }else if(datosEngine.contains("RIEGO_FINALIZADO")) {
                    riegoActivo = false;
                    escribeSocket(central, "#ESTADO#RIEGO_FINALIZADO#");
                    return "espera";
                }
                escribeSocket(central, datosEngine);
            
            
            
       
            
                System.out.println("Recibido: " + datos);
            }
        } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
            }
    }
    public static String bloqueo(Socket skEngine, Socket central){
    // 1. Ordena al Engine detener cualquier riego y bloquearse
    escribeSocket(skEngine, "#BLOQUEO#");
    System.out.println("Estación en Estado NARANJA: Fuera de Servicio.");
    boolean estaBloqueado = true;
    
    // Bucle estricto: Se queda aquí hasta recibir la orden contraria de la Central
    try {
        while (estaBloqueado) {
            String ordenCentral = colaMensajesCentral.take();
            if (ordenCentral != null && ordenCentral.contains("ACTIVAR")) {
                System.out.println("Orden de activación recibida. Volviendo a estado Verde/Reposo.");
                escribeSocket(skEngine, "#ACTIVAR#");
                estaBloqueado = false; // Rompe el bucle de bloqueo
            }
        
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    return "espera";
}
	public static String espera(Socket engine, Socket central) {
    String datosEngine = "";
    String datosCentral = "";
    
    

    try {
        while(true) {
            // 1. Envío continuo (cada segundo) de comprobación de salud al Engine
            
            
            // 2. Recogida de mensajes de los hilos (Engine y Central de forma asíncrona)
            datosEngine = colaMensajesEngine.poll();
            datosCentral = colaMensajesCentral.poll();
            
            // 3. Comprobación de estado de salud del Engine (Fuga / Avería / KO)
            if (datosEngine == null || datosEngine.contains("KO")) {
                System.out.println("¡Alerta! Avería o fuga detectada en el Engine.");
                return "averia"; // Retorna un estado de avería para que el hilo principal pueda manejarlo
            } 
            // 4. Comprobación de órdenes arbitrarias desde la Central (Ej. BLOQUEAR)
            else if (datosCentral != null && datosCentral.contains("BLOQUEAR")) {
                System.out.println("Orden de bloqueo recibida desde Central.");
                return "bloqueo"; // Retorna un estado de bloqueo para que el hilo principal pueda manejarlo
            } 
            // 5. Si todo va bien durante el riego o reposo, retransmitir telemetría a la Central
            else if (datosEngine != null && !datosEngine.isEmpty()) {
                escribeSocket(central, datosEngine);
            }
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("El hilo de supervisión fue interrumpido.");
    }
    return "error"; // Retorna un estado de error si ocurre una interrupción
}
	public static String averia(Socket p_sk, Socket p_skEngine)
    {
        boolean continua = true;
        String datos = "";  
        escribeSocket(p_sk, "#ESTADO#AVERIA#");
        while (continua) {
            datos = colaMensajesCentral.take(); // Espera a que la Central envíe una orden de desbloqueo
            if (datos.contains("ACTIVAR")) {
                continua = false;
            }
        }
        return "espera";
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i = 0;
		
		if (args.length < 6) {
			System.exit(-1);
		}
		String puertoServidor = args[0];
		ipCentral = args[1];
        puertoCentral = args[2];
        id = args[3];
        ubicacion = args[4];
        caudal = args[5];
        try {
            Socket central = conectaSocket(ipCentral, puertoCentral);
            if (central == null || !validaId(central)) { //valida la id con el servidor central
                System.out.println("Error: No se pudo conectar con la central");
                System.exit(-1);
            }
            ServerSocket skServidor = new ServerSocket(Integer.parseInt(puertoServidor));
            Socket skEngine = skServidor.accept();
        
            String datos = "";
            
            while(!transmiteId(skEngine));
            colaMensajesEngine = new ArrayBlockingQueue<>(100);
            HiloMonitor hiloEngine = new HiloMonitor(skEngine, colaMensajesEngine);
            hiloEngine.start();
            colaMensajesEngine.put("Monitorea");
            colaMensajesCentral = new ArrayBlockingQueue<>(100);
            HiloMonitor hiloCentral = new HiloMonitor(central, colaMensajesCentral);
            hiloCentral.start();
            colaMensajesCentral.put("EsperaContinua");
            datos = "espera";
            while(true){
                if(datos.equals("espera")){
                    datos = espera(skEngine);
                }else if(datos.equals("averia")) {
                    datos = averia(central, skEngine);
                    colaMensajesEngine.put("Monitorea");
                } else if(datos.equals("bloqueo")) {
                    hiloEngine.interrupt(); // Interrumpe el hilo de monitorización del Engine
                    datos = bloqueo(skEngine, central);
                    colaMensajesEngine.put("Monitorea");
                } else if(datos.equals("riego")) {
                    datos = regando(skEngine, central);
                }else {
                    System.out.println("Error en la supervisión y operación del sistema.");
                }
            }
        }catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        
        
        
	
	}
}

