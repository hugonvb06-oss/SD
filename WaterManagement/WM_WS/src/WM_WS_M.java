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
			while(p_Datos.equals("\u0015")){
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

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	static public void escribeSocket (Socket p_sk, String p_Datos)
	{
        String mensajeFinal = "";
		try
		{
            if (!p_Datos.equals("\u0005") && !p_Datos.equals("\u0006") && !p_Datos.equals("\u0015")) {
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
        escribeSocket(p_sk, "\u0005");
        String respuesta = "";
        leeSocket(p_sk, "\u0005");
        escribeSocket(p_sk, "#ID:"+id+"#UBICACION:"+ubicacion+"#");
        respuesta = leeSocket(p_sk, respuesta);
        if (respuesta.equals("\u0006"))
            return true;
        else
            return false;
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
        escribeSocket(central, "#STATUS#RIEGO_ACTIVO#");
        String datosEngine = "";
        String datosCentral = "";
        float tiempoRiego = 0;
        try {
            while(true)
            {
            
                // 1000 milisegundos equivalen a 1 segundo
                datosEngine = colaMensajesEngine.take();
                datosCentral = colaMensajesCentral.poll();
                if(datosEngine != null && datosEngine.contains("KO")) {
                    return "averia";
                }else if(datosEngine != null && datosEngine.contains("RIEGO_FINALIZADO")) {
                    
                    escribeSocket(central, "#STATUS#RIEGO_FINALIZADO#");
                    return "espera";
                }else if(datosCentral != null && datosEngine.contains("BLOQUEO")) {
                    return "bloqueo";
                }else if(datosCentral != null && datosCentral.contains("\u0004")) {
                    return "error";
                }
                escribeSocket(central, datosEngine);
            }
        } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
            }
        return "error";
    }
    public static String bloqueo(Socket skEngine, Socket central){
        // 1. Ordena al Engine detener cualquier riego y bloquearse
        escribeSocket(skEngine, "#BLOQUEO#");
        System.out.println("Estación en Estado NARANJA: Fuera de Servicio.");
        boolean estaBloqueado = true;
        
        // Bucle estricto: Se queda aquí hasta recibir la orden contraria de la Central
        try {
            while (estaBloqueado) {
                String ordenCentral = colaMensajesEngine.take();
                String apagado = colaMensajesCentral.take();
                if (ordenCentral != null && ordenCentral.contains("ACTIVAR")) {
                    System.out.println("Orden de activación recibida. Volviendo a estado Verde/Reposo.");
                    estaBloqueado = false; // Rompe el bucle de bloqueo
                }else if(apagado != null && apagado.contains("\u0004")) {
                    return "error";
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
            
            datosEngine = colaMensajesEngine.take();
            datosCentral = colaMensajesCentral.poll();
            
            // 3. Comprobación de estado de salud del Engine (Fuga / Avería / KO)
            if (datosEngine != null && datosEngine.contains("KO")) {
                System.out.println("¡Alerta! Avería o fuga detectada en el Engine.");
                return "averia"; // Retorna un estado de avería para que el hilo principal pueda manejarlo
            }else if (datosEngine != null && datosEngine.contains("BLOQUEAR")) {
                System.out.println("Orden de bloqueo recibida desde Central.");
                return "bloqueo"; // Retorna un estado de bloqueo para que el hilo principal pueda manejarlo
            }else if (datosEngine != null && datosEngine.contains("RIEGO_ACTIVO")) {
                System.out.println("Inicio de riego detectado.");
                return "riego"; // Retorna un estado de riego para que el hilo principal pueda manejarlo
            }else if(datosCentral != null && datosCentral.contains("\u0004")) {
                return "error";
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
        try {
            escribeSocket(p_sk, "#STATUS#AVERIA#");
            while (continua) {
                datos = colaMensajesEngine.take(); // Espera a que la Central envíe una orden de desbloqueo
                if (datos.contains("ACTIVAR")) {
                    continua = false;
                }else if(datos != null && datos.contains("\u0004")) {
                    return "error";
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("El hilo de supervisión fue interrumpido.");
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
            skEngine.setSoTimeout(1000); // Establece un tiempo de espera de 1 segundo para las operaciones de lectura
            String datos = "";
            
            while(!transmiteId(skEngine));
            colaMensajesEngine = new ArrayBlockingQueue<>(100);
            HiloWS hiloEngine = new HiloWS(skEngine, colaMensajesEngine);
            hiloEngine.start();
            colaMensajesEngine.put("Monitorea");
            colaMensajesCentral = new ArrayBlockingQueue<>(100);
            HiloWS hiloCentral = new HiloWS(central, colaMensajesCentral);
            hiloCentral.start();
            colaMensajesCentral.put("EsperaContinua");
            datos = "espera";
            while(true){
                if(datos.equals("espera")){
                    datos = espera(skEngine, central);
                }else if(datos.equals("averia")) {
                    datos = averia(central, skEngine);
                } else if(datos.equals("bloqueo")) {
                    datos = bloqueo(skEngine, central);
                } else if(datos.equals("riego")) {
                    datos = regando(skEngine, central);
                }else {
                    System.out.println("Error en la supervisión y operación del sistema.");
                    escribeSocket(skEngine, "\u0004");
                    break;
                }
            }
            central.close();
            skEngine.close();
            while(hiloEngine.isAlive()) hiloEngine.interrupt();
            while(hiloCentral.isAlive()) hiloCentral.interrupt();
        }catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        
        
        
	
	}
}

