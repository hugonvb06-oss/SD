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
	static private String ipCentral;
    static private String puertoCentral; 
    static private String id;
    static private String ubicacion;
    static private String caudal;
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
			return p_Datos;
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
        return null;
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
    public static String obtenerDatos(String datos){
        return datos.substring(2, datos.length());
    }
    public static void monitoreo(Socket skEngine, Socket central){
        String datos = "";
        String apagado = "";
        boolean averia = false;
		try {
			while(true)
			{
                apagado = colaMensajesCentral.poll();
                escribeSocket(skEngine, "#DATOS?#");
                datos = leeSocket(skEngine, "#DATOS?#");
				Thread.sleep(1000);
                if(datos == null || datos.contains("KO") || datos.contains("AVERIA")) {
                    if(!averia){
                        averia = true;
                        escribeSocket(central, "#ID#"+id+"#STATUS#AVERIA#");
                    }
                    
                }else if(apagado != null && apagado.equals("\u0004")){
                    escribeSocket(skEngine,"\u0004");
                    leeSocket(skEngine, "\u0004");
                    return;
                }else if(datos!=null){
                    averia = false;
                    if(datos.split("#STATUS#")[1].split("#")[0].equals("RIEGO")){
                        escribeSocket(central, "#ID#"+id+obtenerDatos(datos));
                    }
                }
            
            	System.out.println("Recibido: " + datos);
			}
        } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
        }
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
            colaMensajesCentral = new ArrayBlockingQueue<>(100);
            HiloWS hiloCentral = new HiloWS(central, colaMensajesCentral);
            hiloCentral.start();
            colaMensajesCentral.put("EsperaContinua");
            datos = "espera";
            monitoreo(skEngine, central);
            
            central.close();
            skEngine.close();
            while(hiloCentral.isAlive()) hiloCentral.interrupt();
        }catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        
        
        
	
	}
}

