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
    public static void regando(Socket skEngine, Socket central)
    {
        String datos = "";
        float tiempoRiego = 0;
        BlockingQueue<String> colaMensajes = new ArrayBlockingQueue<>( 100);
        HiloMonitor hiloMonitor = new HiloMonitor(skEngine, colaMensajes);
        hiloMonitor.start();
        boolean riegoActivo = true;
        while(riegoActivo)
        {
            try {
                escribeSocket(skEngine, lrc("#DATOS?#"));
                colaMensajes.put("Esperando");
                // 1000 milisegundos equivalen a 1 segundo
                Thread.sleep(1000);
                datos = colaMensajes.poll();
                if(datos == null || datos.contains("KO")) {
                    riegoActivo = false;
                    averia(central,skEngine);
                }else{
                    escribeSocket(central, datos);
                }
            } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
            }

       
            
            System.out.println("Recibido: " + datos);
        }
    }
	public static void esperaRiego(Socket engine)
    {
        
        try
        {
            String datos = "";
            while(!datos.contains("RIEGO_ACTIVO"))
                datos = leeSocket(engine, datos);
            
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
      return;
    }
	public static void averia(Socket p_sk, Socket p_skEngine)
    {
        escribeSocket(p_sk, "#AVERIA#");
        String respuesta = "";
        respuesta = leeSocket(p_sk, respuesta);
        while(respuesta.equals("\u0006")){
            escribeSocket(p_sk, "#AVERIA#");
        
            respuesta = leeSocket(p_sk, respuesta);
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
        

            
            while(!transmiteId(skEngine));
            while(true){
                esperaRiego(skEngine);
                regando(skEngine, central);
            }
        }catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        
        
        
	
	}
}

