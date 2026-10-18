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
	
	
	
	public static Socket conectaSocket ( String p_ip, String p_puerto)
    {
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
	
	public static boolean validaId(String p_id, Socket p_sk)
    {
        String respuesta = "";
        escribeSocket(p_sk, "\u0005"+"#ID#"+p_id);
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
        boolean riegoActivo = false;
        while(rieagoActivo)
        {
            try {
                escribeSocket(skEngine, lrc("\u0002"+"#DATOS#\u0003"));
                colaMensajes.put("Esperando");
                // 1000 milisegundos equivalen a 1 segundo
                Thread.sleep(1000);
                datos = colaMensajes.poll();
                if(datos == null || datos.contains("KO")) {
                    averia(skEngine, central);
                }else{
                    escribeSocket(central, lrc("\u0005"+"#ESTADO_RIEGO#"+datos));
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
            datos = leeSocket(engine, datos);
            if(datos.contains("riego")){
                System.out.println("Riego iniciado");
            }
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
      return;
    }
	public static void averia(Socket p_sk, Socket p_skEngine)
    {
        escribeSocket(p_sk, "\u0005"+"#AVERIA#"+p_averia);
        String respuesta = "";
        respuesta = leeSocket(p_sk, respuesta);
        if (respuesta.equals("\u0006"))
            System.out.println("Averia enviada correctamente");
        else
            System.out.println("Error al enviar averia");
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i = 0;
		
		if (args.length < 4) {
			System.out.println ("Debe indicar la direccion del servidor y el puerto");
			System.out.println ("$./Cliente nombre_servidor puerto_servidor");
			System.exit(-1);
		}
		puertoServidor = args[0];
		ipCentral = args[1];
        puertoCentral = args[2];
        id = args[3];
		Socket central = conectaSocket(ipCentral, puertoCentral);
        ServerSocket skServidor = new ServerSocket(Integer.parseInt(puertoServidor));
        System.out.println("Escucho el puerto " + puerto);
        Socket skEngine = skServidor.accept();
        if (central == null || !validaId(id, central)) { //valida la id con el servidor central
            escribeSocket(sk, lrc("\u0005"+"#Invalido#\u0006"));
            System.out.println("Error: No se pudo conectar con la central");
            System.exit(-1);
        }

        
        while(!validarId(id, skEngine));
        while(true){
            esperaRiego(skEngine);
            regando(skEngine, central);
        }
        
        
        
        
	
	}
}

