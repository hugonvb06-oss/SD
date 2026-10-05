
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
        if (central == null)
        {
            System.out.println("Error: No se pudo conectar con la central");
            System.exit(-1);
        }
        if(!validaId(id, central))
        {
            System.out.println("Error: Id no valida");
            System.exit(-1);
        }
        ServerSocket skServidor = new ServerSocket(Integer.parseInt(puertoServidor));
        System.out.println("Escucho el puerto " + puerto);
        Socket skCliente = skServidor.accept();
        validarId(id, skCliente);
        for(;;)
        {
            String datos = "";
            escribeSocket(skCliente, lrc("\u0002"+"#DATOS#\u0003"));
            try {
                // 1000 milisegundos equivalen a 1 segundo
                Thread.sleep(1000);
                datos = leeSocket(skCliente, datos);
            } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
            }

       
            
            System.out.println("Recibido: " + datos);
        }
	
	}
}

