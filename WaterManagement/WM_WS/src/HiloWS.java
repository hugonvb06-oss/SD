import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloWS extends Thread {

	private Socket skCliente;
	private BlockingQueue<String> colaMensajes;
	

	public HiloWS(Socket p_cliente, BlockingQueue<String> p_colaMensajes) {
		this.skCliente = p_cliente;
		this.colaMensajes = p_colaMensajes;
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
	public static String lrc(String p_datos)
	{
        byte[] bytes = p_datos.getBytes();
        int lrc = 0;
        for (byte b : bytes) {
            lrc ^= b;
        }
        return p_datos+Integer.toHexString(lrc);
    }
	public void esperaContinua(){
		String datos = "";
		try {
			while(!Thread.interrupted()){
				datos = leeSocket(skCliente, datos);
				if(!datos.contains("Datos?")){
					escribeSocket(skCliente, "\u0006");
				}
				
				colaMensajes.put(datos);
			
			}
		} catch (InterruptedException e) {
			// Se ejecuta si otro hilo interrumpe la pausa
			Thread.currentThread().interrupt();
			System.err.println("El hilo fue interrumpido");
		}
	}
	public void esperaEntradaPantalla(){
		String datos = "";
		boolean entradaPantalla = true;
		Scanner scanner = new Scanner(System.in); // 2. Crear el objeto Scanner
		try {
			while(entradaPantalla&&!Thread.interrupted()){
				

        		System.out.print("Posibles comandos: KO, RIEGO_ACTIVO, RIEGO_FINALIZADO");
        		datos = scanner.nextLine();
				if(datos.equals("KO")){
					entradaPantalla = false;
					
					System.out.print("Averia detectada");
				}
				colaMensajes.put(datos);
			
			}
		} catch (Exception e) {
			// Se ejecuta si otro hilo interrumpe la pausa
			Thread.currentThread().interrupt();
			System.err.println("El hilo fue interrumpido");
		}
		scanner.close();
	}
	static public String leeSocket (Socket p_sk, String Datos)
	{
		String p_Datos;
		try
		{
			
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
    public String esperaCola(){
        String mensaje = "";
        try {
            mensaje = colaMensajes.take();
            
        } catch (InterruptedException e) {
            System.out.println("Error: " + e.toString());
        }
        return mensaje;
    }
	public void escribeCola(String p_Datos){
        try {
            colaMensajes.put(p_Datos);
        } catch (InterruptedException e) {
            System.out.println("Error: " + e.toString());
        }
    }
    
    public void run() {
		int resultado=0;
		String Cadena="";
		
        try {
			while (!Thread.interrupted())
			{
				Cadena = esperaCola();
				
				if(Cadena.equals("EsperaContinua")) {
					esperaContinua();
				}else if(Cadena.equals("EsperaPantalla")){
					esperaEntradaPantalla();
				}			
			}
			skCliente.close();
			//System.exit(0); No se debe poner esta sentencia, porque en ese caso el primer cliente que cierra rompe el socket 
			//				  y desconecta a todos				
        }
        catch (Exception e) {
          System.out.println("Error: " + e.toString());
        }
      }
}
