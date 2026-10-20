
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
	public void monitorizacionEngine()
    {
        String datos = "";
        boolean monitorActivo = true;
		try {
			while(monitorActivo&&!Thread.interrupted())
			{
            
                escribeSocket(skCliente, "#DATOS?#");
                datos = leeSocket(skCliente, datos);
				Thread.sleep(1000);
                if(datos == null || datos.contains("KO")) {
                    monitorActivo = false;
                    colaMensajes.put("KO");
                }else{
                    colaMensajes.put(datos);
                }
            
            	System.out.println("Recibido: " + datos);
			}
        } catch (InterruptedException e) {
                // Se ejecuta si otro hilo interrumpe la pausa
                Thread.currentThread().interrupt();
                System.err.println("El hilo fue interrumpido");
        }
    }
	public void esperaContinua(){
		String datos = "";
		try {
			while(true){
				datos = leeSocket(skCliente, datos);
				escribeSocket(skCliente, "\u0006");
				colaMensajes.put(datos);
			
			}
		} catch (InterruptedException e) {
			// Se ejecuta si otro hilo interrumpe la pausa
			Thread.currentThread().interrupt();
			System.err.println("El hilo fue interrumpido");
		}
	}
	public String leeSocket (Socket p_sk, String p_Datos)
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
			while (true)
			{
				Cadena = esperaCola();
				
				if(Cadena.equals("EsperaContinua")) {
					esperaContinua();
				}else if(Cadena.equals("Monitorea")){
					monitorizacionEngine(); 
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
