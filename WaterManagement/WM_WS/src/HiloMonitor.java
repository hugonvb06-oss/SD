
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloMonitor extends Thread {

	private Socket skCliente;
	private BlockingQueue<String> colaMensajes;

	public HiloMonitor(Socket p_cliente, BlockingQueue<String> p_colaMensajes)
	{
		this.skCliente = p_cliente;
		this.colaMensajes = p_colaMensajes;
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
				
				if(Cadena.equals("Esperando")) {
					Cadena = leeSocket (skCliente, Cadena);
                    escribeCola(Cadena);
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
