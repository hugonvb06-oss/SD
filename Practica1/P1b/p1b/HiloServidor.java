package p1b;
import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloServidor extends Thread {

	private Socket skCliente;
	private int x = 0, y = 0;
	public HiloServidor(Socket p_cliente)
	{
		this.skCliente = p_cliente;
	}
	
	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
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

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos)
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
	
	public String[] validar(String datos) {
		String[] campos = datos.split("#");
		if(!"REGISTRO".equals(campos[0])) {
			System.out.println("Validación negativa no aparece REGISTRO");
			return null;
		}
		if(campos.length!=3) {
			System.out.println("Validación negativa numero de argumentos distinto");
			return null;
		}
		
		return campos; 
			
		
	}
	
	
	
	
    public void run() {
		String Cadena="";
		String[] campos;
        try {
			
			Cadena = this.leeSocket (skCliente, Cadena);
			/*
			* Se escribe en pantalla la informacion que se ha recibido del
			* cliente
			*/
			campos =validar(Cadena);
			if(campos!=null) {
				this.escribeSocket (skCliente, "STATUS#OK#Estacion registrada correctamente");
				System.out.println("La id recibida es:"+campos[1]);
				System.out.println("La ubicacion recibida es:"+campos[2]);
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
