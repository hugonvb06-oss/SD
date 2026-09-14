package p1b;

import java.io.*;
import java.net.*;

public class WM_WS_M {

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
	
	
	
	
	
	
	
	
	public void menu(String p_host, String p_puerto)
	{
		String idEstacion;
		String ubicacion;
		String datos = "";
		try
		{
			Socket skCliente = new Socket( p_host , Integer.parseInt(p_puerto ));
			System.out.println("Introduce una ID de estacion:");
			InputStreamReader isr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader (isr);
			idEstacion = br.readLine();
			System.out.println("Introduce una ubicacion:");
			ubicacion = br.readLine();
			escribeSocket(skCliente, "REGISTRO#"+idEstacion+"#"+ ubicacion);
			datos = leeSocket(skCliente, datos);
			System.out.println(datos);
			skCliente.close();
		}catch(Exception e)
		{
			System.out.println("Error " + e.toString());
		}
		return;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		WM_WS_M cl = new WM_WS_M();
		int i = 0;
		String host;
		String puerto;
		if (args.length < 2) {
			System.out.println ("Debe indicar la direccion del servidor y el puerto");
			System.out.println ("$./Cliente nombre_servidor puerto_servidor");
			System.exit(-1);
		}
		host = args[0];
		puerto = args[1];
		cl.menu(host,puerto);
	
	}

}
