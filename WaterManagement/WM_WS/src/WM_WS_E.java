
import java.net.ServerSocket;
import java.net.Socket;

public class WM_WS_E {

	/**
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
        String ipBroker="";
        String ipMonitor="";
		String puertoBroker="";
		String puertoMonitor = "";
		try
		{
			
			if (args.length < 4) {
				System.out.println("Debe indicar el puerto de escucha del servidor");
				System.out.println("$./Servidor puerto_servidor");
				System.exit (1);
			}
			ipBroker = args[0];
			puertoBroker = args[1];
			ServerSocket skBroker = new ServerSocket(Integer.parseInt(puertoBroker));
		    System.out.println("Escucho el puerto " + puertoBroker);
		    
		    ipMonitor = args[2];
            puertoMonitor = args[3];
			ServerSocket skMonitor = new ServerSocket(Integer.parseInt(puertoMonitor));
		    
	
			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			for(;;)
			{
				/*
				* Se espera una peticion de la central
				*/
				skBoot.accept();
		       
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
			
		}
	}

}
