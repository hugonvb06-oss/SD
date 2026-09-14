import java.net.Socket;


public class HiloCentral extends THread{
    private Socket socket;

    public HiloCentral(Socket p_maquina){
        this.socket = p_maquina;
    }
}