
public class ServidorCentral{

    public static void main(String[] args){
        BaseDatos db = new BaseDatos("../../database/watermanagement.db");
        db.cerrar();
    }

}