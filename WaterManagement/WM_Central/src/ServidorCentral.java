
public class ServidorCentral{

    public static void main(String[] args){
        BaseDatos db = new BaseDatos("../../database/watermanagement.db");
        /*
        if(db.registrarWS(1, "Ontur, Castilla la mancha")){
            System.out.println("Ingresado correctamente");
        }else{
            System.out.println("Fallo de ingreso 100%perdido");
        }*/

       // db.mostrarWS();
        if (db.buscarWS(1)) {
            System.out.println("La estación existe");
        } else {
            System.out.println("La estación no existe");
        }
        db.cerrar(); 
    }

}