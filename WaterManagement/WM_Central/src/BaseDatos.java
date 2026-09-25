import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BaseDatos{
    private Connection conexion;

    public BaseDatos(String rutaBD){
        try{
            String url = "jdbc:sqlite:" + rutaBD;
            conexion = DriverManager.getConnection(url);
            System.out.println("Conectado con la base de datos");
        }catch(SQLException e){
            System.out.println("Error al conectar con la BD: " 
                                + e.getMessage());
        }
    }

    public void cerrar(){
        try{
            if(conexion != null){
                conexion.close();
                System.out.println("Conexion cerrada");
            }
        }catch(SQLException e){
            System.out.println("Error al cerrar la BD: "
                                + e.getMessage());
        }
    }
}