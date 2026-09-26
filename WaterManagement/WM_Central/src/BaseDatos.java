import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/*Estas librerias sirven para ejecutar sentencias SQL
o modifica, en caso de ResultSet guarda y recorre en java
los daots que devuelve una consulta tipo SELECT*/
import java.sql.Statement;
import java.sql.ResultSet;

/*Es una version mejorada de Statement, se usa para evitar
injection, es un escudo contra ataques ciberneticos */
import java.sql.PreparedStatement;

public class BaseDatos{
    /*atributo para tabajar con la conexion que viene de la
    Clase Connection que fue importado*/
    private Connection conexion;

    /*Constructor de la base de datos, que se
    conectaria con la base de daatos de SQLite*/
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

    /*Metodo de base de datos para cerrar la conexion*/
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

    /*
    import java.sql.Statement;
    import java.sql.ResultSet;
    Con esas dos librerias podemos hacer este metodo
    */
    public void mostrarWS(){
        String sql = "SELECT * FROM WS";

        try{
            Statement stmt = conexion.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()){
                int id = rs.getInt("id_ws");
                String ubicacion = rs.getString("ubicacion");
                String estado = rs.getString("estado");
                
                System.out.println("WS "+ id +
                                    " | Ubicacion: " + ubicacion +
                                    " | Estado: " + estado);
            }

            stmt.close();
            rs.close();
        }catch(SQLException e){
            System.out.println("Error al consultar WS: "+
                                e.getMessage());
        }

    }


    /*Aqui se hara un INSERT el cual ingresara una nueva WS */
    public boolean registrarWS(int id, String ubi){
        String sql = "INSERT INTO WS (id_ws, ubicacion, ultima_conexion, estado, cant_agua) " + "VALUES (?, ?, ?, ?, ?)";
        boolean ok;
        try{
            PreparedStatement ps = conexion.prepareStatement(sql);

            ps.setInt(1, id);
            ps.setString(2, ubi);
            ps.setString(3, "");
            ps.setString(4, "ACTIVA");
            ps.setDouble(5, 0.0);

            ps.executeUpdate();
            ps.close();
            ok=true;
        }catch(SQLException e){
            System.out.println("Error al registrar WS: "+ e.getMessage());
            ok = false;
        }
        return ok;
    }

    /*Metodo que se encarga de mostrar la informacion de la WS */
    public boolean buscarWS(int id){
        String sql = "SELECT * FROM WS WHERE id_ws = ?";
        boolean ok = false;;
        try{
            PreparedStatement ps = conexion.prepareStatement(sql);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                System.out.println("WS encontrada");
                System.out.println("ID: " + rs.getInt("id_ws"));
                System.out.println("Ubicacion: "+ rs.getString("ubicacion"));
                System.out.println("Ultima conexion: " + rs.getString("ultima_conexion"));
                System.out.println("Estado: " + rs.getString("estado"));
                System.out.println("Cantidad de agua: " + rs.getDouble("cant_agua"));

                rs.close();
                ps.close();
                ok = true;
            }

            rs.close();
            ps.close();
        }catch(SQLException e){
            System.out.println("Error en la busqueda: " + e.getMessage());
            ok = false;
        }

        return ok;
    } 

    /*Metodo que se encarga de actualizar el estado de WS */
    public boolean actualizarEstadoWS(int id, String estado){
        String sql = "UPDATE WS SET estado = ? WHERE id_ws = ?";
        boolean ok = false;
        try{
            PreparedStatement ps = conection.prepareStatement(sql);
            ps.setString(1, estado);
            ps.setInt(2, id);

            int filas = ps.executeUpdate();
            ps.close();

            if(filas>0){
                ok = true;
            }

        }catch(){
            System.out.println("Error al actualizar: "+ e.getMessage());
            ok = false;
        }
        return ok;
    }

}