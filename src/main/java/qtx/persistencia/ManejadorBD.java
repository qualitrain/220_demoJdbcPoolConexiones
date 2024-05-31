package qtx.persistencia;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import qtx.entidades.Articulo;
import qtx.entidades.DetalleVenta;
import qtx.entidades.Persona;
import qtx.entidades.Venta;

public class ManejadorBD {

	private static final String CAD_CONEXION_MYSQL5_1 = 
			"jdbc:mysql://localhost:3306/ejemplosJDBC?serverTimezone=UTC" ;
	
	static private ComboPooledDataSource poolDeConexionesC3po=getConnectionPool();
	
	public ManejadorBD() {
	}

	public static ComboPooledDataSource getConnectionPool() {
		
		try {
			ComboPooledDataSource poolDeConexionesC3po = new ComboPooledDataSource();
			poolDeConexionesC3po.setDriverClass("com.mysql.jdbc.Driver");
			poolDeConexionesC3po.setJdbcUrl(CAD_CONEXION_MYSQL5_1);
			poolDeConexionesC3po.setUser("root");
			poolDeConexionesC3po.setPassword("root");
			poolDeConexionesC3po.setMaxPoolSize(30);
			return poolDeConexionesC3po;
		} 
	    catch (PropertyVetoException e) { 
			e.printStackTrace();
			return null;
		}
	}
	
	public Connection conectarBD() {
		Connection conBD = null;
		try {
			conBD = ManejadorBD.poolDeConexionesC3po.getConnection();
			System.out.println("Usando conexion " + conBD.getClass().getSimpleName() + ", " + conBD.hashCode());

//			conBD = DriverManager.getConnection(CAD_CONEXION_MYSQL5_1, "root", "root");
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}

		return conBD;
	}
	public List<String> getClavesArticulo() {
		final String SENTENCIA_SQL = "SELECT cve_articulo FROM articulo ORDER BY cve_articulo";
		List<String> clavesArticulo = new ArrayList<String>();

		try (Connection conBD = conectarBD()){
			Statement statementBD = conBD.createStatement();
			statementBD.execute(SENTENCIA_SQL);
			ResultSet resultSet = statementBD.getResultSet();
			while(resultSet.next()){
				clavesArticulo.add( resultSet.getString("cve_articulo") );
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return clavesArticulo;
	}
	public Articulo getArticuloXID(String cveArticulo) {
		final String SENTENCIA_SQL = "SELECT * FROM articulo "
								   + "WHERE cve_articulo ='" + cveArticulo + "'";
		Articulo articulo = null;

		try (Connection conBD = conectarBD()){
			Statement statementBD = conBD.createStatement();
			statementBD.execute(SENTENCIA_SQL);
			ResultSet resultSet = statementBD.getResultSet();
			if(resultSet.next()){
				articulo = new Articulo();
				articulo.setCveArticulo( resultSet.getString("cve_articulo") );
				articulo.setDescripcion( resultSet.getString("descripcion"));
				articulo.setCostoProv1( resultSet.getFloat("costo_prov_1") );
				articulo.setPrecioLista( resultSet.getFloat("precio_lista") );
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return articulo;
	}
	public int insertarArticulo(Articulo articulo) {
		int numAfectacionesBD = 0;
		final String INSERT_SQL = "INSERT INTO articulo "
				+ "(cve_articulo, descripcion, costo_prov_1, precio_lista) "
				+ "VALUES ('"
				+ articulo.getCveArticulo() + "','"
				+ articulo.getDescripcion() + "',"
				+ Float.toString(articulo.getCostoProv1()) + ","
				+ Float.toString(articulo.getPrecioLista()) + ")";
		
		try (Connection conBD = conectarBD()){
			Statement stmt = conBD.createStatement();
			numAfectacionesBD = stmt.executeUpdate( INSERT_SQL );
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return numAfectacionesBD;
	}
	public int modificarArticulo(Articulo articulo) {
		int numAfectacionesBD = 0;
		final String UPDATE_SQL = "UPDATE articulo "
				+ "SET descripcion = '" + articulo.getDescripcion() + "', "
				+ "costo_prov_1 = " + articulo.getCostoProv1() + ", "
				+ "precio_lista = " + articulo.getPrecioLista() + " "
				+ "WHERE cve_articulo = '" + articulo.getCveArticulo() + "'";
		
		try (Connection conBD = conectarBD()){
			Statement stmt = conBD.createStatement();
			numAfectacionesBD = stmt.executeUpdate( UPDATE_SQL );
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return numAfectacionesBD;
	}
	public int eliminarArticulo(String cveArticulo) {
		int numAfectacionesBD = 0;
		final String DELETE_SQL = "DELETE FROM articulo "
								+ "WHERE cve_articulo ='"
				                + cveArticulo + "'";
		
		try (Connection conBD = conectarBD()){
			Statement stmt = conBD.createStatement();
			numAfectacionesBD = stmt.executeUpdate( DELETE_SQL );
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return numAfectacionesBD;
	}

	public Persona getPersonaXID(int idPersona) {
		final String SENTENCIA_SQL = "SELECT * FROM persona "
								   + "WHERE id_persona =" + idPersona + "";
		Persona persona = null;

		try (Connection conBD = conectarBD()){
			Statement statementBD = conBD.createStatement();
			statementBD.execute(SENTENCIA_SQL);
			ResultSet resultSet = statementBD.getResultSet();
			if(resultSet.next()){
				String nombre = resultSet.getString("nombre");			
				String direccion = resultSet.getString("direccion");
				Date fechaNacimiento = resultSet.getDate("fecha_nacimiento");

				persona = new Persona(idPersona,nombre,direccion,fechaNacimiento);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return persona;
	}
	
	public Venta getVentaXID( int numVenta ) {
		
		Venta venta=null;
		
		final String SENTENCIA_SQL = "SELECT * FROM venta "
				 				   + "WHERE num_venta =" + numVenta;
		try (Connection conBD = conectarBD()){
			Statement stmt = conBD.createStatement();
			ResultSet resultSet = stmt.executeQuery(SENTENCIA_SQL);
			
			if(resultSet.next()){ // El cursor se avanza para posicionarlo en el renglon le�do
				Date fechaVenta = resultSet.getDate("fecha_venta");
				int idPersonaCte = resultSet.getInt("id_persona_cte");
				int idPersonaVendedor = resultSet.getInt("id_persona_vendedor");
				
				Persona personaCte = this.getPersonaXID(idPersonaCte);
				Persona personaVendedor = this.getPersonaXID(idPersonaVendedor);
				venta = new Venta(numVenta,fechaVenta,personaCte,personaVendedor);
			
				List<DetalleVenta> listaDetallesVenta = this.getDetallesVenta(numVenta);
				for(DetalleVenta det:listaDetallesVenta){
					int numDetalle = det.getNumDetalle();
					int cantidad = det.getCantidad();
					float precioUnitario = det.getPrecioUnitario();
					Articulo articulo = det.getArticuloVendido();
					venta.agregarDetalle(numDetalle, cantidad, articulo, precioUnitario);
				}
			}
		}catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return venta;
	}
	
	
	public List<DetalleVenta> getDetallesVenta(int numVenta) {
		ArrayList<DetalleVenta> listaDetallesVenta = new ArrayList<DetalleVenta>();
		final String SENTENCIA_SQL = "SELECT * FROM detalle_venta "
				   + "WHERE num_venta =" + numVenta;
		
		try (Connection conBD = conectarBD()){
			Statement stmt = conBD.createStatement();			
			ResultSet resultSet = stmt.executeQuery(SENTENCIA_SQL);
			 while(resultSet.next()){
				int numDetalle = resultSet.getInt("num_detalle");
				int cantidad = resultSet.getInt("cantidad");
				String cveArticulo = resultSet.getString("cve_articulo");
				float precioUnitario = resultSet.getFloat("precio_unitario");
				
				listaDetallesVenta.add(new DetalleVenta(numDetalle, cantidad, new Articulo(cveArticulo), precioUnitario));
			}
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		for(DetalleVenta det:listaDetallesVenta){
			String cveArticulo = det.getArticuloVendido()
					                .getCveArticulo();
			det.setArticuloVendido(this.getArticuloXID(cveArticulo));
		}
		return listaDetallesVenta;
		
	}

	public int insertarVentaTransaccional (Venta nuevaVenta){
		// Inserta una relacion maestro-detalle con relaciones hacia otros objetos en la base de datos.
		// Tambien utiliza el concepto de llave auto-generada
		int numVenta = 0;
		final String strInsertVenta = "INSERT INTO venta SET " 
									+ "fecha_venta =  curdate()," 
				                    + "id_persona_cte = " 
									+ nuevaVenta.getCliente()
									            .getIdPersona() + "," 
				                    + "id_persona_vendedor = " 
									+ nuevaVenta.getVendedor()
									            .getIdPersona();
		
		try (Connection conBD = conectarBD()){
			conBD.setAutoCommit(false); 
			conBD.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			try {
				Statement stmtInsercionVenta = conBD.createStatement();
				stmtInsercionVenta.executeUpdate(strInsertVenta, Statement.RETURN_GENERATED_KEYS );
				
				ResultSet rsLlavesAutoGeneradas = stmtInsercionVenta.getGeneratedKeys();
				rsLlavesAutoGeneradas.first();
				numVenta = rsLlavesAutoGeneradas.getInt(1);
				
				for(DetalleVenta detVta: nuevaVenta.getDetallesVta().values()){					
					String sqlInsertDetalleVenta = getSqlInsertDetalleVenta(numVenta, detVta);
					stmtInsercionVenta.executeUpdate(sqlInsertDetalleVenta);			
				}
				conBD.commit(); // *** EN ESTE MOMENTO SE ACTUALIZA TODA LA TRANSACCION
			}
			catch (Exception e) {
				conBD.rollback();
				conBD.setAutoCommit(true);
				throw e;
			}
		}
		catch(SQLException sqlEx) {
			System.out.println(sqlEx.getMessage());
		}
		return numVenta;
	}
	private String getSqlInsertDetalleVenta(int numVenta, DetalleVenta detVta) {
		String sqlInsertDetalleVenta = "INSERT INTO detalle_venta SET "
                + "num_venta = " + numVenta + "," 
                + "num_detalle =" + detVta.getNumDetalle() + "," 
                + "cantidad =" + detVta.getCantidad() + "," 
                + "cve_articulo ='" 
                + detVta.getArticuloVendido()
                        .getCveArticulo() + "'," 
                + "precio_unitario =" 
                + detVta.getArticuloVendido()
                        .getPrecioLista();

		return sqlInsertDetalleVenta;
	}
}
