package qtx.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import qtx.entidades.Articulo;
import qtx.persistencia.ManejadorBD;

public class Cliente01BD {

	public static void testConsulta() {
		System.out.println("==================== "+ "testConsulta()" + " ====================");
		ManejadorBD dao = new ManejadorBD();
		Connection conBD = dao.conectarBD();
		String sentenciaSql = "Select cve_articulo from articulo order by cve_articulo";
		
		try {
			Statement peticionBD = conBD.createStatement();
			if(peticionBD.execute(sentenciaSql))
				System.out.println("Petición exitosa");
			ResultSet resultSet = peticionBD.getResultSet();
			System.out.print("Claves recuperadas: ");
			while(resultSet.next()){
				String cveArticulo = resultSet.getString("cve_articulo");
				System.out.print(cveArticulo + ", " );
			}
			System.out.println();
			
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			if(conBD != null){
				try {
					conBD.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	public static void testConsultaClavesArt_todas() {
		System.out.println("=============== "+ "testConsultaClavesArt_todas()" + " ===============");
		ManejadorBD dao = new ManejadorBD();
		System.out.println("claves de artículos: " + dao.getClavesArticulo());
	}
	public static void testInsercion() {
		System.out.println("==================== "+ "testInsercion()" + " ====================");
		Articulo articulo = new Articulo("ACR-01","Tapón de Gasolina Nissan Tsuru 1998",325.5f, 699.90f);
		ManejadorBD dao = new ManejadorBD();
		int nIserciones = dao.insertarArticulo(articulo);
		System.out.println("Se han insertado " + nIserciones + " registros");
	}
	private static void testEliminacion() {
		System.out.println("==================== "+ "testEliminacion()" + " ====================");
		ManejadorBD dao = new ManejadorBD();
		int nBorrados = dao.eliminarArticulo("ACR-01");
		System.out.println("Se han eliminado " + nBorrados + " registros");
	}
	private static void testConsultaXID() {
		System.out.println("==================== "+ "testConsultaXID()" + " ====================");
		ManejadorBD dao = new ManejadorBD();
		Articulo articulo = dao.getArticuloXID("ACR-01");
		System.out.println("Artículo recuperado por ID: " + articulo);
	}
	private static void testModificacion() {
		System.out.println("==================== "+ "testModificacion()" + " ====================");
		ManejadorBD dao = new ManejadorBD();
		Articulo articulo = dao.getArticuloXID("ACR-01");
		float costoNuevo = articulo.getCostoProv1() * 1.40f;
		float precioNuevo = articulo.getPrecioLista() * 1.40f;
		articulo.setCostoProv1(costoNuevo);
		articulo.setPrecioLista(precioNuevo);
		int nModif = dao.modificarArticulo(articulo);
		System.out.println("Se han modificado " + nModif + " registros");
	}
	
	public static void main(String[] args) {
		testConsulta();
		
		testConsultaClavesArt_todas();
		testInsercion();
		testConsultaXID();
		testConsultaClavesArt_todas();
		testModificacion();
		testConsultaXID();
		testEliminacion();
		testConsultaClavesArt_todas();
	}

}
