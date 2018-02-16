import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JDBCLab {
	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;

	public int connectDB(){
		Properties login = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream("login.properties");
			login.load(input);
			try {
				conn = DriverManager.getConnection("jdbc:mysql://classdb.it.mtu.edu/mglazier", login);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
				return 1;
			}
			return 0;
		} catch (IOException ex) {
			ex.printStackTrace();
			return 1;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void disconnect(){
		try{
			conn.close();
		}
		catch(SQLException ex){
			//handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			
		}
	}
	public void select(){
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM lab2_account");
			while(rs.next()){
				System.out.println(rs.getString(1) + "," + rs.getString(2));
			}
		}
		catch(SQLException ex){
			//handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	public int withdraw(String account_number, double amount){

		ResultSet rs = null;
		PreparedStatement stmt = null;
		int rowcount;
		// start transaction
		try {
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(
					conn.TRANSACTION_SERIALIZABLE);
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
		try {
			stmt = conn.prepareStatement(
					"update lab2_account set balance = balance - " + amount + "where account_number = ?");
			stmt.setString(1, account_number);
			rowcount = stmt.executeUpdate();
			if (rowcount !=1) {
				System.out.println("Something is wrong. You are updateing " + rowcount + " rows");
						conn.rollback();
			} else {
				System.out.println("updated " +
						rowcount + "rows");
				conn.commit();
			}
		}
		catch (SQLException ex){
			// handle any errors
			System.out.println("SQLException: " +
					ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " +
					ex.getErrorCode());
		}
		return 1;
	}
	public static void main (String args[]){
		JDBCLab dblab = new JDBCLab();
		
		dblab.connectDB();
		dblab.select();
		dblab.withdraw("a' or account_number='A001'#",10);
		dblab.select();
		dblab.disconnect();
	}
}