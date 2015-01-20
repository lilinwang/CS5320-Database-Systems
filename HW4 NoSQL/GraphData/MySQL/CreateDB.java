import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

//import com.mysql.jdbc.Driver;

public class CreateDB {

	   
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/dblab2";	                                
	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "";

	   static Connection conn = null;
	   
	   private static void parsefile(String filename) throws IOException, SQLException{
		   BufferedReader reader = new BufferedReader(new FileReader(filename));
		   String strLine;
		   
		   String sql="INSERT INTO facebook (FromNodeId,ToNodeID) VALUES (?,?)";
		   PreparedStatement ps = conn.prepareStatement(sql);
		   final int batchSize = 10000;
		   int count = 0;
		   while ((strLine = reader.readLine()) != null) {
			    ps.setString(1, strLine.split(" ")[0]);
			    ps.setString(2, strLine.split(" ")[1]);			  
			    ps.addBatch();
			    if(++count % batchSize == 0) {
			        ps.executeBatch();
			        //System.out.println("inserted " + count);
			    }
			}
		   ps.executeBatch(); // insert remaining records
		   ps.close();
		   reader.close();		   
		  /* try{
			   Statement stmt = conn.createStatement();		      			      
			      stmt.executeUpdate(sql);
			   }catch(SQLException se){
				      //Handle errors for JDBC
				      se.printStackTrace();
				   }catch(Exception e){
				      //Handle errors for Class.forName
				      e.printStackTrace();
				   }*/	
	   }
	   
	      
	   public static void main(String[] args) throws IOException, SQLException {

		   Statement stmt = null;
		   long start=System.currentTimeMillis();
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver").newInstance();			    
		      //STEP 3: Open a connection
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      System.out.println("Connected database successfully...");		      
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();		      
		      String sql = "CREATE TABLE facebook (FromNodeId INTEGER not NULL, ToNodeID INTEGER not NULL)"; 
		      stmt.executeUpdate(sql);
		      System.out.println("Created table in given database...");
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }		   			   
		      		   
		   parsefile("C:/Users/Acer/downloads/facebook_combined.txt");
		   
		   try{
		         if(stmt!=null)
		            conn.close();
		      }catch(SQLException se){
		      }// do nothing
		      try{
		         if(conn!=null)
		            conn.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		   }//end try
		      System.out.println(System.currentTimeMillis()-start);
		}//end main

}
