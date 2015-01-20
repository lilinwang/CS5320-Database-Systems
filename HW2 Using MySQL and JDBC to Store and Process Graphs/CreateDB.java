import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class CreateDB {

	   
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/DBLab2";

	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "123456";

	   static Connection conn = null;
	   
	   private static void parsefile(String filename) throws IOException, SQLException{
		   BufferedReader reader = new BufferedReader(new FileReader(filename));
		   String strLine;
		   reader.readLine();
		   reader.readLine();
		   int numCases = Integer.parseInt(reader.readLine().split(" ")[4]);
		   System.out.println(numCases);
		   reader.readLine();
		   String sql="INSERT INTO CA_ROADNET2 (FromNodeId,ToNodeID) VALUES (?,?)";
		   PreparedStatement ps = conn.prepareStatement(sql);
		   final int batchSize = 10000;
		   int count = 0;
		   while ((strLine = reader.readLine()) != null) {
			    ps.setString(1, strLine.split("\t")[0]);
			    ps.setString(2, strLine.split("\t")[1]);			  
			    ps.addBatch();
			    if(++count % batchSize == 0) {
			        ps.executeBatch();
			        System.out.println("inserted " + count);
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
	   		   
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");

		      //STEP 3: Open a connection
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      System.out.println("Connected database successfully...");		      
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();		      
		      String sql = "CREATE TABLE CA_ROADNET (FromNodeId INTEGER not NULL, ToNodeID INTEGER not NULL)"; 
		      stmt.executeUpdate(sql);
		      System.out.println("Created table in given database...");
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }		   			   
		      		   
		// parsefile("/Users/Yanjing/Downloads/roadNet-CA2.txt");
		   
		   //create index on fromnodeID
		   stmt = conn.createStatement();		      
		   String sql = "CREATE INDEX NodeIndex ON CA_ROADNET (FromNodeId)"; 
		   stmt.executeUpdate(sql);

		   
		   parsefile(args[0]); 
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
		}//end main

}
