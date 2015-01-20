import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class MySql {

		static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static final String DB_URL = "jdbc:mysql://localhost/DBLab4";
		 //  Database credentials
		   static final String USER = "root";
		   static final String PASS = "123456";
		   static Connection conn = null;
		public static void createTable() throws SQLException{
			
			Statement stmt = conn.createStatement();		      
		    String sql = "CREATE TABLE Trajectory (SetId char(5) not NULL, TrajectoryID char(15) not NULL, lat double(12,8),lon double(12,8),alt int(6), datenum double(18,11), date char(11), time char(10) )"; 
		    stmt.executeUpdate(sql);
		    System.out.println("Created table in given database...");	   			
		}
			

		public static void parseTrajectory (String path) throws IOException{

			File SourceDir = new File(path);
			File[] TrajectorySet=SourceDir.listFiles();
			if(TrajectorySet!=null){
				for(File TSet:TrajectorySet){
					System.out.println(TSet.getName());
					File SourceTraj = new File(TSet.getAbsolutePath()+"/Trajectory");
					File[] Trajectory=SourceTraj.listFiles();
					if(Trajectory!=null){
						for(File Traj:Trajectory){
							try {
								String SetID=TSet.getName();
								String TrajID=Traj.getName().substring(0, Traj.getName().length()-4);
						       
								BufferedReader reader = new BufferedReader(new FileReader(Traj));
								
						        //Skip the first useless 6 lines in Source
						        for(int i=0;i<6;i++) reader.readLine();
						        
						        //Start Parsing Source file
						        String line;
					        	while ( (line = reader.readLine()) != null) {
					        		String[] field=line.split(",");	
					        		insertTrajectoryMeasure(SetID,TrajID,field[0],field[1],field[3],field[4],field[5],field[6]);
					        	}
					        	reader.close();
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					else{
						System.out.println("Trajectory is empty!");
					}
				
				
				}
			}
			else{
				System.out.println("Trajectory Set is empty!");
			}
		}
		
		public static void insertTrajectoryMeasure(String SetID,String trajectoryid,String latitude,String lontitude,String altitude, String dateNum,String date, String time) throws SQLException{
		    String sql = "INSERT INTO Trajectory (SetId, TrajectoryID,lat,lon,alt,datenum, date, time) "
		    		+ "VALUES (?,?,?,?,?,?,?,?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, SetID);
			ps.setString(2, trajectoryid);
			ps.setDouble(3, Double.valueOf(latitude));
			ps.setDouble(4, Double.valueOf(lontitude));
			ps.setInt(5, Integer.valueOf(altitude));
			ps.setDouble(6, Double.valueOf(dateNum));
			ps.setString(7, date);
			ps.setString(8, time);
			ps.executeUpdate();
			ps.close();
		}


		public static int getCount(String SetID, String trajectoryID){
	        String sql="select count(SetId) from Trajectory where SetId="+SetID+" and TrajectoryID="+trajectoryID;
	        ResultSet rs = null;
			try{			
				   Statement stmt = conn.createStatement();		      			      
				   rs=stmt.executeQuery(sql);
				   if (rs.next()) {
					   return rs.getInt(1);
				   };			   
			}catch(SQLException se){
			//Handle errors for JDBC
				se.printStackTrace();
			}catch(Exception e){
			//Handle errors for Class.forName
			    e.printStackTrace();
			}	
			return 0;
		}

		public static int[] getCountByDate(String[] dates){
	        int count[]=new int[dates.length];
	        for(int i=0;i<dates.length;i++){
		        String sql="select count(*) from Trajectory where date='"+dates[i]+"'";
		        ResultSet rs = null;
		        try{			
					   Statement stmt = conn.createStatement();		      			      
					   rs=stmt.executeQuery(sql);
					   if (rs.next()) {
						   count[i]=rs.getInt(1);
					   };			   
				}catch(SQLException se){
				//Handle errors for JDBC
					se.printStackTrace();
				}catch(Exception e){
				//Handle errors for Class.forName
				    e.printStackTrace();
				}	
	        }
			
			return count;
		}
		
		public static void main(String[] args) throws IOException, SQLException{
			Statement stmt = null;
			try{
			      //STEP 2: Register JDBC driver
			      Class.forName(JDBC_DRIVER);
			      //STEP 3: Open a connection
			      conn = DriverManager.getConnection(DB_URL, USER, PASS);
			      System.out.println("Connected database successfully...");		      
			      //STEP 4: Execute a query
			      
			   }catch(SQLException se){
			      //Handle errors for JDBC
			      se.printStackTrace();
			   }catch(Exception e){
			      //Handle errors for Class.forName
			      e.printStackTrace();
			   }	
			createTable();
			long startTime=System.nanoTime(); 		
			parseTrajectory("/Users/Yanjing/Documents/Courses/Database/assignment3/Geolife Trajectories 1.3/Data/");
			long endTime=System.nanoTime();
			System.out.println("insert time "+(endTime-startTime)+"ns");

			startTime=System.nanoTime(); 		
			int count=getCount("004","20090727181402");
			System.out.println(count);
			endTime=System.nanoTime();
			System.out.println("query count time:"+(endTime-startTime)+"ns");

			startTime=System.nanoTime(); 		
			String[] dates=new String[]{"2008-10-28","2008-10-24","2008-10-25","2009-07-04","2008-10-23","2008-10-26","2008-10-27","2008-11-01","2008-10-29","2008-10-30"};

			int countdate[]=getCountByDate(dates);
			for(int c : countdate){
				System.out.println(c);
			}		
			endTime=System.nanoTime();
			System.out.println("query date time:"+(endTime-startTime)+"ns");
			conn.close();
		}
}


