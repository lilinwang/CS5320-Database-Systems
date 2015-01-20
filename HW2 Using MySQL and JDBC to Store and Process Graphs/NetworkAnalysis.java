import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;


public class NetworkAnalysis {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/dblab2";	                                
	//  Database credentials
	static final String USER = "root";
	static final String PASS = "123456";
	static Connection conn = null;
	//static final int nodeSize=2000000;
	//static ArrayList<Integer> nodes=new ArrayList<Integer>();
	static int Diameter;
	static ArrayList<Integer> clique=new ArrayList<Integer>();
	
	public static int NeighbourCount(int id){
		String sql="select count(*) from CA_ROADNET where FromNodeId="+id;
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
	
	public static int ReachabilityCount(int id) throws SQLException{		
		String sql=null;
		ResultSet rs = null;
		Statement stmt=conn.createStatement();
		int ans=0,now=0;
		Queue<Integer> queue=new LinkedList<Integer>(); 
		HashMap<Integer,Boolean> map=new HashMap<Integer,Boolean>(); 
		
		queue.add(id);
		map.put(id,true);
		while (!queue.isEmpty()){
			sql="select ToNodeId from CA_ROADNET where FromNodeId="+queue.peek();		
			try{				      			      
				   rs=stmt.executeQuery(sql);
				   while (rs.next()) {
					   now=rs.getInt(1);
					   if (map.get(now)==null){
						   queue.add(now);
						   map.put(now, true);
						   ans++;
						//   System.out.print(now+" ");//System.out.print(" ");
					   }
					   
				   };			   
			}catch(SQLException se){
			//Handle errors for JDBC
				se.printStackTrace();
			}catch(Exception e){
			//Handle errors for Class.forName
			    e.printStackTrace();
			}
			queue.remove();
		}				
		return ans;
	}
		
	private static void expandCliques(int len,int k) throws SQLException{
		String sql=null;
		ResultSet rs = null;
		Statement stmt=conn.createStatement();
		if (len>k) {
			sql="insert into Cliques"+k+" (";
			for (int i=1;i<k;i++){
				sql=sql+"Node"+i+", "; 
			}
			sql=sql+"Node"+k+") values (";
			for (int i=0;i<clique.size()-1;i++){
				sql=sql+clique.get(i)+", ";
			}
			sql=sql+clique.get(clique.size()-1)+")";
			stmt.executeUpdate(sql);
			return;
		}
		
		HashSet<Integer> result=new HashSet<Integer>();
		HashSet<Integer> set=new HashSet<Integer>();
		sql="select ToNodeId from CA_ROADNET where FromNodeId="+clique.get(0)+" and ToNodeId>FromNodeId";
		rs=stmt.executeQuery(sql);
		while (rs.next()){
			set.add(rs.getInt(1));
		}
		result.addAll(set);
		for (int i=1;i<len-1;i++){
			set.clear();
			sql="select ToNodeId from CA_ROADNET where FromNodeId="+clique.get(i)+" and ToNodeId>FromNodeId";
			rs=stmt.executeQuery(sql);
			while (rs.next()){
				set.add(rs.getInt(1));
			}
			result.retainAll(set);
		}
		Iterator<Integer> it = result.iterator();
		while (it.hasNext()){
			//System.out.print(it.next());
			clique.add(it.next());
			expandCliques(len+1,k);
			clique.remove(len-1);
		}		
	}
	public static void DiscoverCliques(int k) throws SQLException{
		String sql=null;
		ResultSet rs = null;
		Statement stmt=conn.createStatement();	
		
		DatabaseMetaData meta = conn.getMetaData();
		String table="Cliques"+k;
		rs=meta.getTables(null, null, table.toUpperCase(), null);
		while (rs.next()){
			return;
		}
		
		sql="CREATE TABLE Cliques"+k+" (";
		for (int i=1;i<k;i++){
			sql=sql+"Node"+i+" INTEGER not NULL, "; 
		}
		sql=sql+"Node"+k+" INTEGER not NULL)";
		try {
			stmt.executeUpdate(sql);					
		}catch(SQLException se){
		//Handle errors for JDBC			
			se.printStackTrace();
			return;
		}catch(Exception e){
		//Handle errors for Class.forName
		    e.printStackTrace();
		}
		
		if (k==1){
			sql="CREATE TABLE Cliques1 (Node1 INTEGER not NULL)";
			try {
				stmt.executeUpdate(sql);					
			}catch(SQLException se){
			//Handle errors for JDBC			
				se.printStackTrace();
				return;
			}catch(Exception e){
			//Handle errors for Class.forName
			    e.printStackTrace();
			}
			sql="select DISTINCT FromNodeId from CA_ROADNET order by FromNodeId asc";
			rs=stmt.executeQuery(sql);
			while (rs.next()){
				sql="insert into Cliques1 (Node1) VALUES ("+rs.getInt(1)+")";
				stmt.executeUpdate(sql);
			}
			return;
		}
		
		sql="select * from CA_ROADNET where FromNodeId<ToNodeId";
		rs=stmt.executeQuery(sql);
		while (rs.next()){
			clique.clear();
			clique.add(rs.getInt(1));
			clique.add(rs.getInt(2));
			expandCliques(3,k);			
		}		
		return;
	}
	private static int ShortestPath(int startNode) throws SQLException{
		int ans=0,endNode=-1,now=-1;
		String sql=null;
		ResultSet rs = null;
		Statement stmt=conn.createStatement();
		Queue<Integer> queue=new LinkedList<Integer>(); 
		HashMap<Integer,Integer> map=new HashMap<Integer,Integer>(); 
		int tem;
		queue.add(startNode);
		map.put(startNode,0);
		while (!queue.isEmpty()){
			tem=queue.peek();
			sql="select ToNodeId from CA_ROADNET where FromNodeId="+tem;	
			if (ans<map.get(tem)) {
				ans=map.get(tem);
				endNode=tem;
			}
			try{				      			      
				   rs=stmt.executeQuery(sql);
				   while (rs.next()) {
					   now=rs.getInt(1);
					   if (map.get(now)==null){
						   queue.add(now);
						   map.put(now, map.get(tem)+1);
					   }
					   
				   };			   
			}catch(SQLException se){
			//Handle errors for JDBC
				se.printStackTrace();
			}catch(Exception e){
			//Handle errors for Class.forName
			    e.printStackTrace();
			}
			queue.remove();
		}						
		Diameter=ans;
		return endNode;
	}
	public static int NetworkDiameter() throws SQLException{
		int tem=ShortestPath(0);		
		ShortestPath(tem);					
		return Diameter;
	}
	public static void main(String[] args) throws IOException, SQLException {
		 Statement stmt = null;
 		   
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver").newInstance();			    
		      //STEP 3: Open a connection
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      System.out.println("Connected database successfully...");		      
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();		      
		      String sql = "use dblab2"; 
		      stmt.executeUpdate(sql);		     
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }
		   
		   long start=System.currentTimeMillis();		   
		   switch(args[0]){
		   		case "NeighbourCount":
		   			System.out.println(NeighbourCount(Integer.parseInt(args[1])));
		   			break;
		   		case "ReachabilityCount":
		   			System.out.println(ReachabilityCount(Integer.parseInt(args[1])));
		   			break;
		   		case "DiscoverCliques":
		 		    DiscoverCliques(Integer.parseInt(args[1]));
		 		    break;
		   		case "NetworkDiameter()":
		   			System.out.println(NetworkDiameter());
		   			break;		   		
		   }
		   System.out.println("Time cost is: "+(System.currentTimeMillis()-start)+"ms");
	   
	}
}