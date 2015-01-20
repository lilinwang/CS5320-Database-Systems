package neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

public class CreateDB {

	   
	   static final String DB_PATH = "lab4-store";
	   static GraphDatabaseService graphDb;
	   private static final String PRIMARY_KEY = "id";
	   private static Index<Node> nodeIndex;
	   
	   private static enum RelTypes implements RelationshipType
	   {
	       CONNECTS
	   }
	   private static Node createAndIndexUser( final int id )
	    {
	        Node node = graphDb.createNode();	        
	        node.setProperty( PRIMARY_KEY, id );
	        nodeIndex.add( node, PRIMARY_KEY, id );
	        return node;
	    }
	   
	   private static void parsefile(String filename) throws IOException, SQLException{		
           BufferedReader reader = new BufferedReader(new FileReader(filename));
		   String strLine;		   
		   try ( Transaction tx = graphDb.beginTx() ){
			   while ((strLine = reader.readLine()) != null) {			  
				   Node fromNode = nodeIndex.get(PRIMARY_KEY, Integer.parseInt(strLine.split(" ")[0])).getSingle();	           
				   Node toNode = nodeIndex.get(PRIMARY_KEY, Integer.parseInt(strLine.split(" ")[1])).getSingle();
				   fromNode.createRelationshipTo( toNode, RelTypes.CONNECTS );
				   //System.out.println("100");
			   }	
		   }
		   reader.close();
	   }
	   private static void clearDB() {
			try {
				FileUtils.deleteRecursively(new File(DB_PATH));
			}
			catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		
	      
	   public static void main(String[] args) throws IOException, SQLException {

		   clearDB();
		   graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		   registerShutdownHook( graphDb );
		   long start=System.currentTimeMillis();	
		   try ( Transaction tx = graphDb.beginTx() ){
			   nodeIndex = graphDb.index().forNodes( "nodes" );			   
	            // Create some users and index their names with the IndexService
	            for ( int id = 0; id < 4039; id++ )
	            {
	                createAndIndexUser( id );
	            }
	            String filename="C:/Users/Acer/downloads/facebook_combined.txt";
	            BufferedReader reader = new BufferedReader(new FileReader(filename));
	 		   String strLine;		   
	 			   while ((strLine = reader.readLine()) != null) {			  
	 				   Node fromNode = nodeIndex.get(PRIMARY_KEY, Integer.parseInt(strLine.split(" ")[0])).getSingle();	           
	 				   Node toNode = nodeIndex.get(PRIMARY_KEY, Integer.parseInt(strLine.split(" ")[1])).getSingle();
	 				   fromNode.createRelationshipTo( toNode, RelTypes.CONNECTS );
	 				   //System.out.println("100");
	 			   }	
	 		   reader.close();

			   System.out.println("Time cost is: "+(System.currentTimeMillis()-start)+"ms");
	 		   System.out.println("Created "+IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllNodes())+" Nodes");
			   System.out.println("Created "+IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllRelationships())+" Relationships");
			   
			   tx.success();
		   }	   
		   graphDb.shutdown();
		}//end main
	   
	   private static void registerShutdownHook( final GraphDatabaseService graphDb )
	   {
	       // Registers a shutdown hook for the Neo4j instance so that it
	       // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	       // running example before it's completed)
	       Runtime.getRuntime().addShutdownHook( new Thread()
	       {
	           @Override
	           public void run()
	           {
	               graphDb.shutdown();
	           }
	       } );
	   }

}

