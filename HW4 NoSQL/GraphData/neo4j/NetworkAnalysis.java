package neo4j;

import java.io.IOException;
import java.sql.*;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

public class NetworkAnalysis {

	   static final String DB_PATH = "lab4-store";
	   static GraphDatabaseService graphDb;
	   private static final String PRIMARY_KEY = "id";
	   private static Index<Node> nodeIndex;
	   
	   private static enum RelTypes implements RelationshipType
	   {
	       CONNECTS
	   }
	   
	   public static Traverser getFriends(final Node person) {
			TraversalDescription td = graphDb.traversalDescription()
				.breadthFirst()
				.relationships(RelTypes.CONNECTS, Direction.OUTGOING)
				.evaluator(Evaluators.excludeStartPosition());
			return td.traverse(person);
		}
	   
	   public static Traverser getNeighbors(final Node person) {
			TraversalDescription td = graphDb.traversalDescription()
				.breadthFirst()
				.relationships(RelTypes.CONNECTS, Direction.OUTGOING)
				.evaluator(Evaluators.pruneWhereLastRelationshipTypeIs(RelTypes.CONNECTS));
			return td.traverse(person);
		}
	public static int ReachabilityCount(int id) {
//		Node neo = graphDB.getNodeById(startNodeId)
//			.getSingleRelationship(RelTypes.NEO_NODE, Direction.OUTGOING)
//			.getEndNode();
		Node node=nodeIndex.get(PRIMARY_KEY, id).getSingle();
		int friendsNumbers = 0;
		for(Path friendPath: getFriends(node)) {
			//System.out.println("At depth " + friendPath.length() + " => "
				//	+ friendPath.endNode().getProperty(PRIMARY_KEY));
			friendsNumbers++;
		}
		System.out.println(node.getProperty(PRIMARY_KEY) + " has "+friendsNumbers+" Reachable Nodes");
		
		return friendsNumbers;
	}
	public static int NeighbourCount(int id){
		Node node=nodeIndex.get(PRIMARY_KEY, id).getSingle();
		int neighborsNumbers = 0;
		for(Path friendPath: getNeighbors(node)) {
			//System.out.println("At depth " + friendPath.length() + " => "
				//	+ friendPath.endNode().getProperty(PRIMARY_KEY));
			neighborsNumbers++;
		}
		System.out.println(node.getProperty(PRIMARY_KEY) + " has "+neighborsNumbers+" Neighbor Nodes");
		return neighborsNumbers;
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		   //nodeIndex = graphDb.index().forNodes("nodes");
		   registerShutdownHook( graphDb );
		   
		   try ( Transaction tx = graphDb.beginTx() ){
			   nodeIndex = graphDb.index().forNodes( "nodes" );
			   //System.out.println(IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllNodes()));
			   //System.out.println(IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllRelationships()));
	            // Create some users and index their names with the IndexService
			   long start=System.currentTimeMillis();		   

			   //System.out.println(NeighbourCount(Integer.parseInt("1")));
			   System.out.println(ReachabilityCount(Integer.parseInt("1")));
			   /*switch(args[0]){
			   		case "NeighbourCount":
			   			System.out.println(NeighbourCount(Integer.parseInt(args[1])));
			   			break;
			   		case "ReachabilityCount":
			   			System.out.println(ReachabilityCount(Integer.parseInt(args[1])));
			   			break;		   		   		
			   }*/
			   System.out.println("Time cost is: "+(System.currentTimeMillis()-start)+"ms");
				tx.success();
		   }	   		   		   				  
		   graphDb.shutdown();		   		   	   
	}
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
