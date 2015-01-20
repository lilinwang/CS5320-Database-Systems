import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import org.json.JSONException;

public class MongoDB {
	public static MongoClient mongoClient;
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
	
	public static void insertTrajectoryMeasure(String SetID,String trajectoryid,String latitude,String lontitude,String altitude, String dateNum,String date, String time) throws UnknownHostException{

        // get handle to "mydb"
        DB db = mongoClient.getDB("dblab4");

        // get a collection object to work with
        DBCollection coll = db.getCollection("Trajectory");

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("TrajectorySetID",SetID)
        		.append("TrajectoryID", trajectoryid)
                .append("Latitude", Double.valueOf(latitude))
                .append("Longtitude", Double.valueOf(lontitude))
                .append("Altitude", Integer.valueOf(altitude))
                .append("DateNum", Double.valueOf(dateNum))
        		.append("Date",date)
        		.append("Time",time);

        coll.insert(doc);
	}
	public static void findall() throws UnknownHostException{
		DB db = mongoClient.getDB("dblab4");
		Set<String> collectionNames = db.getCollectionNames();
        for (final String s : collectionNames) {
            System.out.println("************"+s+"***********");
            DBCollection coll = db.getCollection(s);
    	    DBCursor cursor = coll.find();
            try {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }
	}
	public static int getCount(String SetID, String trajectoryID) throws JSONException{
        int count=0;
		DB db = mongoClient.getDB("dblab4");
        DBCollection coll = db.getCollection("Trajectory");
		BasicDBObject query = new BasicDBObject("TrajectorySetID",SetID)
							.append("TrajectoryID",trajectoryID);
		DBCursor cursor = coll.find(query);
		count = cursor.count();
		return count;
	}

	public static int[] getCountByDate(String[] dates){
        int count[]=new int[dates.length];
		DB db = mongoClient.getDB("dblab4");
        DBCollection coll = db.getCollection("Trajectory");        
        for(int i=0;i<dates.length;i++){
			BasicDBObject query = new BasicDBObject("Date",dates[i]);
			DBCursor cursor = coll.find(query);	
			count[i]=cursor.count();
            cursor.close();
        }		
		return count;
	}
	
	public static void main(String[] args) throws IOException, JSONException{
		mongoClient = new MongoClient();
		
		//insert data into database
		long startTime=System.nanoTime(); 		
		parseTrajectory("/Users/Yanjing/Documents/Courses/Database/assignment3/Geolife Trajectories 1.3/Data/");
		long endTime=System.nanoTime();
		System.out.println("insert time "+(endTime-startTime)+"ns");
		
		//query count number of trajectory
		startTime=System.nanoTime(); 		
		int count=getCount("004","20090727181402");
		System.out.println(count);
		endTime=System.nanoTime();
		System.out.println("query count time:"+(endTime-startTime)+"ns");
		
		//query count of trajectory of specific 10 days
		startTime=System.nanoTime(); 		
		String[] dates=new String[]{"2008-10-28","2008-10-24","2008-10-25","2009-07-04","2008-10-23","2008-10-26","2008-10-27","2008-11-01","2008-10-29","2008-10-30"};
		int countdate[]=getCountByDate(dates);
		for(int c : countdate){
			System.out.println(c);
		}		
		endTime=System.nanoTime();
		System.out.println("query date time:"+(endTime-startTime)+"ns");

		mongoClient.close();
	}
}
