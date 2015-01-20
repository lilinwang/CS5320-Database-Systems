import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import redis.clients.jedis.Jedis;


public class Redis {
	public static Jedis jedis; 
	public static void parseTrajectory(String path){
		File SourceDir = new File(path);
		File[] TrajectorySet=SourceDir.listFiles();
		if(TrajectorySet!=null){
			for(File TSet:TrajectorySet){
				File SourceTraj = new File(TSet.getAbsolutePath()+"/Trajectory");
				File[] Trajectory=SourceTraj.listFiles();
				if(Trajectory!=null){
					for(File Traj:Trajectory){
						try {
							//Use current time as Unique ID
							String SetID=TSet.getName();
							String TrajID=Traj.getName().substring(0, Traj.getName().length()-4);
							//Record Additional Info for Advanced Retrieval
					       
							BufferedReader reader = new BufferedReader(new FileReader(Traj));
							
					        //Skip the first useless 6 lines in Source
					        for(int i=0;i<6;i++) reader.readLine();
					        
					        //Start Parsing Source file
					        String line;
				        	while ( (line = reader.readLine()) != null) {
				        		String[] field=line.split(",");	
				        		insertTrajectoryMeasure(SetID,TrajID,field[0]+","+field[1]+","+field[3]+","+field[4]+","+field[5]+","+field[6]);
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
	public static void insertTrajectoryMeasure(String SetID,String trajectoryid,String data){
		jedis.lpush(SetID+trajectoryid, data);
	}
	public static int getCount(String SetID, String trajectoryID){
		return Integer.valueOf(jedis.lrange(SetID+trajectoryID,0,-1).size());
	}
	public static int[] getCountByDate(String[] dates){
	    int count[]=new int[dates.length];
	    Set<String> keys=jedis.keys("*");
        for(int i=0;i<dates.length;i++){
		    for(String key:keys){
		    	List<String> Trajectory = jedis.lrange(key, 0, -1);
		    	for(String measure:Trajectory){
		    		String[] fields=measure.split(",");
		    		if(fields[4].equals(dates[i])){
		    			count[i]++;
		    		}		    		
		    	}	    	
		    }
        }			
		return count;	
	}
	public static void main(String[] args){	
		jedis = new Jedis("localhost");	
		String[] dates=new String[]{"2008-10-28","2008-10-24","2008-10-25","2009-07-04","2008-10-23","2008-10-26","2008-10-27","2008-11-01","2008-10-29","2008-10-30"};

		//insert data into database
		long startTime=System.nanoTime(); 		
		parseTrajectory("/Users/Yanjing/Documents/Courses/Database/assignment3/Geolife Trajectories 1.3/Data/");
		long endTime=System.nanoTime();
		System.out.println("insert time： "+(endTime-startTime)+"ns");
		
		//query count number of trajectory
		startTime=System.nanoTime(); 		
		int count=getCount("004","20090727181402");
		endTime=System.nanoTime();
		System.out.println(count);
		System.out.println("query count time： "+(endTime-startTime)+"ns");
		
		//query count of trajectory of specific 10 days
		startTime=System.nanoTime(); 		
		int[] counts=getCountByDate(dates);
		endTime=System.nanoTime();
		for (int c:counts){
			System.out.println(c);
		}
		System.out.println("query date time： "+(endTime-startTime)+"ns");


	}
	
}
