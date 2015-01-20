import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Scanner;

public class TrajDB {
	
	public static String LOCAL_PATH="/Users/Yanjing/Documents/Courses/Database/assignment3/TrajectorySet/";
	
	public static void sourcelocal (String tname, String path) throws IOException{
		if(!createTrajactorySet(tname)){
			System.out.println("Trajector Set already exists!");
			return;		
		}
		File SourceDir = new File(path);
		File[] files=SourceDir.listFiles();
		if(files!=null){
			for(File f:files){
				try {
					//Use current time as Unique ID
					long TrajID=System.currentTimeMillis();					
					//Record Additional Info for Advanced Retrieval
					int line_count=0;
			        double max_lon=Double.MIN_VALUE;
			        double min_lon=Double.MAX_VALUE;
			        double max_lat=Double.MIN_VALUE;
			        double min_lat=Double.MAX_VALUE;
			        int max_height=Integer.MIN_VALUE;
			        int min_height=Integer.MAX_VALUE;
			        double max_time=Double.MIN_VALUE;
			        double min_time=Double.MAX_VALUE;
			        
					BufferedReader reader = new BufferedReader(new FileReader(f));
			        BufferedWriter writer = Files.newBufferedWriter(Paths.get(LOCAL_PATH+tname+"/"+TrajID),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
			        //Store additional Info to File "Summary"
			        BufferedWriter summary= Files.newBufferedWriter(Paths.get(LOCAL_PATH+tname+"/Summary"),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		        	
			        //Skip the first useless 6 lines in Source
			        for(int i=0;i<6;i++) reader.readLine();
			        
			        //Start Parsing Source file
			        String line;
		        	while ( (line = reader.readLine()) != null) {
		        		line_count++;
		        		String[] field=line.split(",");
		        		max_lat=(Double.parseDouble(field[0])>max_lat)?Double.parseDouble(field[0]):max_lat;
		        		min_lat=(Double.parseDouble(field[0])<min_lat)?Double.parseDouble(field[0]):min_lat;
		        		max_lon=(Double.parseDouble(field[1])>max_lon)?Double.parseDouble(field[1]):max_lon;
		        		min_lon=(Double.parseDouble(field[1])<min_lon)?Double.parseDouble(field[1]):min_lon;
		        		max_height=(Integer.parseInt(field[3])>max_height)?Integer.parseInt(field[3]):max_height;
		        		min_height=(Integer.parseInt(field[3])<min_height)?Integer.parseInt(field[3]):min_height;
		        		max_time=(Double.parseDouble(field[4])>max_time)?Double.parseDouble(field[4]):max_time;
		        		min_time=(Double.parseDouble(field[4])<min_time)?Double.parseDouble(field[4]):min_time;
		        		writer.append(line+"\n");
		        	}
		        	writer.close();
		        	reader.close();
	        		summary.append(TrajID+","+line_count+","+max_lat+","+min_lat+","+max_lon+","+min_lon+","+max_height+","+min_height+","+max_time+","+min_time+"\n");
					summary.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("Success");

		}	
		else{
			System.out.println("No file in the source folder");
		}
	}
	
	
	public static boolean createTrajactorySet (String tname){
		File theDir = new File(LOCAL_PATH+tname);
		  // if the directory does not exist, create it
		if(theDir.exists()) 
			return false;
		try{
		   theDir.mkdir();
		} catch(SecurityException se){
		}        
		return true;  
		    		  
	}
	
	public static boolean insertTrajactory(String tname, ArrayList<String> sequence){
		File theDir = new File(LOCAL_PATH+tname);
		if(theDir.exists()){
			//Use current time as Unique ID
			long TrajID=System.currentTimeMillis();					
			//Record Additional Info for Advanced Retrieval
			int line_count=0;
	        double max_lon=Double.MIN_VALUE;
	        double min_lon=Double.MAX_VALUE;
	        double max_lat=Double.MIN_VALUE;
	        double min_lat=Double.MAX_VALUE;
	        int max_height=Integer.MIN_VALUE;
	        int min_height=Integer.MAX_VALUE;
	        double max_time=Double.MIN_VALUE;
	        double min_time=Double.MAX_VALUE;	
			try {
					        
		        BufferedWriter writer = Files.newBufferedWriter(Paths.get(LOCAL_PATH+tname+"/"+TrajID),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		        //Store additional Info to File "Summary"
		        BufferedWriter summary= Files.newBufferedWriter(Paths.get(LOCAL_PATH+tname+"/Summary"),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
	        		        
		        //Start Parsing Source file
	        	for(String line:sequence){
	        		line_count++;
	        		String[] field=line.split(",");
	        		max_lat=(Double.parseDouble(field[0])>max_lat)?Double.parseDouble(field[0]):max_lat;
	        		min_lat=(Double.parseDouble(field[0])<min_lat)?Double.parseDouble(field[0]):min_lat;
	        		max_lon=(Double.parseDouble(field[1])>max_lon)?Double.parseDouble(field[1]):max_lon;
	        		min_lon=(Double.parseDouble(field[1])<min_lon)?Double.parseDouble(field[1]):min_lon;
	        		max_height=(Integer.parseInt(field[3])>max_height)?Integer.parseInt(field[3]):max_height;
	        		min_height=(Integer.parseInt(field[3])<min_height)?Integer.parseInt(field[3]):min_height;
	        		max_time=(Double.parseDouble(field[4])>max_time)?Double.parseDouble(field[4]):max_time;
	        		min_time=(Double.parseDouble(field[4])<min_time)?Double.parseDouble(field[4]):min_time;
	        		writer.append(line+"\n");
	        	}
	        	writer.close();
	    		summary.append(TrajID+","+line_count+","+max_lat+","+min_lat+","+max_lon+","+min_lon+","+max_height+","+min_height+","+max_time+","+min_time+"\n");
				summary.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			System.out.println("Trajector ID: "+TrajID);
			return true;
		}
		else{
			System.out.println("Trajector Set doesn't existed!");
			return false;
		}
		
	}
	public static boolean deleteTrajactory(String tname, String id){
		File theDir = new File(LOCAL_PATH+tname);
		if(theDir.exists()){			
			File file=new File(LOCAL_PATH+tname+"/"+id);
			if (file.exists()){
				file.delete();
				try {			
					File file3=new File(LOCAL_PATH+tname+"/temp");
					file3.createNewFile();
					
			        //Update File "Summary"
			        BufferedReader reader= Files.newBufferedReader(Paths.get(LOCAL_PATH+tname+"/Summary"),Charset.defaultCharset());
			        BufferedWriter writer = Files.newBufferedWriter(Paths.get(LOCAL_PATH+tname+"/temp"),Charset.defaultCharset(),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
					
			        String line=null;
			        while ((line=reader.readLine())!=null){
			        	String[] field=line.split(",");
			        	if (!field[0].equals(id)){
			        		writer.append(line+"\n");
			        	}		        	
			        }		       	        	
		    		reader.close();
		    		writer.close();	
		    		
		    		File file2=new File(LOCAL_PATH+tname+"/Summary");
		    		file2.delete();
												
					
					file3.renameTo(new File(LOCAL_PATH+tname+"/Summary"));
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}									
			}else {
				System.out.println("Trajectory "+id+ "doesn't existed!");
				return false;
			}		
			System.out.println("Delete Trajectory "+id+" Sucessfully");
			return true;
		}
		else{
			System.out.println("Trajector Set doesn't existed!");
			return false;
		}										
	}
	public static boolean retrieveTrajactoryCount(String tname, String id){
		File theDir = new File(LOCAL_PATH+tname);
		if(theDir.exists()){			
			File file=new File(LOCAL_PATH+tname+"/"+id);
			if (file.exists()){
				try {								
			        //Read Trajactory File
			        BufferedReader reader= Files.newBufferedReader(Paths.get(LOCAL_PATH+tname+"/Summary"),Charset.defaultCharset());
			        
			        String line=null;
			        while ((line=reader.readLine())!=null){
			        	String[] field=line.split(",");
			        	if (field[0].equals(id)){
			        		System.out.println("Count of Trajectory "+id+" is: "+field[1]);
			        		break;	
			        	}				        	        
			        }		       	        	
		    		reader.close();		    		
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}									
			} else {
				System.out.println("Trajectory "+id+ " doesn't existed!");
				return false;
			}
			//System.out.println(id);
			return true;
		}
		else{
			System.out.println("Trajector Set doesn't existed!");
			return false;
		}										
	}
	public static boolean retrieveTrajactoryAdvanced(String tname, int field, int op, double val){
		File theDir = new File(LOCAL_PATH+tname);
		if(theDir.exists()){			
			try {								
		        //Read Trajactory File
		        BufferedReader reader= Files.newBufferedReader(Paths.get(LOCAL_PATH+tname+"/Summary"),Charset.defaultCharset());
		        String line=null;
		        ArrayList<String> selected=new ArrayList<String>();
		        switch (op){
		        case 1:
		        	while ((line=reader.readLine())!=null){
			        	String[] sfield=line.split(",");
			        	if (Double.parseDouble(sfield[field*2])==val && Double.parseDouble(sfield[field*2+1])==val){			        		
			        		selected.add(sfield[0]);
			        	}				        	        
			        }
		        	break;
		        case 2:
		        	while ((line=reader.readLine())!=null){
			        	String[] sfield=line.split(",");
			        	if (Double.parseDouble(sfield[field*2])<=val){			        		
			        		selected.add(sfield[0]);
			        	}				        	        
			        }
		        	break;
		        case 3:
		        	while ((line=reader.readLine())!=null){
			        	String[] sfield=line.split(",");
			        	if (Double.parseDouble(sfield[field*2+1])>=val){			        		
			        		selected.add(sfield[0]);
			        	}				        	        
			        }
		        	break;
		        case 4:
		        	while ((line=reader.readLine())!=null){
			        	String[] sfield=line.split(",");
			        	if (Double.parseDouble(sfield[field*2])<val){			        		
			        		selected.add(sfield[0]);
			        	}				        	        
			        }
		        	break;
		        case 5:
		        	while ((line=reader.readLine())!=null){
			        	String[] sfield=line.split(",");
			        	if (Double.parseDouble(sfield[field*2+1])>val){			        		
			        		selected.add(sfield[0]);
			        	}				        	        
			        }
		        	break;		        		        	
		        }
		        for (int i=0;i<selected.size();i++) retrieveTrajactory(tname,selected.get(i));
		        		       	        	
	    		reader.close();		    		
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}															
			//System.out.println(id);
			return true;
		}
		else{
			System.out.println("Trajector Set doesn't existed!");
			return false;
		}										
	}

	public static boolean retrieveTrajactory(String tname, String id){
		File theDir = new File(LOCAL_PATH+tname);
		if(theDir.exists()){			
			File file=new File(LOCAL_PATH+tname+"/"+id);
			if (file.exists()){
				try {								
			        //Read Trajactory File
					System.out.println("Trajectory "+id+ ":");
			        BufferedReader reader= Files.newBufferedReader(Paths.get(LOCAL_PATH+tname+"/"+id),Charset.defaultCharset());
			        
			        String line=null;
			        while ((line=reader.readLine())!=null){
			        	System.out.println(line);	        	
			        }		       	        	
		    		reader.close();		    		
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}									
			} else {
				System.out.println("Trajectory "+id+ " doesn't existed!");
				return false;
			}
			
			return true;
		}
		else{
			System.out.println("Trajector Set doesn't existed!");
			return false;
		}										
	}		
	public static void main(String [] args) throws IOException{
		boolean setLocalPathFlag=false;
		while(!setLocalPathFlag){
			System.out.println("Please enter where you want to store your tracjectory sets:"); 
			Scanner sc_path = new Scanner(System.in);  
			LOCAL_PATH=sc_path.nextLine();
			File theDir = new File(LOCAL_PATH);
			if (theDir.exists()){
				setLocalPathFlag=true;	
				System.out.println("Set Path successfully!");
			}
			else
				System.out.println("Folder "+LOCAL_PATH+" doesn't existed!");
		}
		while(true){
			System.out.print("TrajDB>"); 
			Scanner sc = new Scanner(System.in);   
	        String command = sc.nextLine();
	        if (command.charAt(command.length()-1)!=';'){
	        	System.out.println("Missing ;");
	        	continue;
	        }
	        command=command.substring(0, command.length()-1);
			//String command="SOURCE test /Users/Yanjing/Documents/Courses/Database/assignment3/Geolife Trajectories 1.3/Data/001/Trajectory";
			//String command="INSERT INTO abc VALUES 39.906631,116.385564,0,492,40097.5864583333,2009-10-11,14:04:30 39.906554,116.385625,0,492,40097.5865162037,2009-10-11,14:04:35";
			//String command="DELETE FROM abc TRAJECTORY 1415421265858";
			//String command="RETRIEVE FROM abc TRAJECTORY 1415408028771";
			//String command="RETRIEVE FROM abc COUNT OF 1415421265858";
			//String command="RETRIEVE FROM abc WHERE long>=116.326243";
	
			String[] element=command.split(" ");
			switch(element[0].toUpperCase()){
				case "CREATE":
					if(element.length==2&&!element[1].isEmpty()){
						if(createTrajactorySet(element[1]))
							System.out.println("The Directory is existed!");
						else 
							System.out.println("Directory created successfully!");
					}
					else{
						System.out.println("Paratemer missing!");
						System.out.println("Format should be CREATE <tname>;");
					}
					break;
					
				//load a trajector set	
				case "SOURCE":				
					if(element.length>=3&&!element[1].isEmpty()&&!element[2].isEmpty()){
						String path="";
						for(int i=2;i<element.length;i++){
							path=path+element[i]+" ";
						}
						sourcelocal(element[1],path.trim());
					}
					else{
						System.out.println("Paratemer missing!");
						System.out.println("Format should be SOURCE <tname> <sourcePath>;");
					}
					break;
				
				case "INSERT":
					if(element.length>=5){
						if(element[1].toUpperCase().equals("INTO")&&element[3].toUpperCase().equals("VALUES")){
						
						ArrayList<String> sequence=new ArrayList<String>();
						for(int i=4;i<element.length;i++){
							sequence.add(element[i]);
						}						
						insertTrajactory(element[2],sequence);	
						}
						else{
							System.out.println("Command not found!");
							System.out.println("Format should be INSERT INTO <tname> VALUES <sequence>;");		
						}					
					}
					else{
						System.out.println("Parameter missing!");
						System.out.println("Format should be INSERT INTO <tname> VALUES <sequence>;");
					}
					break;
				case "DELETE":
					if(element.length==5){
						if(element[1].equals("FROM")&&element[3].equals("TRAJECTORY")){																			
							deleteTrajactory(element[2],element[4]);	
						}
						else{
							System.out.println("Command not found!");
							System.out.println("Format should be DELETE FROM <tname> TRAJECTORY <id>;");		
						}					
					}
					else{
						System.out.println("Parameter missing!");
						System.out.println("Format should be DELETE FROM <tname> TRAJECTORY <id>;");
					}
					break;
					
				case "RETRIEVE":
					if(element.length==5){
						if(element[1].equals("FROM")&&element[3].equals("TRAJECTORY")){																			
							retrieveTrajactory(element[2],element[4]);	
						}
						else if (element[1].equals("FROM")&&element[3].equals("WHERE")){
							 //lat, long, alt or date; op is an operator such as =, <, >, <=, >=
							boolean flag=true;
							int field=-1,op=-1;
							double val=-1;
							int start=0;
							//parse <field>
							if (element[4].substring(0,3).equals("lat")){
								field=1;start=3;							
							}
							else if (element[4].substring(0,4).equals("long")){
								field=2;start=4;
							}
							else if (element[4].substring(0,3).equals("alt")){
								field=3;start=3;
							}
							else if (element[4].substring(0,2).equals("op")){
								field=4;start=2;
							}
							else flag=false;													
							//parse <op> && <val>
							if (flag){
								if (element[4].substring(start,start+1).equals("=")){
									op=1;			
									val=Double.parseDouble(element[4].substring(start+1));
								}
								else if (element[4].substring(start,start+2).equals("<=")){
									op=2;
									val=Double.parseDouble(element[4].substring(start+2));								
								}
								else if (element[4].substring(start,start+2).equals(">=")){
									op=3;
									val=Double.parseDouble(element[4].substring(start+2));								
								}
								else if (element[4].substring(start,start+1).equals("<")){
									op=4;
									val=Double.parseDouble(element[4].substring(start+1));								
								}
								else if (element[4].substring(start,start+1).equals(">")){
									op=5;
									val=Double.parseDouble(element[4].substring(start+1));								
								}
								else flag=false;
							}
							//System.out.println(element[2]+field+" "+op+" "+val);
							if (flag) retrieveTrajactoryAdvanced(element[2],field,op,val);
							else {
								System.out.println("Command not found!");
								System.out.println("Format should be RETRIEVE FROM <tname> WHERE <field><op><val>;");
								System.out.println("where field is lat, long, alt or date; op is an operator such as =, <, >, <=, >=; and val is a value;");	
							}
						}
						else{
							System.out.println("Command not found!");
							System.out.println("Format should be RETRIEVE FROM <tname> TRAJECTORY <id>");		
						}					
					}else if (element.length==6){
						if (element[1].equals("FROM")&&element[3].equals("COUNT")&&element[4].equals("OF")){
							retrieveTrajactoryCount(element[2],element[5]);	
						}
						else{
							System.out.println("Command not found!");
							System.out.println("Format should be RETRIEVE FROM <tname> COUNT OF <id>");		
						}			
					}
					else{
						System.out.println("Parameter missing!");
						System.out.println("Format should be RETRIEVE FROM <tname> TRAJECTORY <id> | RETRIEVE FROM <tname> COUNT OF <id> | RETRIEVE FROM <tname> WHERE <field><op><val>;");

					}
					break;	
				case "EXIT":
					return;
			}	
		}
	}

}
