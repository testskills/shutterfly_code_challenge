package main;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import db.CreateDBArtifacts;
import db.sqlLiteConnection;

/*
 * 
 * This is main Java invocation class to process input file,trigger DB creation and calculate LTV
 * User has to input fully qualified input file name, top x customer value and output file name
 */
public class MainCustomerLTV {

	static String output_filename=null;
	
	/* 
	 * Method to ingest json input data to SQLLite schema.If any data issue it will skip only that record 
	 * and will continue to load rest
	 * 
	*/
	
	 
	private  void Ingest(JSONArray jsonArray,CreateDBArtifacts cda) throws ParseException
	{
		
		for(Object str:jsonArray)
        {
     	   JSONObject jsonObject = (JSONObject)  new JSONParser().parse(str.toString());  
     	   //System.out.println(jsonObject.get("type"));
     	   //switch
     	  switch (jsonObject.get("type").toString().toUpperCase())
     	  {
     	  	case "CUSTOMER":
     	  		cda.loadCustomerData(jsonObject);
             break;
     	  	
     	  	case "SITE_VISIT":
     	  		cda.loadSiteVisitData(jsonObject);
             break;
     	  	
     	  	case "IMAGE":
     	  		cda.loadImageData(jsonObject);
             break;
             
     	  	case "ORDER":
     	  		cda.loadOrdersData(jsonObject);
                break;
     	  	
     	  	default: 
     	  		System.out.println("Warning: Type not found skipping record");
             break;
          }
        }
		
		//cda.topData();
		
	}
	
	/* 
	 * Method to calculate LTV value. First it loads data into analytical table and then select
	 * top records based on input. It will redirect output to file
	 * Logic  used to calculate LTV:
	 * 52 * D (default:10) * total amount spent by customer / number of site visit weeks per customer
	 * 
	*/
	
	private  void TopXSimpleLTVCustomers(int x,int D)
	{
		Connection connection;
		Statement statement;
		try
		{
		
		connection = new sqlLiteConnection().getConnection();
		statement = connection.createStatement();					  		
	   
        
        //load LTV analytical table
        
       statement.executeUpdate("insert into analytical_LTV select orders_data.customer_id,"
        		+ "ifnull (52*"+D +"*(total_amount/ number_of_Weeks),1) as LTV from "
        		+ " (select customer_id,sum(total_amount) total_amount from orders"
	    		+ " group by customer_id ) orders_data,"
	    		+ " (select customer_id,"
	    		+ "	ifnull((julianday(max(event_time)) - julianday(min(event_time)))/7,1) number_of_Weeks from SiteVisit "  
	    		+ "	group by customer_id) weeks_data"
	    		+ " where orders_data.customer_id=weeks_data.customer_id"
	    		+ "");  
	    
	    
	    FileWriter fw=new FileWriter(new File(output_filename));
	    
	    ResultSet rs   = statement.executeQuery("select * from analytical_LTV order by LTV desc limit "+x);  
	    
        
        // loop through the result set  
        while (rs.next()) {  
        	fw.write(String.format(rs.getString("customer_id")));
        	fw.write("\t");
     	    fw.write(String.format(rs.getString("LTV")));
    	    fw.write(System.lineSeparator());
    	    
            System.out.println(rs.getString("customer_id") +  "\t" + 
                               rs.getString("LTV")); 
            
        } 
        
        fw.close();
		}
		catch(Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
	}
	public static void main(String[] args) {
		
		
		// Accept input file name, top records and output file name from user
		
		System.out.println("Enter fully qualified INPUT file name: ");
		Scanner scanner = new Scanner(System.in);
		String json = scanner.nextLine();
		
		System.out.println("Enter value for Top Number of records: ");
		int top=scanner.nextInt();
		
		scanner.nextLine();
		
		System.out.println("Enter fully qualified OUTPUT file name: ");
		 
		output_filename = scanner.nextLine();
		
		MainCustomerLTV main_obj=new MainCustomerLTV();
		
		try{
        	
        	//Shutterfly Average customer lifespan: 10 years
        	   int D=10;
			 
	         //parse input json file
        	   
	           FileReader reader = new FileReader(json);
	   	       JSONParser jsonParser = new JSONParser();
	           
	           JSONArray jsonArray = (JSONArray) jsonParser.parse(reader);
	           
	           CreateDBArtifacts cda =new CreateDBArtifacts();
	           
	           //create in-memory SQLLite schema
	           cda.createSchema();
	           
	           //Ingest input json data into database
	           main_obj.Ingest(jsonArray,cda);
	           
	           //Calculate LTV for top x customers and redirect output to file
	           main_obj.TopXSimpleLTVCustomers(top,D);
	          
	 
	        } 
		
		catch (ParseException p)
		{
			System.out.println("ERROR: Unable to parse input file");
			p.printStackTrace();
		}
		catch (SQLException p)
		{
			System.out.println("ERROR: While processing database transaction");
			p.printStackTrace();
		}
		catch (Exception e) {
	            e.printStackTrace();
	        }

		}

	}


