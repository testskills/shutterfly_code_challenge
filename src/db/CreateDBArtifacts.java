package db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.simple.JSONObject;

public class CreateDBArtifacts {

	Connection connection;
	Statement statement;
	PreparedStatement pstmt ;
	
	/*
	 * Create database schema and required tables. Referential integrity is enforced based on customer_id
	 * Not null constraint is defined for required columns
	 */
	public void createSchema() throws SQLException
	{
		sqlLiteConnection dbo=new sqlLiteConnection();
	      
		try {
			
			connection = dbo.getConnection();
			
			statement = connection.createStatement();
			statement.executeUpdate("drop table if exists Customer");
		    statement.executeUpdate("CREATE TABLE Customer (customer_id  TEXT PRIMARY KEY , "
		    		+ "event_time DATE NOT NULL, last_name TEXT, adr_city TEXT, adr_state TEXT)");
		
		    statement.executeUpdate("drop table if exists SiteVisit");
		    statement.executeUpdate("CREATE TABLE SiteVisit (page_id TEXT PRIMARY KEY , "
		    		+ "event_time DATE NOT NULL, customer_id TEXT NOT NULL, tags TEXT, FOREIGN KEY(customer_id) "
		    		+ "REFERENCES customer(customer_id))");
		    
		    statement.executeUpdate("drop table if exists image");
		    statement.executeUpdate("CREATE TABLE image(image_id TEXT PRIMARY KEY ,"
		    		+ " event_time DATE NOT NULL, customer_id TEXT NOT NULL, camera_make TEXT, camera_model TEXT, "
		    		+ "FOREIGN KEY(customer_id) REFERENCES customer(customer_id))");
		    
		    statement.executeUpdate("drop table if exists Orders");
		    statement.executeUpdate("CREATE TABLE Orders (order_id TEXT PRIMARY KEY , "
		    		+ "event_time DATE NOT NULL, customer_id TEXT NOT NULL, total_amount REAL NOT NULL, "
		    		+ "FOREIGN KEY(customer_id) REFERENCES Customer(customer_id))");
		    
		    statement.executeUpdate("drop table if exists analytical_LTV");
		    statement.executeUpdate("CREATE TABLE analytical_LTV (customer_id  TEXT PRIMARY KEY , "
		    		+ "LTV REAL, FOREIGN KEY(customer_id) REFERENCES Customer(customer_id))");
		  
		} 

	     finally
	     {
	    	 if (connection != null)
	    	 {
	    		 dbo.closeConnection(connection);
	    	 }
	    	 
	     }
	}
	
	/*
	 * Load CUSTOMER type data into customer table. Based on Verb value it will insert or update
	 * 
	 */
	public void loadCustomerData(JSONObject jsonObject)
	{
		
		sqlLiteConnection dbo=new sqlLiteConnection();
		
		try {
			
			if (jsonObject.get("verb").equals("NEW"))
			{
				connection = dbo.getConnection();
			
				pstmt = connection.prepareStatement("INSERT INTO customer VALUES (?,?,?,?,?)");  
			
				pstmt.setString(1, (String) jsonObject.get("key"));
				pstmt.setString(2, (String) jsonObject.get("event_time"));
				pstmt.setString(3, (String) jsonObject.get("last_name"));
				pstmt.setString(4, (String) jsonObject.get("adr_city"));
				pstmt.setString(5, (String) jsonObject.get("adr_state"));
		    
				pstmt.executeUpdate();  
		  
			}
			else if (jsonObject.get("verb").equals("UPDATE"))
			{
				connection = new sqlLiteConnection().getConnection();
				pstmt = connection.prepareStatement("UPDATE customer set event_time=?,adr_city=?,adr_state=?,"
												 + "last_name =? where customer_id=?"); 
				
				pstmt.setString(1, (String) jsonObject.get("event_time"));
				pstmt.setString(2, (String) jsonObject.get("adr_city"));
				pstmt.setString(3, (String) jsonObject.get("adr_state"));
				pstmt.setString(4, (String) jsonObject.get("last_name"));
				pstmt.setString(5, (String) jsonObject.get("key"));
				
			}
			else
			{
				System.out.println("Warning: Incorrect Type while updating customer data, skipping record");
			}
		  
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  

	     finally
	     {
	    	 if (connection !=null)
	    	 {
	    		 try {
					 
	    			 dbo.closeConnection(connection);
					 
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	 }
	    	 
	     }
		
	}
	
	/*
	 * load Site Visit data into SiteVisit table
	 */
	
	public void loadSiteVisitData(JSONObject jsonObject)
	{
		try {
			
			if (jsonObject.get("verb").equals("NEW")) {
				connection = new sqlLiteConnection().getConnection();
			
				pstmt = connection.prepareStatement("INSERT INTO SiteVisit VALUES (?,?,?,?)");  
			
				pstmt.setString(1, (String) jsonObject.get("key"));
				pstmt.setString(2, (String) jsonObject.get("event_time"));
				pstmt.setString(3, (String) jsonObject.get("customer_id"));
				pstmt.setString(4, jsonObject.get("tags").toString());
			
		    
				pstmt.executeUpdate();  
		 
			}
		   else {
			   System.out.println("Warning: Incorrect Type while updating SITEVISIT data, skipping record");
		   }
	  
	    } 
		catch (SQLException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    }
		finally
		{
			if (connection !=null)
			{
				try {
					// pstmt.close();
					new sqlLiteConnection().closeConnection(connection);
				 
				} 
				catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
   }
	
	/*
	 * load Image data into Image table
	 */
	
	public void loadImageData(JSONObject jsonObject)
	  {
		
		try {	
			if (jsonObject.get("verb").equals("UPLOAD")) {
			
				connection = new sqlLiteConnection().getConnection();
			
				pstmt = connection.prepareStatement("INSERT INTO image VALUES (?,?,?,?,?)");  
			
				pstmt.setString(1, (String) jsonObject.get("key"));
				pstmt.setString(2, (String) jsonObject.get("event_time"));
				pstmt.setString(3, (String) jsonObject.get("customer_id"));
				pstmt.setString(4, (String) jsonObject.get("camera_make"));
				pstmt.setString(5, (String) jsonObject.get("camera_model"));
			
		    
				pstmt.executeUpdate();  		
			}
			else {
				System.out.println("Warning: Incorrect Type while updating IMAGE data, skipping record");
			}
	  
	    } catch (SQLException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    }  

		finally {
			if (connection !=null) {
				try {
					// pstmt.close();
					new sqlLiteConnection().closeConnection(connection);
				 
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/*
	 * load orders data into ORDERS table. Skip bad record
	 */
	
	public void loadOrdersData(JSONObject jsonObject)	
	{
		try {
			if (jsonObject.get("verb").equals("NEW")) {
				connection = new sqlLiteConnection().getConnection();
			
				pstmt = connection.prepareStatement("INSERT INTO Orders VALUES (?,?,?,?)");  
			
				pstmt.setString(1, (String) jsonObject.get("key"));
				pstmt.setString(2, (String) jsonObject.get("event_time"));
				pstmt.setString(3, (String) jsonObject.get("customer_id"));
				pstmt.setString(4, (String) jsonObject.get("total_amount"));
			
		    
				pstmt.executeUpdate();  
			}
			else if (jsonObject.get("verb").equals("UPDATE")) {
				connection = new sqlLiteConnection().getConnection();
				pstmt = connection.prepareStatement("UPDATE Orders set event_time=?,customer_id=?,total_amount=?"
												 + " where order_id=?"); 
				
				pstmt.setString(1, (String) jsonObject.get("event_time"));
				pstmt.setString(2, (String) jsonObject.get("customer_id"));
				pstmt.setString(3, (String) jsonObject.get("total_amount"));
				pstmt.setString(4, (String) jsonObject.get("order_id"));
				
			}
			else {
				System.out.println("Warning: Incorrect Type while updating customer data, skipping record");
			}
		  
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		finally {
	    	 if (connection !=null) {
	    		 try {
					 new sqlLiteConnection().closeConnection(connection);
				 } 
	    		 catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	 }
	     }		
	}	
}
