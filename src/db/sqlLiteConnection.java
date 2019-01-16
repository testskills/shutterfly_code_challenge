package db;

import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;

public class sqlLiteConnection {
	
	/*
	 * Create sample SQLLite schema and connect
	 * To Do: Connection pooling if parallel connections increased and we see lot of Login/Logout events
	 * at the database
	 */
	public Connection getConnection ()
	{
		Connection connection = null;
		try
		{
			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
		}
		catch(SQLException e)
	    {
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
	    }	
		catch(Exception e)
	    {
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
	    }
		return connection;
	}
	
	/*
	 * Close database connection 
	 */
	public  void closeConnection (Connection connection)
	{
		 try
	      {
	        if(connection != null) {
		          connection.close();
	        }
	      }
	      catch(SQLException e)
	      {
	        // connection close failed.
	        System.err.println(e);
	      }
	}

}
