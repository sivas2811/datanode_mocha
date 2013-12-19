import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
 
public class WorkerThread_DataNode implements Runnable {
     
    private String input;
    private DataNode datanode;
    //private Connection con;
     
    public WorkerThread_DataNode(String input,DataNode datanode){
        this.input=input;
        this.datanode = datanode;
    //    this.con = datanode.getCon();
    }

  // s;tc;topic;content (success), s;up;user;password(success), s;ut;user;topic (success)
  // r;up;user;password(authenticated), r;tc;topic(content), r;ut;user(topic)
        
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Start. Command = "+input);
        try {
                        processCommand(Thread.currentThread().getName());
                } catch (IOException e) {
                        e.printStackTrace();
                } catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    }
 
    private void processCommand(String port) throws UnknownHostException, IOException, SQLException {
    	
    	String[] splits = input.split(";");
        
        // s;tc;topic;content;user_name (success), 
        //s;ut;user;topic(success)
        if(splits[0].equalsIgnoreCase("s")){
        	if(splits[1].equalsIgnoreCase("tc")){
        		try{
        			

        			 String location = "jdbc:mysql://localhost:3306/mochadb";
        	         String user="root";
        	         String password = "root";
        	         Connection con = null;
        	         try {
        				Class.forName("com.mysql.jdbc.Driver");
        				con =DriverManager.getConnection(location, user, password);
        			} catch (ClassNotFoundException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
        			} catch (SQLException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
        			}
        			
        			
        			
        	         Statement stmt = con.createStatement();
            		String sql = "INSERT INTO topic_content values('"
                        		+splits[2]+Math.random()+"','"+splits[3]+"','"+splits[4]+"',NOW());";
            		System.out.println("SQL statement:" + sql);
                    stmt.executeUpdate(sql);
                    
                    stmt.close();
                    con.close();
                    
                    //send success to middleWare
                    String serverAddress = datanode.getmiddleWareIP();
                    System.out.println("Trying to create socket");
                    Socket s = new Socket(serverAddress,Integer.parseInt(splits[5]));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                    out.println("success");
                    s.close();
                    
                   
                   }
        		
                 catch(SQLException e){
                	 System.out.println("in exception ");
                	 String serverAddress = datanode.getmiddleWareIP();
                     Socket s = new Socket(serverAddress,Integer.parseInt(splits[5]));
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                     out.println("failure");
                     s.close();		
                   }
        		}
        		else if(splits[1].equalsIgnoreCase("ut")){
        			try{

           			 String location = "jdbc:mysql://localhost:3306/mochadb";
           	         String user="root";
           	         String password = "root";
           	         Connection con = null;
           	         try {
           				Class.forName("com.mysql.jdbc.Driver");
           				con =DriverManager.getConnection(location, user, password);
           			} catch (ClassNotFoundException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			} catch (SQLException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			}
           			
           			
           			
           	         Statement stmt = con.createStatement();
                		String sql = "INSERT INTO user_topic values('"
                            		+splits[2]+Math.random()+"','"+splits[3]+"',NOW());";
                		System.out.println("SQL statement:" + sql);
                        stmt.executeUpdate(sql);
                        
                        //send success to middleWare
                        String serverAddress = datanode.getmiddleWareIP();
                        System.out.println("Trying to create socket to server" + serverAddress);
                        Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
                        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                        out.println("success");
                        s.close();
                        con.close();
                        stmt.close();
                       }
                     catch(SQLException e){
                    	 System.out.println("in exception ");
                    	 String serverAddress = datanode.getmiddleWareIP();
                         Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
                         PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                         out.println("failure");
                         s.close();		
                       }
                	
            	}
        	
        		else {
        			try{

           			 String location = "jdbc:mysql://localhost:3306/mochadb";
           	         String user="root";
           	         String password = "root";
           	         Connection con = null;
           	         try {
           				Class.forName("com.mysql.jdbc.Driver");
           				con =DriverManager.getConnection(location, user, password);
           			} catch (ClassNotFoundException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			} catch (SQLException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			}
           			
           			
           			
           	         Statement stmt = con.createStatement();
                		String sql = "INSERT INTO user_follow values('"
                            		+splits[2]+"','"+splits[3]+"');";
                		System.out.println("SQL statement:" + sql);
                        stmt.executeUpdate(sql);
                        
                        //send success to middleWare
                        String serverAddress = datanode.getmiddleWareIP();
                        System.out.println("Trying to create socket");
                        Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
                        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                        out.println("success");
                        s.close();
                        con.close();
                        stmt.close();
                       }
                     catch(SQLException e){
                    	 System.out.println("in exception ");
                    	 String serverAddress = datanode.getmiddleWareIP();
                         Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
                         PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                         out.println("failure");
                         s.close();		
                       }
                	
            	}
        }
   
       else{
		System.out.println("in else\n");
        	if(splits[1].equalsIgnoreCase("tc")){
        		// r;tc;topic(content),
        		try{
           			 String location = "jdbc:mysql://localhost:3306/mochadb";
           	         String user="root";
           	         String password = "root";
           	         Connection con = null;
           	         try {
           				Class.forName("com.mysql.jdbc.Driver");
           				con =DriverManager.getConnection(location, user, password);
           			} catch (ClassNotFoundException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			} catch (SQLException e) {
           				// TODO Auto-generated catch block
           				e.printStackTrace();
           			}
           			
           			
           			
           	         Statement stmt = con.createStatement();
            		String query = "SELECT user_name,content from topic_content" +
            				" WHERE topic LIKE '"+splits[2]+"%' ORDER BY time_stamp DESC;";
            		ResultSet rs = stmt.executeQuery(query);
            		ArrayList<String> contentList = new ArrayList<String>();
            		
            	    while (rs.next()) {
            	    	contentList.add(rs.getString("user_name")+" says: "+rs.getString("content"));
            	    }
            	    rs.close();
            	    stmt.close();
            	    con.close();
            	    String serverAddress = datanode.getmiddleWareIP();
            	    Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
            	    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            	    oos.writeObject(contentList);
            	    oos.close();
            	    
                    s.close();
                   }
                 catch(SQLException e){
                	 String serverAddress = datanode.getmiddleWareIP();
                     Socket s = new Socket(serverAddress,Integer.parseInt(splits[4]));
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                     out.println("failure");
                     s.close();		
                   }
        		}
        	else if(splits[1].equalsIgnoreCase("ut")) {
        			 //  r;ut;user(topic)
        			try{
        				
        				
                			

               			 String location = "jdbc:mysql://localhost:3306/mochadb";
               	         String user="root";
               	         String password = "root";
               	         Connection con = null;
               	         try {
               				Class.forName("com.mysql.jdbc.Driver");
               				con =DriverManager.getConnection(location, user, password);
               			} catch (ClassNotFoundException e) {
               				// TODO Auto-generated catch block
               				e.printStackTrace();
               			} catch (SQLException e) {
               				// TODO Auto-generated catch block
               				e.printStackTrace();
               			}
               			
               			
               			
               	         Statement stmt = con.createStatement();
                		String query = "SELECT topic from user_topic" +
                				" WHERE user_name LIKE '"+splits[2]+"%';";
                		ResultSet rs = stmt.executeQuery(query);
                		ArrayList<String> contentList = new ArrayList<String>();
                	    while (rs.next()) {
                	    	contentList.add(rs.getString("topic"));
                	    }
                	    rs.close();
                	    stmt.close();
                	    con.close();
			    System.out.println("after query exec");
                	    String serverAddress = datanode.getmiddleWareIP();
                	    Socket s = new Socket(serverAddress,Integer.parseInt(splits[3]));
                	    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                	    oos.writeObject(contentList);
                	    oos.close();
                        s.close();
                       }
                     catch(SQLException e){
                    	 String serverAddress = datanode.getmiddleWareIP();
                         Socket s = new Socket(serverAddress,Integer.parseInt(splits[3]));
                         PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                         out.println("failure");
                         s.close();		
                       }
                	
            	}
        	
        	else  {
   			 //  r;ut;user(topic)
   			try{
   				
   				
           			

          			 String location = "jdbc:mysql://localhost:3306/mochadb";
          	         String user="root";
          	         String password = "root";
          	         Connection con = null;
          	         try {
          				Class.forName("com.mysql.jdbc.Driver");
          				con =DriverManager.getConnection(location, user, password);
          			} catch (ClassNotFoundException e) {
          				// TODO Auto-generated catch block
          				e.printStackTrace();
          			} catch (SQLException e) {
          				// TODO Auto-generated catch block
          				e.printStackTrace();
          			}
          			
          			
          		 System.out.println("Input received is " +input);		
          	         Statement stmt = con.createStatement();
           		String query = "SELECT following from user_follow" +
           				" WHERE user_name='"+splits[2]+"';";
           		System.out.println("Read user_follow:"+ query);
           		ResultSet rs = stmt.executeQuery(query);
           		ArrayList<String> contentList = new ArrayList<String>();
           	    while (rs.next()) {
           	    	contentList.add(rs.getString("following"));
           	    }
           	    rs.close();
           	    stmt.close();
           	    con.close();
           	    String serverAddress = datanode.getmiddleWareIP();
			
		    System.out.println("Sending to middleware address" + serverAddress);
           	    Socket s = new Socket(serverAddress,Integer.parseInt(splits[3]));
			System.out.println("I am here yaya");
           	    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
           	    oos.writeObject(contentList);
           	    oos.close();
                   s.close();
                  }
                catch(SQLException e){
               	 String serverAddress = datanode.getmiddleWareIP();
                    Socket s = new Socket(serverAddress,Integer.parseInt(splits[3]));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                    out.println("failure");
                    s.close();		
                  }
           	
       	}
        	
        	
        	}
           
                 
                 
    }
         
    @Override
    public String toString(){
        return this.input;
    }
}
