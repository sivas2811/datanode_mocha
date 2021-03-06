

/* topic_content : topic,content, user_name,time_stamp
 * user_topic: user_name,topic,time_stamp
*/
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * A TCP server that runs on port 9090.  When a client connects, it
 * sends the client the current date and time, then closes the
 * connection with that client.  Arguably just about the simplest
 * server you can write.
 */
public class DataNode {
	String masterIP;
	String middleWareIP;
	Connection con;
	public DataNode() 
	{
		this.masterIP = "23.22.232.19"; 
                this.middleWareIP = "54.226.150.230";
	}
	
	public String getMasterIP(){
		return this.masterIP;
	}
	
	public String getmiddleWareIP(){
		return this.middleWareIP;
	}
	
	public Connection getCon(){
		return this.con;
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
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
                String sql_tc = "delete from topic_content;";
                String sql_ut = "delete from user_topic;";
                String sql_uf = "delete from user_follow;";
                stmt.executeUpdate(sql_tc);
                stmt.executeUpdate(sql_ut);
                stmt.executeUpdate(sql_uf);

                stmt.close();
                con.close();


		 ExecutorService executor = Executors.newFixedThreadPool
                 (10, new ThreadFactory(){
                         public Thread newThread(Runnable runnable){
                                 Thread thread = new MyThreadFactory().newThread(runnable);
                                 return thread;
                         }
                 });
		 DataNode dataNode = new DataNode();
		//Listen to messages from middleWare
	        ServerSocket listener = new ServerSocket(8000);
        	Socket socket;
        
	        while (true) {
        		socket = listener.accept();
   			//System.out.println("connection received from middleware"); 
        		BufferedReader in = new BufferedReader
            			(new InputStreamReader(socket.getInputStream()));   
                	String request = in.readLine();
	                Runnable worker = new WorkerThread_DataNode(request,dataNode,socket);
        	        executor.execute(worker);  
        	}       
		//socket.close();
		//listener.close(); 
	}
}
