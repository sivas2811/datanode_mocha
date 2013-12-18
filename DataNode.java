

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
		this.masterIP = "54.235.60.113";//"54.196.140.87";
                this.middleWareIP ="50.17.52.54"; //"23.21.9.190";    
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
        		BufferedReader in = new BufferedReader
            			(new InputStreamReader(socket.getInputStream()));   
                String request = in.readLine();
                Runnable worker = new WorkerThread_DataNode(request,dataNode);
                executor.execute(worker);  
                
                   
        }
	}
}
