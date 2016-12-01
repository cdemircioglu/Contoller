//javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." Control.java -Xlint:unchecked
//java -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." Control
//java -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." Control

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.StringReader;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class Control implements Runnable {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 */
	static String runNumber = "0";
	static String xmlParameters = "";
	static String xmlParametersOld = "";
	static String lastservercnt = "";
	static int totalMSISDN = 0; 
    static String marketInterestID = ""; 
    static String sprayPrayUptake = "";

    private Thread t;
	private String threadName;
	   
	public void start () {
	      //System.out.println("Starting " +  threadName );
	      if (t == null) {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	   }
	
	Control( String name, int myvalue) {
	      threadName = name;
	   }
    
    
	public void run() 
	{		
		
		if (threadName == "T1" )
        {
        	System.out.println("Receiving the messages.");
        	receiveMessage();
        }
        if (threadName == "T2" )
        {	
        	System.out.println("Query DB");
        	while (true) //Loop indefinetly
        	{	
        		try {
        				if(xmlParameters != "")	
        				{
        					if (!xmlParameters.equals(xmlParametersOld)) //Make sure we are receiving new parameters. 
        					{
        						System.out.println("Processing the XML message.");
        						xmlParametersOld = xmlParameters;
        						processXML(xmlParameters);        						
        					}
        					Thread.sleep(1000);
        					
        				}
        				Thread.sleep(1000); 
        					
					} catch (Exception e) {
						e.printStackTrace();
					}
        	}
        }
        
	}

	public static void setRunNumber() {
		//This is the number used to set the individual runs. 
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar cal = Calendar.getInstance();
		runNumber = dateFormat.format(cal.getTime());
	}
	
	 
	  public static void asynchCallDBStoredProcedure() {
		  	ExecutorService executor = Executors.newSingleThreadExecutor();

	        //creates a DB thread pool
		  	executor.execute(new Runnable() {
	            @Override
	            public void run() {
	    			try {
						Class.forName("com.mysql.jdbc.Driver");
		    	        java.sql.Connection con = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/openroads","root","KaraburunCe2");
		    	        PreparedStatement stmt = con.prepareStatement("CALL spc_marketinterest()");
    					stmt.execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
					    System.out.println(" [x] Sent '" + e.getMessage() + "'");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    System.out.println(" AAAA ");
					    

					}

	            }
	        });
	  }

	public static void processXML(String xmlMsg) throws SAXException, IOException, TimeoutException, SQLException {
		//Create the lists to hold parameters
		List parameterName = new ArrayList();
		List parameterValue = new ArrayList();
		
		//Time to get the parameters as a list
		getParameters(xmlMsg, parameterName, parameterValue);		 
		
		//Let's get the MSISDN list based on market interest		
		String marketInterest = parameterValue.get(parameterName.indexOf("marketInterest")).toString();
		
		//AAAA SET MARKETINTERESTID & SPRAYANDPRAY
    	try {
			Class.forName("com.mysql.jdbc.Driver");
	        java.sql.Connection con = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/openroads","root","KaraburunCe2");
	        PreparedStatement stmt = con.prepareStatement("SELECT MARKETINTERESTID,SPRAYPRAYUPTAKE FROM dim_marketinterest WHERE marketinterest = ?");
	        stmt.setString(1,marketInterest);
	        	        
	        // Execute the query, and get a java resultset                         
	        ResultSet rsMARKETINTEREST = stmt.executeQuery();

	        while (rsMARKETINTEREST.next())
	        {
                marketInterestID = rsMARKETINTEREST.getString("MARKETINTERESTID");
                sprayPrayUptake = rsMARKETINTEREST.getString("SPRAYPRAYUPTAKE");
	        }
	        
	        stmt.close();
            con.close();

			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
		//Let's get the server count
		String servercnt = parameterValue.get(parameterName.indexOf("servercnt")).toString();
				
		//One of the most important business logic to continue work when just server count changed. 
		if (!servercnt.equals(lastservercnt) && lastservercnt != "") //The user just changed the server count 
		{
			//stopServer();			
			startServer(servercnt);	
			lastservercnt = servercnt; //Set the server count
			return;
		} else //The use has changed a parameter
		{
			clearMessageGeneric("workermsisdn"); //Clear the queue of workermsisdn
			//stopServer();			
			startServer(servercnt);
			lastservercnt = servercnt; //Set the server count
		}	
		
		//Let's send the message based on market interest
		createMessage(marketInterest);
	}

	public static void startServer(String servercnt) throws IOException, TimeoutException {
		    //Start the server based on servercnt
			sendMessageExchangeGeneric(servercnt,"worknumber");			
	}
	
	public static void sendMessageExchangeGeneric(String servercnt, String echangeName) throws IOException, TimeoutException
	{
		String EXCHANGE_NAME = echangeName;
		ConnectionFactory factory = new ConnectionFactory();
		
		factory.setHost("hwcontrol.cloudapp.net");
		factory.setUsername("controller");
		factory.setPassword("KaraburunCe2");
		Connection connection = factory.newConnection();
		connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		
	    String message = servercnt;

	    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
	    System.out.println(" [x] Sent '" + message + "'");

	    channel.close();
	    connection.close();
		
	}
	
	public static void createMessage(String marketInterest) {
		//Create the result set of MSISDN		
		try{
        	Class.forName("com.mysql.jdbc.Driver");
            java.sql.Connection con = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/openroads","root","KaraburunCe2");
            PreparedStatement stmt = con.prepareStatement("SELECT DISTINCT MSISDN FROM fct_marketinterest WHERE marketinterestID = ?");
            stmt.setString(1,marketInterestID);

            // Execute the query, and get a java resultset                         
            ResultSet rsMSISDN = stmt.executeQuery();
             
            // Message content
            String msgContent = "";
            int i = 0; //Counter
          
            // Get the total number of customers
            rsMSISDN.last(); 
            totalMSISDN = rsMSISDN.getRow();
            rsMSISDN.beforeFirst();
            
            //Before we create new messages based on the new parameter set, we need to clear the current queue
            clearMessage();
            
            //Call the async stored procedure
            asynchCallDBStoredProcedure();
            
            // Iterate through the java resultset
            while (rsMSISDN.next())
            {
               msgContent += rsMSISDN.getString("MSISDN") + ",";
               i++; //Count the records //Only 10 records at a time
               if(i%100==0)
               {
            	   msgContent = msgContent.substring(1, msgContent.length() - 1);
            	   //System.out.println(msgContent);
            	   sendMessage(msgContent);
            	   msgContent = "";
               }
               
               if(!xmlParametersOld.equals(xmlParameters)) //If there is a new xml message stop sending immediately. 
            	   rsMSISDN.last();
               
             }
            
            //Send the last message if there is one. 
            if (msgContent.length() > 1)
            {
            	//At the end send the last content
            	sendMessage(msgContent.substring(1, msgContent.length() - 1));     	      	     
            }
            
            // Deal with the connection
            stmt.execute();
            stmt.close();
            con.close();

             
        } catch (Exception e) {
             e.printStackTrace();
        }
	}

	public static void getParameters(String xmlMsg, List parameterName,
			List parameterValue) throws SAXException, IOException {
		try {
			 	//Create the document factory
				DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				InputSource is = new InputSource();
				is.setCharacterStream(new StringReader(xmlMsg));
			
				//Set the node
				Document doc = db.parse(is);
			    NodeList nodes = doc.getElementsByTagName("parameter");
			    			    			    
			    //Read parameters into two lists			  
			    for (int i = 0; i < nodes.getLength(); i++) 
			    {
			    	//Retreive the element
			        Element element = (Element) nodes.item(i);

			        NodeList name = element.getElementsByTagName("name");
			        Element line = (Element) name.item(0);			        			        			        
			        parameterName.add(getCharacterDataFromElement(line));
			        
			        NodeList title = element.getElementsByTagName("value");
			        line = (Element) title.item(0);
			        parameterValue.add(getCharacterDataFromElement(line));			        
			    }

			    
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getCharacterDataFromElement(Element e) {
	    Node child = e.getFirstChild();
	    if (child instanceof CharacterData) {
	      CharacterData cd = (CharacterData) child;
	      return cd.getData();
	    }
	    return "";
	  }

	public static void receiveMessage()
	{
		try {
		String QUEUE_NAME = "workorder";
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("hwcontrol.cloudapp.net");
		factory.setUsername("controller");
		factory.setPassword("KaraburunCe2");
		Connection connection;		
		connection = factory.newConnection();
		Channel channel = connection.createChannel();		

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

	    Consumer consumer = new DefaultConsumer(channel) {
	        @Override
	        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	            throws IOException {
	        		String xmlMsg = new String(body, "UTF-8");	          	          
	        		System.out.println(xmlMsg); //Get the new message
	        		setRunNumber();  //Set the run number
	        		xmlParameters = xmlMsg; //Set the xml message
	        		//processXML(xmlMsg); //Process the xml message
	        }
	      };
	      channel.basicConsume(QUEUE_NAME, true, consumer);
		
		} catch (Exception e) {
			// TODO Auto-generated catch block			
		}
		

	}

	public static void clearMessage()
	{
		try {
			
			String QUEUE_NAME = "workermsisdn";
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.setHost("hwcontrol.cloudapp.net");
			factory.setUsername("controller");
			factory.setPassword("KaraburunCe2");
			Connection connection;
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
		    channel.queueDeclare(QUEUE_NAME, false, false, false, null);		    
		    channel.queuePurge(QUEUE_NAME); //Purge the queue
		    
		    channel.close();
		    connection.close();
		    
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	    	
		
	}

	public static void sendMessage(String msisdn)
	{
		try {
			
			String QUEUE_NAME = "workermsisdn";
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.setHost("hwcontrol.cloudapp.net");
			factory.setUsername("controller");
			factory.setPassword("KaraburunCe2");
			Connection connection;
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
		    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		    
		    String MSISDNMessage = "<parameter><name>msisdn</name><value>"+ msisdn +"</value></parameter>"; //MSISDN XML
		    String RunNumber = "<parameter><name>runnumber</name><value>" + runNumber + "</value></parameter>"; //runNumber XML
		    String RunTime = "<parameter><name>runtime</name><value>"+totalMSISDN+"</value></parameter>"; //TODO run time
		    
		    String MarketInterestID = "<parameter><name>marketinterestid</name><value>"+marketInterestID+"</value></parameter>"; //TODO run time
		    String SprayPrayUptake = "<parameter><name>sprayprayuptake</name><value>"+sprayPrayUptake+"</value></parameter>"; //TODO run time
		    
		    String FinalMessage = xmlParameters.replace("</ShinnyParameters>", ""); //remove the close of xml		    
		    FinalMessage = FinalMessage + RunNumber + RunTime + MSISDNMessage + MarketInterestID + SprayPrayUptake +"</ShinnyParameters>";   
		    		    
		    channel.basicPublish("", QUEUE_NAME, null, FinalMessage.getBytes("UTF-8"));
		    System.out.println(" [x] Sent '" + FinalMessage + "'");

		    channel.close();
		    connection.close();
		    
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	    	
		
	}

	public static void clearMessageGeneric(String queueName)
	{
		try {
			
			String QUEUE_NAME = queueName;
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.setHost("hwcontrol.cloudapp.net");
			factory.setUsername("controller");
			factory.setPassword("KaraburunCe2");
			Connection connection;
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
		    channel.queueDeclare(QUEUE_NAME, false, false, false, null);		    
		    channel.queuePurge(QUEUE_NAME); //Purge the queue
		    
		    channel.close();
		    connection.close();
		    
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	    	
		
	}
	
	public static void sendMessageGeneric(String msg, String queueName)
	{
		try {
			
			String QUEUE_NAME = queueName;
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.setHost("hwcontrol.cloudapp.net");
			factory.setUsername("controller");
			factory.setPassword("KaraburunCe2");
			Connection connection;
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
		    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		    		    		    
		    channel.basicPublish("", QUEUE_NAME, null, msg.getBytes("UTF-8"));

		    channel.close();
		    connection.close();		    
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	    	
		
	}

}
