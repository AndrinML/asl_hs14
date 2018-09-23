package mps.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;
import java.util.logging.Logger;

import mps.config.Config;
import mps.config.ConfigFile;
import mps.request.Request;
import mps.request.Response;
import mps.statistics.StatisticsFormatter;
import mps.statistics.StatisticsLogger;

public class Client implements Runnable {
	
	// Assign global singleton Config class
	private static Config config;
	
	private static Logger statisticsLog;
	
	// Uniquely defined client id
	private int clientId;
	
	// The host:port combination to connect to
	private InetAddress hostAddress;
	private int port;

	// Each client uses its own socket
	private Socket clientSocket;
	
	// Unique request id. For statistic purposes
	private int requestId;
	
	// Data input stream in which primitive data will be read from the socket
	private DataInputStream in;
	
	// Data output stream to which primitive data will be written to the socket 
	private DataOutputStream out;
	
	// STATS
	// Current number of the messages sent
	private int numRequestsSent;
	
	// Number of messages to send per second
	private int numRequestsPerSecond;
	
	// Count number of requests
	private int numOfSuccessfulRequests;
	
	// Total number of messages that should have been sent
	private int totNumberOfRequests;
	
	// Time to think
	private long thinkTime;
	
	private long sleepTime;
	
	private long timer;
	
	// Time when request sent
	private long startTimeRequestSent;
	
	// Time when a response to a given request arrives
	private long endTimeResponseReceived;
	
	private int msgSize;

	// STATS END
	
	
	/**
	 * Constructor of Client
	 * @param hostAddress
	 * @param port
	 * @param clientId
	 * @param numMsgPerSecond
	 * @throws IOException
	 */
	public Client(InetAddress hostAddress, int port, int clientId) throws IOException {
		
		config = Config.getInstance();
		
		/*
		 * STATISTICS
		 * STATS Configuration for the experiments
		 */
		// Set unique request id initially to 0
		this.requestId = 0;
		
		// Set number of messages sent initially to 0
		this.numRequestsSent = 0;
		
		// Set number of requests sent initially to 0
		this.numOfSuccessfulRequests = 0;
		
		// Start initial thinking time
		this.thinkTime = 0; // thinking time in milliseconds
		
		this.msgSize = config.messageSize;
		
		this.sleepTime = 400;
		// STATS END
		
		/*
		 *  Assign host address and port to each client, 
		 *  this is necessary information for connecting to the socket
		 */
		this.hostAddress = hostAddress;
		this.port = port;
		
		// Assign unique client id
		this.clientId = clientId;
		
		// Number of messages the client should send per second
		this.numRequestsPerSecond = config.numOfRequestsPerSecond;
		
		/*
		 * The total amount of messages that each client should have sent
		 * before shutting down
		 */
		this.totNumberOfRequests = config.totalNumberOfRequestsToSend;
		
		/*
		 * Prepare for sending and receiving messages
		 * Connect to given hostAdress at the given port
		 */
		initiateConnection();
	}
	
	/**
	 * This function is called in the constructor
	 * A socket connection to the server is established and
	 * input and output buffers are opened 
	 */
	public void initiateConnection() throws IOException {
		
		// Create new socket and connect to host
		this.clientSocket = new Socket(this.hostAddress, this.port);
		
		if (config.DEBUG) {
			System.out.println("Client: " + clientId + " succesfully established connection to: " + this.hostAddress + " , port: " + this.port);
		}
		
		/*
		 * Use BufferedInputStream between DataInputStream and socket
		 * to avoid too many system calls
		 */
		BufferedInputStream bis = new BufferedInputStream(this.clientSocket.getInputStream());
		in = new DataInputStream(bis);
		
		/*
		 * Use BufferedOutputStream between DataOutputStream and socket
		 * to avoid too many system calls
		 */
		BufferedOutputStream bos = new BufferedOutputStream(this.clientSocket.getOutputStream());
		out = new DataOutputStream(bos);
	}
	
	/**
	 * To close the socket and all corresponding buffers
	 * finishConnection() is called at the very end of each experiment
	 */
	public void terminateConnection() throws IOException {
		
		// Try to close the active socket after all activity is done
		try {
			
			// Try to closing all open buffers
			this.in.close();
			this.out.close();
			
			// Try closing the current socket
			this.clientSocket.close();
			
			System.out.println("Client: " + this.clientId + " succesfully disconnected");	
			
		} catch (IOException e) {
			
			System.out.println(e);
		}
	
	}

	public void run() {
		
		System.out.println("client: " + this.clientId + " starts sending requests...");
		
		// STATS EXPERIMENT
		
		this.timer = System.nanoTime();
		
		/*
		 * Start the actual action loop of a client
		 * All important requests happen here
		 */		
		while (this.numRequestsSent < this.totNumberOfRequests) {
		//while(this.numRequestsPerSecond < goalMsgsPerSecond) { // STATS Increase messages per second
			
			// Send requests and listen for response
			try {
						
				// STATS
				/*if (config.DEBUG) {
					System.out.println(System.currentTimeMillis() / 1000);
					System.out.println(this.numRequestsPerSecond);
					System.out.println("sleep time: " + (this.sleepTime));
				}
				*/
				/* 
				 * STATS
				 * For a balanced system. Send m messages then receive m messages
				 * SEND 50%, RECEIVE 50% This process should take exactly 1 second
				 */
				for (int i = 0; i < this.numRequestsPerSecond / 2; ++i) {
					
					// STATS
					this.thinkTime = System.nanoTime();
					
					sendRandomMessage();
					
					this.thinkTime = (System.nanoTime() - this.thinkTime) / 1000000;
					//System.out.println("think time: " + this.thinkTime/1000000);
					
					getResponse();
					
					//Thread.sleep(Math.max(--this.sleepTime,0));
					
					//STATS
					this.sleepTime = Math.max((1000 / this.numRequestsPerSecond) - this.thinkTime, 0);	

					// Only sleep the number of requests per second minus think time
					Thread.sleep(this.sleepTime);
					
					// STATS	
					this.thinkTime = System.nanoTime();

					receiveMessageFromQueueWithDelete();
					
					this.thinkTime = (System.nanoTime() - this.thinkTime) / 1000000;
					//System.out.println("think time: " + this.thinkTime/1000000);
					
					getResponse();
					
					//STATS
					this.sleepTime = Math.max((1000 / this.numRequestsPerSecond) - this.thinkTime, 0);	

					// Only sleep the number of requests per second minus think time
					Thread.sleep(this.sleepTime);
				}
				
				// STATS
				//this.numRequestsPerSecond = this.numRequestsPerSecond + 10;
				// STATS END

				
				// Sending again m messages, should take again 1 second
				/*for (int i = 0; i < this.numRequestsPerSecond; ++i) {

					// STATS					
					receiveMessageFromQueueWithDelete();
					getResponse();

					//STATS
					this.sleepTime = (1000 / this.numRequestsPerSecond) + this.thinkTime;
					if (this.sleepTime < 0) {
						this.sleepTime = 0;
					}
															
					// Only sleep the number of requests per second minus think time
					Thread.sleep(this.sleepTime);
				}
				*/
				// STATS:
				// Experiment: Increase message size after 2 seconds by 10
				//this.msgSize = this.msgSize + 10;
				
				/*if (((System.nanoTime() - this.timer) / 1000000000) >= 10) { // IMPORTANT Update every 10 seconds
				
					this.numRequestsPerSecond = this.numRequestsPerSecond + 10;

					//System.out.println(((System.nanoTime() - this.timer) / 1000000000) + ":req/sec=" + this.numRequestsPerSecond);

					this.timer = System.nanoTime();
				}*/
				
				//this.numRequestsPerSecond = this.numRequestsPerSecond + 2;

				// STATS END
				
				// IMPORTANT
				// Remaining request types
//				receiveMessageFromQueueWithDelete(80);
//				getResponse();				
				
//				deleteQueue();
//				createQueue(999);
//				getResponse();
				
//				receiveMessageFromReceiver();
//				getResponse();
				
//				receiveMessageFromQueue();
//				getResponse();
				
//				queryForQueue();
//				getResponse();
				// IMPORTANT
				
			} catch (Exception e) {
				
				e.printStackTrace();
				
			}
			
		}
		
		try {
			
			// After all messages sent and run() terminates close all open connections
			terminateConnection();
			
		} catch (IOException e) {
			
			// Exception if failed to close all connections
			e.printStackTrace();
		}
		
	}

	// Write data into the socket buffer
	private void write(Request request) throws IOException {
		
		/*
		 * This is a simplified serialization of the request object
		 * Write all fields of the Request object to the socket
		 * IMPORTANT: Based on the type of request the message body is sent as well
		 */
		
		// Write unique (client-wide) request id for statistics
		out.writeInt(request.requestId);
		
		// Write request type
		out.writeInt(request.type);
		
		// Write sender id
		out.writeInt(request.senderId);
		
		// Write receiver id
		out.writeInt(request.receiverId);
		
		// Write queue id
		out.writeInt(request.queueId);
		
		// Check if MSG_SEND_REQ (message send request)
		if (request.type == 0) {
			out.writeUTF(request.message);
		}
				
		// Flush written data to the stream
		out.flush();

	}
	
	// Read data from the socket input reader
	public Response read() throws IOException {
		
		// Initialize new Response object
		Response rsp = new Response();
		
		/*
		 * This is a simplified deserialization of the Response object
		 * Read all fields of the Response object to the socket
		 * IMPORTANT: Based on the type of Response the retrieved message body is sent as well
		 */
		rsp.errorType = in.readInt();
		rsp.requestId = in.readInt();
		rsp.requestType = in.readInt();
		rsp.messageId = in.readInt();
		rsp.retrievedMessage = in.readUTF();				

		// Return new response object
		return rsp;
	}
	
	/**
	 * Each request will be answered by the server in a Response object
	 * @throws IOException
	 */
	private void getResponse() throws IOException {
		
		// Read the socket buffer
		Response rsp = read();
		
		// IMPORTANT Count only successful request
		
		if (rsp.errorType <= 0) {
			
			// Increment number of successful requests
			this.numOfSuccessfulRequests += 1;
			
			// Set ending time of response received
			this.endTimeResponseReceived = System.nanoTime();
			
			// Compute elapsed milliseconds from sending request to receiving response
			double elapsedResponseTimeMilliseconds = (this.endTimeResponseReceived - this.startTimeRequestSent) / 1000000.0; // In milliseconds
			
			// Round to two decimals
			BigDecimal bd = new BigDecimal(elapsedResponseTimeMilliseconds).setScale(2, RoundingMode.HALF_UP);
			
			// Log response
			statisticsLog.info(StatisticsFormatter.formatLogRecord(this.clientId, rsp.requestId, bd.doubleValue()));
		}
		
		/*
		 * Error handling of response
		 */
		switch (rsp.errorType) {
			
			case 0: // NO_ERROR
				
				if (config.DEBUG) {
					System.out.println("Client: " + this.clientId + " gets response to request: " + rsp.requestId + ", request type: " + rsp.requestType + " with no error");
				}
				
				break;
				
			case 1: // SQL_EXECUTION_ERROR
				
				if (config.DEBUG) {
					System.out.println("Client: " + this.clientId + " gets response to request: " + rsp.requestId + ", request type: " + rsp.requestType + " with 'SQL execution error'");
				}
				
				break;
				
			case 2: // RECORD_NOT_FOUND_ERROR
				
				if (config.DEBUG) {
					System.out.println("Client: " + this.clientId + " gets response to request: " + rsp.requestId + ", request type: " + rsp.requestType + " with 'Record not found error'");
				}
				
				break;

			case 3: // PSQL_EXCEPTION_ERROR
				
				if (config.DEBUG) {
					System.out.println("Client: " + this.clientId + " gets response to request: " + rsp.requestId + ", request type: " + rsp.requestType + " with 'PSQL exception error");
				}
				
				break;
				
			default: // Default. Should not happen
				
				if (config.DEBUG) {
					System.out.println("FATAL: NOT COVERED ERROR TYPE");
				}
				
				break;
		}

		if (config.DEBUG) {
			System.out.println("Client: " + this.clientId + " received msg_id: " + rsp.messageId + " content: " + rsp.retrievedMessage);
		}

	}
	
	/**
	 * Define helper functions here. 
	 */
	
	/**
	 * Initially to all experiments populate queue table with empty queues
	 */
	private void createAllQueues() {

		/*
		 * IMPORTANT: This is done to avoid an overhead 
		 * of failing queries in the beginning
		 * 
		 * Initial call to create all queues
		 * where clients can send messages to
		 */
		for (int id = 0; id < config.numberOfQueues; ++id) {
			
			try {
				
				// Send SQL query to DB and wait for response
				createQueue(id);
				getResponse();
				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		
	}
	
	// Random number generator from 1 to maxInt [1..maxInt]
	private int createRandomInt(int maxInt) {
		Random rand = new Random();
		return (rand.nextInt(maxInt));
	}
	
	/**
	 * Random String generation 
	 * @param length
	 * @return random string
	 */
	private String createRandomString(int length) {
		
		 char[] subset = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();
			
		Random r = new Random(); 
		
		char buf[] = new char[length];
		for (int i = 0; i < buf.length; i++) {
			int index = r.nextInt(subset.length);
			buf[i] = subset[index];
		}
		
		return new String(buf);
	}
	
	
	/*
	 * Define all request functions here 
	 */
	
	/**
	 * Basic configuration of all Request objects
	 * @param req
	 */
	private void configureDefaultRequest(Request req) {
		
		// Uniquely defined request id
		int requestId = this.requestId;
	
		// Increment request id
		this.requestId += 1;
		
		// Generate random receiver id
		int receiverId = createRandomInt(config.totalNumberOfClients);
		
		// Create random queue id
		int queueId = createRandomInt(config.numberOfQueues);
		
		// Create a new request and assign all fields
		req.requestId = requestId;
		req.senderId = this.clientId;
		req.receiverId = receiverId;
		req.queueId = queueId;
		req.message = "";

	}
	
	/**
	 * Send request sends the request to the output stream
	 * @param req
	 */
	private void sendRequest(Request req) {
		
		try {
			
			/*
			 * STATS
			 */			
			// Set starting time of send request
			this.startTimeRequestSent = System.nanoTime();
			
			/*
			 * STATS END
			 */
			
			// Try to write the request to the output buffer of the socket
			write(req);
			
			// STATS
			// Increment number of sent requests
			this.numRequestsSent += 1;
			
			if (config.DEBUG) {
				System.out.println("Client: " + this.clientId + " sent request with id: " + (this.requestId) + " message body: " + req.message);
			}
			
		} catch (IOException e) {
			
			if (config.DEBUG) {
				System.out.println("ERROR: Could not send request data");
			}
			
			e.printStackTrace();
						
		}
		
	}
	
	/**
	 * Sends a request to create a new queue
	 */
	public void createQueue(int queueId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 6; // QUEUE_CREATE_REQ
		
		// Assign fixed id to queue
		req.queueId = queueId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	/**
	 * Sends a request to delete a queue identified by queueId
	 */
	public void deleteQueue() {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 7; // QUEUE_DEL_REQ
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	/**
	 * Sends a random generated message to a random queue and random client
	 */
	public void sendRandomMessage() {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 0; // MSG_SEND_REQ
		
		// Create a new message by passing the number of characters the message body should contain
		req.message = createRandomString(this.msgSize);
	
		// Randomly chose a specific receiver or not
		// 75% specific receivers, 25% not specified
		int rand = createRandomInt(100);
		if (rand <= 25) {
			req.receiverId = -1; // -1 defines NO specific receiver
		}
		
		// Send request to the output stream
		sendRequest(req);
	}

	/**
	 * Retrieve a message by a specific sender id (Request.receiveFromId)
	 * @throws IOException 
	 */
	public void receiveMessageFromReceiver() {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 1; // MSG_REC_REQ
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	/**
	 * Receive a random message from a specific random queue and
	 * delete message after it is retrieved from the DB
	 */
	public void receiveMessageFromQueueWithDelete() {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 4; // QUEUE_READ_WITH_REMOVE_REQ
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	/**
	 * Receive a message from a queue without deleting after reading
	 */
	public void receiveMessageFromQueue() {
			
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 3; // QUEUE_READ_REQ
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	/**
	 * Query for queues where messages for a random client are waiting
	 * this requests returns only one possible queueId where such messages
	 * exists
	 */
	public void queryForQueue() {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 5; // QUEUE_QUERY_REQ
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	
	
	
	
	
	/**
	 * IMPORTANT
	 * 
	 * All these methods serve only for testing analysis
	 * 
	 */
	
	public void deleteQueue(int queueId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 7; // QUEUE_DEL_REQ
		
		req.queueId = queueId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	public void sendRandomMessage(String message, int receiverId, int queueId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 0; // MSG_SEND_REQ
		
		// Create a new message by passing the number of characters the message body should contain
		req.message = message;
	
		req.receiverId = receiverId;
		
		req.queueId = queueId;
		
		// Send request to the output stream
		sendRequest(req);
	}
	
	public void receiveMessageFromReceiver(int receiverId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 1; // MSG_REC_REQ
		
		req.receiverId = receiverId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	public void receiveMessageFromQueue(int queueId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 3; // QUEUE_READ_REQ
		
		req.queueId = queueId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	public void receiveMessageFromQueueWithDelete(int queueId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 4; // QUEUE_READ_WITH_REMOVE_REQ
		
		req.queueId = queueId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	
	public void queryForQueue(int receiverId) {
		
		// Create a new request and assign all fields
		Request req = new Request();
		
		// Default configuration of request
		configureDefaultRequest(req);
		
		// Define request type
		req.type = 5; // QUEUE_QUERY_REQ
		
		req.receiverId = receiverId;
	
		// Send request to the output stream
		sendRequest(req);
	}
	

	/**
	 * Main call
	 * @param args
	 */
	public static void main(String[] args) {
		
		/**
		 * Prepare Config class
		 */
		// Get Config instance
		config = Config.getInstance();
		
        //Read arguments        
        System.out.println("path to config file: " + args[0]);
		
		try {

			/**
			 * Read in config file
			 */
			if (!args[0].equals("${config_file}")) {
				// If a path to a config file is passed as argument read config file
				ConfigFile cf = new ConfigFile(args[0]);
				cf.readConfigFile();
				//cf.printConfigs();
				System.out.println("Config file read");
			}
			
			/*
			 * STATS: Set up logger
			 */
			statisticsLog = StatisticsLogger.setup(Client.class.getName());
			/*
			 * STATS
			 */
						
			// Host configuration
			InetAddress hostURL = InetAddress.getByName(config.hostURL);
			int hostPort = config.hostPort;
			InetAddress hostURL2 = InetAddress.getByName(config.hostURL2);
			int hostPort2 = config.hostPort2;
			InetAddress hostURL3 = InetAddress.getByName(config.hostURL3);
			int hostPort3 = config.hostPort3;
			
			for (int i = 0; i < config.totalNumberOfClients; ++i) {
				
				// STATS: 2 instances experiment
				if (config.numberOfInstances == 3) {
					
					if (i % 3 == 0) {
						
						System.out.println("Client " + i + " tries to connect to: " + hostURL.getHostAddress() + " at port: " + hostPort);
						Client client = new Client(hostURL, hostPort, i);
						
						// For each client start a new thread
						Thread t = new Thread(client);
						t.start();
						
					} else if (i % 2 == 0) {
						
						System.out.println("Client " + i + " tries to connect to: " + hostURL2.getHostAddress() + " at port: " + (hostPort2));
						Client client = new Client(hostURL2, hostPort2, i);
						
						// For each client start a new thread
						Thread t = new Thread(client);
						t.start();
						
					} else {
						
						// IMPORTANT 
						// Connect to hostPort + 1!
						
						System.out.println("Client " + i + " tries to connect to: " + hostURL3.getHostAddress() + " at port: " + (hostPort3));
						Client client = new Client(hostURL3, (hostPort3), i);
						
						// For each client start a new thread
						Thread t = new Thread(client);
						t.start();
						
					}

				} else if (config.numberOfInstances == 2) {
					
					if (i % 2 == 0) {
						
						System.out.println("Client " + i + " tries to connect to: " + hostURL.getHostAddress() + " at port: " + hostPort);
						Client client = new Client(hostURL, hostPort, i);
						
						// For each client start a new thread
						Thread t = new Thread(client);
						t.start();
						
					} else {
						
						// IMPORTANT 
						// Connect to hostPort + 1!
						
						System.out.println("Client " + i + " tries to connect to: " + hostURL2.getHostAddress() + " at port: " + (hostPort2));
						Client client = new Client(hostURL2, (hostPort2), i);
						
						// For each client start a new thread
						Thread t = new Thread(client);
						t.start();
						
					}

				} else {
				
					/**
					 * Create new client that connects to
					 * host address: host at port: port with the
					 * client id: clientId
					 */
					System.out.println("Client " + i + " tries to connect to: " + hostURL.getHostAddress() + " at port: " + hostPort);
					Client client = new Client(hostURL, hostPort, i);
					
					// For each client start a new thread
					Thread t = new Thread(client);
					t.start();
					
					// STATS: Create new client every 2 seconds until 300 clients created
					Thread.sleep(1000);
					
				}
				
			}
												
		} catch (Exception e) {
			// Exception if client could not be created
			System.out.println("Error: Client could not be created");
			e.printStackTrace();
		}
	}
}
