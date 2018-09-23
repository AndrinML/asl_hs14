package mps.middleware;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.channels.SocketChannel;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import mps.config.Config;
import mps.database.DatabaseCommunication;
import mps.request.Request;
import mps.request.Response;
import mps.statistics.StatisticsFormatter;

public class RequestHandlerThread implements Runnable {
	
	/*
	 * STATS
	 */
	//Assign global singleton Config class
	private static Config config;
		
	// Time when data is started getting processed
	private long timer;
			
	private double processingTimeOfSelector;
	
	//private int numberOfRequestsInTheSystem;
	
	private long waitingTimeStart;
	private double elapsedWaitingTime;
	/*
	 * STATS END
	 */
	
	// Remember the server instance to write data back to the SocketChannel
	private ConnectionManagement server;
	
	// Remember the SocketChannel from which the data came from
	private SocketChannel socketChannel;
	
	// Store the byte[] from the client
	private byte[] clientData;
	
	// Desired request from the client
	private Request clientRequest;
	
	// Pooled data source for DB connections
	private Jdbc3PoolingDataSource pooledDataSource;
	
	/**
	 * Prepare RequestHandlerThread to process the client request
	 * @param server
	 * @param socketChannel
	 * @param clientData
	 * @param pTime - Processing time of selector thread
	 * @param numOfRqsts - number of requests in system
	 */
	public RequestHandlerThread(ConnectionManagement server, SocketChannel socketChannel, byte[] clientData, Jdbc3PoolingDataSource ds, double selectorProcessingTime, long waitingTimeStart, int numOfRqsts) {
		
		// Assign global singleton Config class
		config = Config.getInstance();
		
		this.server = server;
		this.socketChannel = socketChannel;
		this.clientData = clientData;
		
		this.processingTimeOfSelector = selectorProcessingTime;
		this.waitingTimeStart = waitingTimeStart;
		//this.numberOfRequestsInTheSystem = numOfRqsts;
		
		// Assign pooled data source
		this.pooledDataSource = ds;
		
		if (config.DEBUG) {
			System.out.println("RequestHandler: " + Thread.currentThread().getId() + " initialized");
		}
	}

	public void run() {
		
		// Complex logic to handle requests
		try {
			
			// STATS: Elapse time for processing data
			double elapsedWaitingTime = (System.nanoTime() - this.waitingTimeStart) / 1000000.0;
			this.elapsedWaitingTime = new BigDecimal(elapsedWaitingTime).setScale(2, RoundingMode.HALF_UP).doubleValue();
			this.timer = System.nanoTime(); // Start stopping for processing time
			// STATS END
			
			// Try to process byte[] clientData to Request object
			processClientData();
			
			// If successfully deserialized the Request object process the client Request
			processClientRequest();
			
		} catch (IOException e) {
			// Exception failed to process the byte[]
			e.printStackTrace();
		
		}
	}
	
	private void processClientRequest() throws IOException {
		
		// If byte stream successfully deserialized open database connection to perform request
		DatabaseCommunication dbCommunication = new DatabaseCommunication(this.pooledDataSource);
			
		// In any case a response is sent back to the client
		Response rsp;
		/*
		 * Request.type: MSG_SEND_REQ = 0, MSG_REC_REQ = 1, MSG_QUERY_REQ = 2, QUEUE_READ_REQ = 3, 
		 * QUEUE_READ_WITH_REMOVE_REQ = 4, QUEUE_QUERY_REQ = 5, QUEUE_CREATE_REQ = 6, QUEUE_DEL_REQ = 7
		 */
		switch (this.clientRequest.type) {
			
			case 0:
				
				/*
				 * MSG_SEND_REQ:
				 * Insert message into the DB
				 */
				rsp = dbCommunication.insertMessage(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
								
				break;
		 		
			case 1:
				
				/*
				 * MSG_REC_REQ:
				 * Retrieve a message by receiver id from the DB
				 */
				rsp = dbCommunication.retrieveMessageByReceiverId(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
								
				break;
		
			// IMPORTANT case 2: not used so far
				
			case 3:
				
				/*
				 * QUEUE_READ_REQ:
				 * Retrieve a message from a queue from the DB
				 */
				rsp = dbCommunication.retrieveMessageFromQueueById(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
								
				break;
				
			case 4:
				
				/*
				 * QUEUE_READ_WITH_REMOVE_REQ:
				 * Retrieve a message from a queue from the DB
				 */
				rsp = dbCommunication.retrieveMessageFromQueueByIdWithDelete(this.clientRequest);
								
				// Send response back to client
				sendResponse(rsp);
								
				break;
				
			case 5:				
				
				/*
				 * QUEUE_QUERY_REQ:
				 * Query for a queue where messages are waiting for a particular client
				 */
				rsp = dbCommunication.queryForQueueByReceiverId(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
				
				break;
				
			case 6:
				
				/*
				 * QUEUE_CREATE_REQ:
				 * Insert queue by id into the DB
				 */
				rsp = dbCommunication.insertQueueById(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
				
				break;
				
			case 7:
				
				/*
				 * QUEUE_DEL_REQ:
				 * Insert queue by id into the DB
				 */
				rsp = dbCommunication.deleteQueueById(this.clientRequest);
				
				// Send response back to client
				sendResponse(rsp);
								
				break;
			
			default:
				
				System.out.println("Error: Cannot handle Request with undefined request type");
				
		}
		
	}

	/**
	 * Byte array is transformed to primitive types which are 
	 * then used to create a Request object
	 * @throws IOException
	 */
	private void processClientData() throws IOException {

		// Process byte[] read from the SocketChannel with a ByteArrayInputStream
		ByteArrayInputStream bais = new ByteArrayInputStream(clientData);
		
		// DataInputStream is used to retrieve primitive types from the byte[]
		DataInputStream dis = new DataInputStream(bais);
		
		// Process DataInputStream and retrieve all Request attributes
		int requestId = dis.readInt();
		int type = dis.readInt();
		int senderId = dis.readInt();
		int receiverId = dis.readInt();
		int queueId = dis.readInt();
		String message = null;
		
		// If request type is SEND_MSG_REQ then retrieve the message string as well
		if (type == 0) {
			message = dis.readUTF();
		}
		
		// Create new Request object with all assigned fields
		this.clientRequest = new Request();
		this.clientRequest.requestId = requestId;
		this.clientRequest.type = type;
		this.clientRequest.senderId = senderId;
		this.clientRequest.receiverId = receiverId;
		this.clientRequest.queueId = queueId;
		
		if (message != null) {
			this.clientRequest.message = message;
		}
		
		if (config.DEBUG) {
			System.out.println("RequestHandler: " + Thread.currentThread().getId() + " received request from client: " + this.clientRequest.senderId + " : type:" + this.clientRequest.type + " request id: " + this.clientRequest.requestId + " request msg: " + this.clientRequest.message);
		}
	}
	
	/**
	 * Send a response back to the client in any case
	 * Client is responsible to handle the error messages
	 * @param response
	 * @throws IOException
	 */
	public void sendResponse(Response response) throws IOException {
		
		// Prepare ByteArrayOutputStream to write data to the ByteBuffer
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		// DataInputStream is used to write primitive types from to the buffer
		DataOutputStream dos = new DataOutputStream(baos);
		
		// Process DataInputStream and write all Response attributes
		dos.writeInt(response.errorType);
		dos.writeInt(response.requestId);
		dos.writeInt(response.requestType);
		dos.writeInt(response.messageId);
		dos.writeUTF(response.retrievedMessage);
		
		// Flush written data to the stream
		dos.flush();
		
		// Get byte[] from ByteArrayOutputStream
		byte[] data = baos.toByteArray();	
		
		/*
		 *  Send data back to the selector thread which
		 *  writes data to the corresponding SocketChannel
		 */
		this.server.send(socketChannel, data);
		
		
		// STATS: Elapsed time to process data		
		double elapsedProcessingTime = (System.nanoTime() - this.timer) / 1000000.0; // In milliseconds
		
		elapsedProcessingTime = elapsedProcessingTime - response.sqlExecutionTime;
		
		// Round to two decimals
		elapsedProcessingTime = new BigDecimal(elapsedProcessingTime).setScale(2, RoundingMode.HALF_UP).doubleValue();
		
		// Write to the log
		Config.statisticsLog.info(StatisticsFormatter.formatLogRecord(this.clientRequest.senderId, this.clientRequest.requestId, elapsedProcessingTime, response.sqlExecutionTime, this.processingTimeOfSelector, this.elapsedWaitingTime));
		// STATS END		
	}
	
}
