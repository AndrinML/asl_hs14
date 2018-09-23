package mps.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;

import mps.config.Config;
import mps.request.Response;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author andrin
 * 
 * 
 * IMPORTANT 
 *
 * All tests base on the assumption that there are NO messages in the DB
 * and 100 queues with id's from 0 to 99 already exists
 * 
 * These JUnit tests are performed to guarantee correctness of the
 * basic requests executed atomically
 *
 */

public class ClientTest {
	
	public static Client testClient;
	
	@BeforeClass
	public static void getConnectedClient() {
		
		Config config = Config.getInstance();

		try {
			
			InetAddress host = InetAddress.getByName(config.hostURL);
			int port = config.hostPort;
			
			// Test client with id 99
			testClient = new Client(host, port, 99);
						
		}  catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testConnection() {
				
		assertNotEquals(null, testClient);
	}
	
	@Test
	public void testCreateQueueAndDeleteQueue() {
		
		testClient.createQueue(999);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
		
		testClient.deleteQueue(999);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
	}

	@Test
	public void testSendAndReceiveMessageFromReceiver() {
		
		// Send msg: "Hello client 99" with receiverId 99 and queueId 5
		testClient.sendRandomMessage("Hello client 99", 99, 10);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
		
		testClient.receiveMessageFromReceiver(99);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals("Hello client 99", rsp.retrievedMessage);
	}

	@Test
	public void testSendAndReceiveMessageFromQueue() {

		// Send msg: "Hello client 99" with receiverId 99 and queueId 5
		testClient.sendRandomMessage("Hello from queue 27", 99, 27);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
		
		testClient.receiveMessageFromQueue(27);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals("Hello from queue 27", rsp.retrievedMessage);
	}
	
	@Test
	public void testSendMessageAndQueryForQueue() {
		// Send msg: "Hello client 99" with receiverId 99 and queueId 5
		testClient.sendRandomMessage("Just a message", 99, 9);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
		
		testClient.queryForQueue(99);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals("Queue where messages are waiting: 9", rsp.retrievedMessage);
	}
	
	@Test
	public void testReceiveMessageFromQueueWithDelete() {
		
		// Send msg: "Hello client 99" with receiverId 99 and queueId 5
		testClient.sendRandomMessage("Delete me", 99, 18);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals(0, rsp.errorType);
		
		testClient.receiveMessageFromQueueWithDelete(18);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Check that no error message occurred
		assertEquals("Delete me", rsp.retrievedMessage);
	}
	
	@Test
	public void testErrorHandling() {
		
		// Send msg: "Hello client 99" with receiverId 99 and queueId 5
		testClient.sendRandomMessage("Delete me", 99, 189);
		
		Response rsp = null;
		
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// PSQL Error foreign key constraint
		assertEquals(3, rsp.errorType);
		
		testClient.receiveMessageFromQueueWithDelete(182);
				
		try {
			
			rsp = testClient.read();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Record not found error
		assertEquals(2, rsp.errorType);
	}

}
