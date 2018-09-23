package mps.request;

public class Request {

	/*
	 * IMPORTANT
	 * Send request id to uniquely identify requests and their
	 * corresponding response from the server
	 */
	public int requestId;
	
	/*
	 * Type of possible requests: MSG_SEND_REQ = 0, MSG_REC_REQ = 1, MSG_QUERY_REQ = 2, QUEUE_READ_REQ = 3, 
	 * QUEUE_READ_WITH_REMOVE_REQ = 4, QUEUE_QUERY_REQ = 5, QUEUE_CREATE_REQ = 6, QUEUE_DEL_REQ = 7
	 */
	public int type;
	
	// Uniquely defined client (sender) id
	public int senderId;
	
	// Uniquely defined client (receiver) id
	public int receiverId;
	
	// Uniquely defined queue id if a queue is necessary for the specific request type
	public int queueId;
	
	// Message body
	public String message;
}