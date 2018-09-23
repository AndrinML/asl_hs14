package mps.request;

public class Response {

	/*
	 * Type of possible errors: NO_ERROR = 0, SQL_EXECUTION_ERROR = 1, RECORD_NOT_FOUND_ERROR = 2, PSQL_EXCEPTION_ERROR = 3
	 */
	public int errorType = 0;
	
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
	public int requestType;
	
	// Uniquely defined (Auto-increment in DB) message id
	public int messageId;
	
	// Message body
	public String retrievedMessage;
}
