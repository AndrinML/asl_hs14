package mps.database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;
import org.postgresql.util.PSQLException;

import mps.config.Config;
import mps.request.Request;
import mps.request.Response;

public class DatabaseCommunication {
	
	/*
	 * STATS
	 */
	private Config config;
	
	// Time when a query is sent to the DB
	private long startTimeSQLExecute;
	
	// Time when a response from the DB arrives
	private long endTimeSQLExecute;
		
	/*
	 * Pooled data source. It is important to open/close the connection to the DB
	 * as short as possible. close() will not physically close the DB connection
	 * but rather return it to the connection pool
	 */
	private Jdbc3PoolingDataSource pooledDataSource;

	/**
	 * Important: first register then connect to the DB
	 */
	public DatabaseCommunication(Jdbc3PoolingDataSource ds) {
		
		// STATS
		config = Config.getInstance();
		
		// Assign pooled data source
		this.pooledDataSource = ds;
		
		// Default initialization to avoid errors
		this.startTimeSQLExecute = 0;
		this.endTimeSQLExecute = 0;
	}
	
	/**
	 * SQL QUERY
	 * Define all possible SQL queries here
	 */
	
	/**
	 * Handle all update queries
	 * @param sqlQuery
	 * @param req
	 * @return Response
	 */
	public Response updateQuery(String sqlQuery, Request req) {
		
		/*
		 * IMPORTANT Connection to the DB should be kept open as short as possible
		 */
		Connection dbConnection = null;
		CallableStatement cstmt = null;
		
		// Initialize new Response object
		Response rsp = new Response();
		// Pre-populate response
		rsp.errorType = 0; // NO_ERROR
		rsp.requestId = req.requestId;
		rsp.requestType = req.type;
		rsp.retrievedMessage = "";

		/*
		 * Distinguish between specific receivers and 'NULL' receiver
		 */
		
		try {
			
			// Try to get a pooled connection from the data source
			dbConnection = pooledDataSource.getConnection();
			
			// Ensure messages will be inserted concurrently
			//dbConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

			// Create new statement
			cstmt = dbConnection.prepareCall(sqlQuery);

			/*
			 * STATS
			 */
			this.startTimeSQLExecute = System.nanoTime();
			/*
			 * STATS END
			 */
			
			if (req.type == 0) {
				
				// MSG_SEND_REQ
				
				cstmt.setInt(1, req.senderId);
				
				/*
				 * Distinguish between specific receivers and 'NULL' receiver
				 */
				if (req.receiverId == -1) {
					cstmt.setNull(2, Types.INTEGER);
				} else {
					cstmt.setInt(2, req.receiverId);
				}
				
				cstmt.setInt(3, req.queueId);
				cstmt.setString(4, req.message);
				
				cstmt.registerOutParameter(5, Types.INTEGER);
	
				/**
				 * executeUpadte does not return a ResultSet
				 * sqlQuery gets executed
				 */
				cstmt.executeUpdate();
	
				rsp.messageId = cstmt.getInt(5);
				
				if (config.DEBUG) {
					System.out.println("Retrieved message id: " + rsp.messageId);
				}
			
			} else if (req.type == 1) {
								
				// MSG_REC_REQ
				cstmt.setInt(1, req.senderId);
				cstmt.setInt(2, req.receiverId);
				
				cstmt.registerOutParameter(3, Types.INTEGER);
				cstmt.registerOutParameter(4, Types.VARCHAR);
				
				cstmt.executeUpdate();
				
				rsp.messageId = cstmt.getInt(3);
				
				// Check whether record was found
				if (cstmt.wasNull()) {
					// Record was not found
					
					// Debugging purpose
					if (config.DEBUG) {
						System.out.println("RETURNED EMPTY RESULT SET");
					}
					
					// Populate response
					rsp.errorType = 2; // RECORD_NOT_FOUND_ERROR
					rsp.requestId = req.requestId;
					rsp.requestType = req.type;
					rsp.messageId = -1;
					rsp.retrievedMessage = "";
					
				} else {
					
					// Proceed with reading out parameters
					rsp.retrievedMessage = cstmt.getString(4);

					if (config.DEBUG) {
						System.out.println("Retrieved message id: " + rsp.messageId);
					}
				}
				
			} else if (req.type == 3 || req.type == 4) {
								
				// QUEUE_READ_REQ
				// or
				// QUEUE_READ_WITH_REMOVE_REQ
				
				cstmt.setInt(1, req.senderId);
				cstmt.setInt(2, req.queueId);
				
				cstmt.registerOutParameter(3, Types.INTEGER);
				cstmt.registerOutParameter(4, Types.VARCHAR);
				
				cstmt.executeUpdate();
				
				// Get message id
				rsp.messageId = cstmt.getInt(3);
				
				// Check whether record was found
				if (cstmt.wasNull()) {
					// Record was not found
					
					// Debugging purpose
					if (config.DEBUG) {
						System.out.println("RETURNED EMPTY RESULT SET");
					}
					
					// Populate response
					rsp.errorType = 2; // RECORD_NOT_FOUND_ERROR
					rsp.requestId = req.requestId;
					rsp.requestType = req.type;
					rsp.messageId = -1;
					rsp.retrievedMessage = "";
					
				} else {
					
					// Proceed with reading out parameters
					rsp.retrievedMessage = cstmt.getString(4);

					if (config.DEBUG) {
						System.out.println("Retrieved message id: " + rsp.messageId);
					}
				}
				
			} else if (req.type == 5) {
				
				//QUEUE_READ_WITH_REMOVE_REQ
				cstmt.setInt(1, req.receiverId);
				
				cstmt.registerOutParameter(2, Types.INTEGER);
				
				cstmt.executeUpdate();
				
				// Get message id
				rsp.retrievedMessage = "Queue where messages are waiting: " + cstmt.getInt(2);
				rsp.messageId = -1;
				
				// Check whether record was found
				if (cstmt.wasNull()) {
					// Record was not found
					
					// Debugging purpose
					if (config.DEBUG) {
						System.out.println("RETURNED EMPTY RESULT SET");
					}
					
					// Populate response
					rsp.errorType = 2; // RECORD_NOT_FOUND_ERROR
					rsp.requestId = req.requestId;
					rsp.requestType = req.type;
					rsp.messageId = -1;
					rsp.retrievedMessage = "";
					
				}
				
			} else if (req.type == 6) {
				
				// QUEUE_CREATE_REQUEST
				
				cstmt.setInt(1, req.queueId);
				
				// If query is a normal update query
				cstmt.executeUpdate();
				
			} else if (req.type == 7) {
				
				// QUEUE_DEL_REQUEST
								
				cstmt.setInt(1, req.queueId);
				
				// If query is a normal update query
				cstmt.executeUpdate();
				
			} else if (req.type == 2) {
								
				// MESSAGE_DELETE_REQUEST
				// This queueId corresponds to the message id
				// IMPORTANT: Evil hack!
				cstmt.setInt(1, req.queueId);
				
				cstmt.executeUpdate();
			}

		} catch (PSQLException e) {
			
			//e.printStackTrace();
			
			// PSQL error occurred
			rsp.errorType = 3;
			rsp.requestId = req.requestId;
			rsp.requestType = req.type;
			rsp.messageId = -1;
			rsp.retrievedMessage = "";
			
			// STATS
			this.endTimeSQLExecute = System.nanoTime();
			
			double elapsedSqlExecutionTime = (this.endTimeSQLExecute - this.startTimeSQLExecute) / 1000000.0; // In milliseconds
			
			// Round to two decimals
			BigDecimal bd = new BigDecimal(elapsedSqlExecutionTime).setScale(2, RoundingMode.HALF_UP);

			rsp.sqlExecutionTime = bd.doubleValue();
			// STATS END
			
			// Return Response object
			return rsp;
			
		} catch (SQLException e) {
			/**
			 * Catch exception if query could not be executed
			 * If this happens thread should terminate and not
			 * try to further handle the request
			 */
			e.printStackTrace();
			
			// SQL error occurred
			rsp.errorType = 1;
			rsp.requestId = req.requestId;
			rsp.requestType = req.type;
			rsp.messageId = -1;
			rsp.retrievedMessage = "";
			
			// STATS
			this.endTimeSQLExecute = System.nanoTime();
			
			double elapsedSqlExecutionTime = (this.endTimeSQLExecute - this.startTimeSQLExecute) / 1000000.0; // In milliseconds
			
			// Round to two decimals
			rsp.sqlExecutionTime = new BigDecimal(elapsedSqlExecutionTime).setScale(2, RoundingMode.HALF_UP).doubleValue();
			// STATS END
			
			// Return Response object
			return rsp;
			
		} finally {
			
			// IMPORTANT This gets executed always
			
			try {
				
				// After executing statement close everything
				cstmt.close();
				dbConnection.close();
				
			} catch (SQLException e) {
				
				// TODO What happens with the error here?
				e.printStackTrace();
			}
			
		}
		
		/*
		 * STATS
		 */
		this.endTimeSQLExecute = System.nanoTime();	
		
		double elapsedSqlExecutionTime = (this.endTimeSQLExecute - this.startTimeSQLExecute) / 1000000.0; // In milliseconds
		
		// Round to two decimals
		rsp.sqlExecutionTime = new BigDecimal(elapsedSqlExecutionTime).setScale(2, RoundingMode.HALF_UP).doubleValue();
		
		// Return Response object
		return rsp;
	}
	
	public Response insertQueueById(Request req) {
		
		String sqlQuery = "{call insert_queue(?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}
		
		Response rsp = updateQuery(sqlQuery, req);
		
		return rsp;
		
	}
	
	public Response deleteQueueById(Request req) {
		
		String sqlQuery = "{call delete_queue(?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}		
		Response rsp = updateQuery(sqlQuery, req);
		
		return rsp;
	}
	
	public Response insertMessage(Request req) {
		
		String sqlQuery = "{call insert_message(?,?,?,?,?)}";
				
		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}		
		Response rsp = updateQuery(sqlQuery, req);
		
		// Return Response object
		return rsp;
	}
	
	public Response retrieveMessageByReceiverId(Request req) {
		
		String sqlQuery = "{call get_message_from_receiver(?,?,?,?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}
		
		Response rsp = updateQuery(sqlQuery, req);
		
		return rsp;
		
	}

	public Response retrieveMessageFromQueueById(Request req) {
		
		String sqlQuery = "{call get_message_from_queue(?,?,?,?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}
		
		Response rsp = updateQuery(sqlQuery, req);
		
		return rsp;
		
	}
	
	public Response retrieveMessageFromQueueByIdWithDelete(Request req) {
		
		String sqlQuery = "{call get_message_from_queue(?,?,?,?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}
		
		Response rsp = updateQuery(sqlQuery, req);
		
		// If successfully retrieved message, try to delete it		
		if (rsp.messageId >= 0) {
			
			sqlQuery = "{call delete_message(?)}";
			
			if (config.DEBUG) {
				System.out.println(sqlQuery);
			}
			
			req.type = 2;
			req.queueId = rsp.messageId;
			
			updateQuery(sqlQuery, req);

		}
		
		return rsp;
		
	}

	public Response queryForQueueByReceiverId(Request req) {
		
		String sqlQuery = "{call get_queue_from_receiver(?,?)}";

		if (config.DEBUG) {
			System.out.println(sqlQuery);
		}		
		Response rsp = updateQuery(sqlQuery, req);
		
		return rsp;
	}
	
}
