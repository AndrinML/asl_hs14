package mps.statistics;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class StatisticsFormatter extends Formatter {

	/**
	 * IMPORTANT Only successful request will be logged (This means such
	 * with database interaction)
	 * @param clientId
	 * @param requestId
	 * @param elapsedTime
	 */
	// Format a log record
	public static String formatLogRecord(int clientId, int requestId, double processingTime, double sqlTime, double preProcessingTime, double waitingTime) {
		
		StringBuilder sb = new StringBuilder();
		
		// FORMAT: client id, request id, service time selector, waiting time request handler, service time request handler, service + waiting time SQL
		
		sb.append("c_id="); // Client id
		sb.append(clientId);
		sb.append(" ");
		sb.append("r_id=");
		sb.append(requestId);
		sb.append(" ");
		sb.append(preProcessingTime);
		sb.append(" ");
		sb.append(waitingTime);
		sb.append(" ");
		sb.append(processingTime);
		sb.append(" ");
		sb.append(sqlTime);

		return sb.toString();
	}

	
	public String format(LogRecord record) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(System.currentTimeMillis() / 1000);
		sb.append(" ");
		sb.append(record.getMessage());
		sb.append("\n");

        return sb.toString();
	}

}
