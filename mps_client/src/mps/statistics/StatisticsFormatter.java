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
	public static String formatLogRecord(int clientId, int requestId, double elapsedTime) {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("c_id="); // Client id
		sb.append(clientId);
		sb.append(" ");
		sb.append("r_id=");
		sb.append(requestId);
		sb.append(" ");
		sb.append("rtt=");
		sb.append(elapsedTime);

		return sb.toString();
	}

	
	public String format(LogRecord record) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(System.currentTimeMillis() / 1000); // Time in seconds
		sb.append(" ");
		sb.append(record.getMessage());
		sb.append("\n");

        return sb.toString();
	}

}
