package mps.statistics;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import mps.config.Config;

public class StatisticsLogger {
	
	static public Logger statisticsLog;
		
	static public Logger setup(String className) throws SecurityException, IOException {
		
		Config config = Config.getInstance();
		
		// Assign logger
		statisticsLog = Logger.getLogger(className);
	    
		// Create date string to uniquely name the logging file
		String dateString = new SimpleDateFormat("MMdd_HHmm").format(new Date());
		
		FileHandler logHandler = new FileHandler("logs/" + config.experimentName + "_" + className + "_statistics" + dateString + ".log");  

		statisticsLog.setLevel(Level.INFO);
        // This block configure the logger with handler and formatter  
        statisticsLog.addHandler(logHandler);
        StatisticsFormatter formatter = new StatisticsFormatter();  
        logHandler.setFormatter(formatter);  
        
        statisticsLog.setUseParentHandlers(false);

        return statisticsLog;
	}
}
