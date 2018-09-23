package mps.config;

import java.util.logging.Logger;

public class Config {
	
	/*
	 * IMPORTANT
	 * These are the default system configuration parameters
	 * If a config file is passed to the run .jar then the parameters in this file
	 * will be overwritten
	 * 
	 */
	// EXPERIMENT DETAILS
	public String experimentName = "EXPERIMENT";
	
	/*
	 * STATS
	 */
	public static Logger statisticsLog = null;
	
	/**
	 * DEFAULT configurations
	 */
	
	public boolean DEBUG = false;
	
	/*
	 * CLIENT
	 */
	public int totalNumberOfClients = 3;
	public int numOfRequestsPerSecond = 2;
	public int totalNumberOfRequestsToSend = 10;
	public int messageSize = 200;
	
	/*
	 * MIDDLEWARE
	 */
	public int numberOfInstances = 1;
	public int numberOfThreadsInThreadPool = 10;
	public int maxPooledConnections = 10; // Pooling configuration
	
	/*
	 * DATABASE
	 */
	public int numberOfQueues = 100;
	
	/**
	 * Define here all default configurations for the database communication
	 */
	
	// JDBC driver name and database URL
	public String JDBC_DRIVER = "org.postgresql.Driver";  
	public String DB_URL = "localhost";
	public int DB_PORT = 5432;
	public String DB_NAME = "mps_db";
	
	//  Database credentials
	public String DB_USER = "mps_admin";
	public String DB_PASS = "password";
	
	
	/**
	 * Define here all configurations for client server communication
	 */
	
	// Default host address and port
	public String hostURL = "127.0.1.1";
	public int hostPort = 17087;
	public String hostURL2 = "127.0.1.1";
	public int hostPort2 = 17088;
	public String hostURL3 = "127.0.1.1";
	public int hostPort3 = 17089;
	
	/*
	 * IMPORTANT END ALL CONFIGURATION PROPERTIES HERE
	 */
	
	
	/**
	 * Singleton definitions
	 */
	// Keep only one existing instance of Config 
	private static Config instance = null;
	
	protected Config() {
		// Exists only to defeat instantiation.
	}
	
	// Singleton constructor
	public static Config getInstance() {
		
		if (instance == null) {
			
			instance = new Config();
			
		}
		
		return instance;
	}
	
}