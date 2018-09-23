package mps.middleware;

import java.io.IOException;
import java.net.InetAddress;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import mps.config.Config;
import mps.config.ConfigFile;
import mps.statistics.StatisticsLogger;

/**
 * 
 * @author Andrin Jenal
 * @description In this version of NIO ConnectionManagement the selection thread handles all I/O tasks.
 * The ThreadPool maintains all RequestHandleThreads
 * For the underlying database communication a ConnectionPool is created.
 *
 */

public class MessagingSystem {
	
	private static Config config;
	
	public static void main(String[] args) {
	
		//Simple output
		System.out.println("MessagingSystem");
	
		/**
		 * Prepare Config class
		 */
		// Get Config instance
		config = Config.getInstance();
		
		//Read arguments
		assert(args.length >= 1);
		System.out.println("path to config file: " + args[0]);
				
		//Check if JDBC library is in classpath
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("JDBC Driver found");
		} catch(ClassNotFoundException e) {
			System.out.println("JDBC Driver not found!!!");
		}
				
		// Try to start server
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
			 * STATS Start the logging
			 */
			Config.statisticsLog = StatisticsLogger.setup(MessagingSystem.class.getName());
			System.out.println("Logger assigned to " + MessagingSystem.class.getName());
			/*
			 * STATS
			 */
			
			/*
			 * Initialize the Connection Pool for the server - Database communication
			 */
			final Jdbc3PoolingDataSource pooledDataSource = new Jdbc3PoolingDataSource();
			pooledDataSource.setDataSourceName("PooledDataSource");
			pooledDataSource.setServerName(config.DB_URL);
			pooledDataSource.setPortNumber(config.DB_PORT);
			pooledDataSource.setDatabaseName(config.DB_NAME);
			pooledDataSource.setUser(config.DB_USER);
			pooledDataSource.setPassword(config.DB_PASS);
			// IMPORTANT: Number of pooled connections is number of threads * number of instances
			pooledDataSource.setMaxConnections(config.maxPooledConnections);
			pooledDataSource.setInitialConnections(config.maxPooledConnections);
			
			/**
			 * Prepare and start ConnectionManagement
			 * ConnectionManagment is responsible for all client - server SocketChannel connections
			 * and passes all client request to RequestHandlerThread, which handles all client request
			 */
			InetAddress hostURL = InetAddress.getLocalHost();
			int hostPort = config.hostPort;
			
			// Initiate middleware instance
			new Thread(new ConnectionManagement(hostURL, hostPort, pooledDataSource)).start();
			
		} catch (IOException e) {
			
				e.printStackTrace();
				
		}
		
	}
}
