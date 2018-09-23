package mps.config;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigFile {
	
	private String filePath;
	
	private Config config;
	
	public ConfigFile(String path) throws IOException {
		
		this.filePath = path;
		this.config = Config.getInstance();
	}
	
	public void readConfigFile() throws IOException {
		
		Properties prop = new Properties();
 
		//InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		FileReader inputStream = new FileReader(this.filePath);
		prop.load(inputStream);
  
		config.experimentName = prop.getProperty("experimentName");
		
		config.DEBUG = Boolean.parseBoolean(prop.getProperty("debug"));
		
		config.hostURL = prop.getProperty("hostURL");
		config.hostPort = Integer.parseInt(prop.getProperty("hostPort"));
		config.hostURL2 = prop.getProperty("hostURL2");
		config.hostPort2 = Integer.parseInt(prop.getProperty("hostPort2"));
		config.hostURL3 = prop.getProperty("hostURL3");
		config.hostPort3 = Integer.parseInt(prop.getProperty("hostPort3"));
		
		config.DB_URL = prop.getProperty("DB_URL");
		config.DB_PORT = Integer.parseInt(prop.getProperty("DB_PORT"));
		
		config.totalNumberOfClients = Integer.parseInt(prop.getProperty("numberOfClients"));
		config.totalNumberOfRequestsToSend = Integer.parseInt(prop.getProperty("totalNumberOfRequests"));
		config.numOfRequestsPerSecond = Integer.parseInt(prop.getProperty("requestsPerSecond"));
		config.messageSize = Integer.parseInt(prop.getProperty("messageSize"));
		
		config.numberOfInstances = Integer.parseInt(prop.getProperty("numberOfInstances"));
		config.numberOfThreadsInThreadPool = Integer.parseInt(prop.getProperty("numberOfThreadsInPool"));
		config.maxPooledConnections = Integer.parseInt(prop.getProperty("numberOfConnectionsToDb"));
		
	}
	
	public void printConfigs() {
		
		System.out.println(config.experimentName);
		System.out.println(config.DEBUG);
		System.out.println(config.totalNumberOfClients);
		System.out.println(config.numOfRequestsPerSecond);
		System.out.println(config.totalNumberOfRequestsToSend);
		System.out.println(config.messageSize);
		System.out.println(config.numberOfInstances);
		System.out.println(config.numberOfThreadsInThreadPool);
		System.out.println(config.maxPooledConnections);
		System.out.println(config.numberOfQueues);
		System.out.println(config.JDBC_DRIVER);
		System.out.println(config.DB_URL);
		System.out.println(config.DB_PORT);
		System.out.println(config.DB_NAME);
		System.out.println(config.DB_USER);
		System.out.println(config.DB_PASS);
		System.out.println(config.hostURL);
		System.out.println(config.hostPort);
		
	}
}
