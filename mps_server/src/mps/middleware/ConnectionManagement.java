package mps.middleware;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import mps.config.Config;

/**
 * 
 * @author Andrin Jenal
 * @description In this version of NIO ConnectionManagement the selection thread handles all I/O tasks.
 * The ThreadPool maintains all RequestHandleThreads
 * For the underlying database communication a ConnectionPool is created.
 *
 */

public class ConnectionManagement implements Runnable {
	
	/*
	 * STATS
	 */
	boolean increaseNumberOfThreadsExperiment = false;

	private int numberOfRequestsInSystem;
	
	private long timer;
			
	private List<ExecutorService> executors;
	
	private int numberOfThreadsInPool;
	
	private long timingForThreads;
	
	private int threadPoolIdx;
	
	/*
	 * STATS END
	 */
	
	// Assign global singleton Config class
	private static Config config;
	
	// host:port to listen on
	private InetAddress hostAddress;
	private int portNumber;
	
	// Channel on which clients can connect
	private ServerSocketChannel serverChannel;

	// The selector that is monitored
	private Selector selector;
	
	// ThreadPool with waiting threads to process incoming requests from clients
	private ExecutorService executor;
	
	// A list of SendRequest instances
	private List<SendRequest> sendRequests = new LinkedList<SendRequest>();
	
	// Maps a SocketChannel to a list of ByteBuffer instances
	private ConcurrentHashMap<SocketChannel, List<ByteBuffer>> pendingData = new ConcurrentHashMap<SocketChannel, List<ByteBuffer>>();
	
	// Pooled data source for DB connections
	private Jdbc3PoolingDataSource pooledDataSource;
	
	/**
	 * ConnectionManagement constructor
	 * @param hostAddress
	 * @param portNumber
	 * @throws IOException
	 */
	public ConnectionManagement(InetAddress hostAddress, int portNumber, Jdbc3PoolingDataSource ds) throws IOException {
		
		// Load config class
		config = Config.getInstance();
		
		// STATS		
		this.numberOfThreadsInPool = config.numberOfThreadsInThreadPool;
		
		this.numberOfRequestsInSystem = 0;
				
		// Start measuring for increasing threadpool size
		
		// STATS EXPERIMENT		
		if (increaseNumberOfThreadsExperiment) {
			
			this.executors = new ArrayList<ExecutorService>();
			
			// Prepare for initialization phase
			this.executors.add(Executors.newFixedThreadPool(1));
			this.executors.add(Executors.newFixedThreadPool(2));
			this.executors.add(Executors.newFixedThreadPool(4));
	
			
			// Create "dynamically" increasing list of fixed thread pool executors
			for (int i = 1; i <= (this.numberOfThreadsInPool / 5); ++i) {
				
				executors.add(Executors.newFixedThreadPool(i * 5));
			}
			
			this.threadPoolIdx = 0;
	
			this.timingForThreads = System.nanoTime();
		}
		// STATS END
		
		// Middleware host location with port
	    this.hostAddress = hostAddress;
	    this.portNumber = portNumber;
	    
	    // Initialize nio.selector. The selector is responsible for handling all arising events
	    this.selector = this.initSelector();	
	    
	    // Initialize fixed thread pool
	    /*
	     * Creates a thread pool that reuses a fixed number of threads operating off a shared unbounded queue. 
	     * At any point, at most nThreads threads will be active processing tasks. If additional tasks are submitted 
	     * when all threads are active, they will wait in the queue until a thread is available. 
	     * If any thread terminates due to a failure during execution prior to shutdown, a new one will take its 
	     * place if needed to execute subsequent tasks.
	     * The threads in the pool will exist until it is explicitly shutdown.
	     */
	    executor = Executors.newFixedThreadPool(config.numberOfThreadsInThreadPool);
	    
	    // Assign DB connection data source
	    this.pooledDataSource = ds;
	}
	
	/**
	 * This method gets called by the RequestHandlerThread instances
	 * as soon as they want data that should be written to the SocketChannel
	 * to one of the clients
	 * @param socket
	 * @param data
	 */
	public void send(SocketChannel socket, byte[] data) {
		
		/*
		 * Make synchronized call, since a lot of RequestHandlerThread
		 * instances can access sendRequests concurrently
		 */
		synchronized(this.sendRequests) {
			
			// Indicate we want the interest ops set for a specific SocketChannel changed
			this.sendRequests.add(new SendRequest(socket, SelectionKey.OP_WRITE));
	
			// TODO pendingData is concurrent hash map. Is synchronized still necessary?
			// Queue the data that should be written back
			synchronized (this.pendingData) {
				
				// Get queue corresponding to the SocketChannel
				List<ByteBuffer> queue = (List<ByteBuffer>) this.pendingData.get(socket);
				
				if (queue == null) {
					queue = new ArrayList<ByteBuffer>();
					this.pendingData.put(socket, queue);
				}
				
				queue.add(ByteBuffer.wrap(data));
			}
		}

		// Finally, wake up the selecting thread so it can make the required changes
		this.selector.wakeup();

	}

	// Initialize a selector, which is responsible for handling all open connections
	private Selector initSelector() throws IOException {
		
		// Create a new selector provider
		Selector socketSelector = SelectorProvider.provider().openSelector();
		
		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);
		
		// Bind the server socket to the specified address and port
		InetSocketAddress isa = new InetSocketAddress(this.hostAddress, this.portNumber);
		serverChannel.socket().bind(isa);
		
		// Register the server socket channel, indicating an interest in accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
		
		return socketSelector;
	}
	
	public void run() {
		
		System.out.println("ConnectionManagement starts listening at: " + this.hostAddress + " , port: " + this.portNumber);
		
		while (true) {
			
			try {
				
				// Process any pending send requests
				synchronized (this.sendRequests) {
					
					// Create iterator to iterate over requests
					Iterator<SendRequest> requests = this.sendRequests.iterator();
					
					// If any data available, process data
					while (requests.hasNext()) {
						
						// Select next request
						SendRequest request = (SendRequest) requests.next();
						
						// Get the corresponding key to the SocketChannel
						SelectionKey key = request.socket.keyFor(this.selector);
						
						// Change interesting ops to write (if sending request)
						key.interestOps(request.ops);
					}
					
					// Clear all sending requests
					this.sendRequests.clear();
				}
				
				// Wait for an event one of the registered channels
				this.selector.select();
				
				Iterator<SelectionKey> selectedKeys = this.selector.selectedKeys().iterator();
				
				// Iterate over the set of keys for which events are available
				while (selectedKeys.hasNext()) {
					
					SelectionKey key = (SelectionKey) selectedKeys.next();
					
					// After processing channel remove SlectionKey instance
					selectedKeys.remove();
					
					if (!key.isValid()) {
						continue;
					}
		
					// Check what event is available and deal with it
					if (key.isAcceptable()) {
						
						this.accept(key);
					
					} else if (key.isReadable()) {
						
						this.read(key);
						
					} else if (key.isWritable()) {
						
						this.write(key);
						
					}
					
					// STATS
					/*if (((System.nanoTime() - this.timer) / 1000000000) >= 5) { // IMPORTANT Update every 10 seconds
						
						System.out.println(System.nanoTime()/1000000000 + ":rqsts=" + this.numberOfRequestsInSystem);
						
						this.timer = System.nanoTime();
					}*/
					// STATS END
					
				}
				
				// Since selector thread is spinning. Time to write something to the log
				// TODO STATS
				
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
		}
	}
	
	
	// If accept event is available register to SocketChannel
	private void accept(SelectionKey key) throws IOException {
		
		// For an accept to be pending the channel must be a server socket channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		
		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();

		socketChannel.configureBlocking(false);
		
		// Register new SocketChannel with the Selector. Notify when data is waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ);
		
		if (config.DEBUG) {
			System.out.println("Middleware accepted client");
		}
	}
	
	/**
	 *  If a read event is available read from the SocketChannel to the ByteBuffer
	 * @param key
	 * @throws IOException
	 */
	private void read(SelectionKey key) throws IOException {
		
		// STATS
		this.timer = System.nanoTime();
		// STATS END
		
		/**
		 * Buffer in which data will be read from SocketChannel
		 * Set size to 8192 bytes. Should be large enough to contain all data
		 * of the Request object
		 */		
		ByteBuffer readBuffer = ByteBuffer.allocate(8192);
		
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		// Clear out the readBuffer to make it ready for the new data
		readBuffer.clear();
				
		// Attempt to read off the SocketChannel
		int numRead;
		
		try {
			
			// numRead indicates whether whole channel was read
			numRead = socketChannel.read(readBuffer);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			// If the client closed the connection, cancel
			// the selection key and close the channel.
			key.cancel();
			socketChannel.close();
			return;
			
		}
		
		if (numRead == -1) {
			
			// The client shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			key.channel().close();
			key.cancel();
			return;
			
		}
		
		// STATS
		this.numberOfRequestsInSystem += 1;		
		double elapsedProcessingTime = (System.nanoTime() - timer) / 1000000.0; // In milliseconds
		elapsedProcessingTime = new BigDecimal(elapsedProcessingTime).setScale(2, RoundingMode.HALF_UP).doubleValue(); // Round
		// STATS END

		// Hand the data off to a request handler from the thread pool. The ThreadPool has a inifinite long queue
		// Initialize requestHandler to make it ready to process the data from the channel
		RequestHandlerThread requestHandler = new RequestHandlerThread(this, socketChannel, readBuffer.array(), this.pooledDataSource, elapsedProcessingTime, System.nanoTime(), numberOfRequestsInSystem);
		
		this.executor.execute(requestHandler);

		/*
		 * ExecutorService provides an internal BlockingQueue which handles the
		 * tasks which are passed to the executor.
		 */
		// STATS
		// Increase size of threadpool
		if (increaseNumberOfThreadsExperiment) {
			
			if (((System.nanoTime() - this.timingForThreads) / 1000000000) >= 20) { // IMPORTANT Update every 10 seconds
			
			this.threadPoolIdx = this.threadPoolIdx + 1;
			
			this.timingForThreads = System.nanoTime();
			
			//System.out.println(System.nanoTime()/1000000000 + " New index to pool size: " + this.threadPoolIdx);
				
			}
			
			this.executors.get(this.threadPoolIdx).execute(requestHandler);
		}
		// STATS END
	}
	
	/**
	 * Write event, writes data from the pendingData hash map to 
	 * the corresponding SocketChannel
	 * @param key
	 * @throws IOException
	 */
	private void write(SelectionKey key) throws IOException {
		
		// STATS
		//timeProcessingRequestStart = System.nanoTime();
		// STATS END
		
		SocketChannel socketChannel = (SocketChannel) key.channel();

		// TODO: ConcurrentHashMap -> is "synchronized()" still necessary?
		synchronized (this.pendingData) {
			
			List<ByteBuffer> queue = (List<ByteBuffer>) this.pendingData.get(socketChannel);
	
			// Data is written to corresponding SocketChannel until no data left
			while (!queue.isEmpty()) {
				
				// Get ByteBuffers from queue (List)
				ByteBuffer buf = (ByteBuffer) queue.get(0);
				
				// Write buffers to the SocketChannel
				socketChannel.write(buf);
				
				if (buf.remaining() > 0) {
					// If SocketChannel buffer is full break
					break;
				}
				
				// After the buffer is written, remove it from the queue
				queue.remove(0);
				
			}
		
			if (queue.isEmpty()) {
				
				// All data is written. Selector should now be switched back to wait for data
				key.interestOps(SelectionKey.OP_READ);
			}
		}
		
		// STATS
		
		this.numberOfRequestsInSystem -= 1;

		/*
		numberOfResponsesSent += 1;
		
		timeProcessingRequestEnd = System.nanoTime();
		
		double elapsedTimeProcessing = (timeProcessingRequestEnd - timeProcessingRequestStart) / 1000000.0; // In ms
		*/
		// STATS END
	}
	
}
