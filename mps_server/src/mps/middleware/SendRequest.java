package mps.middleware;

import java.nio.channels.SocketChannel;

public class SendRequest {
	
	public SocketChannel socket; // Corresponding socket channel
	public int ops; // SelectionKey options

	public SendRequest(SocketChannel socket, int ops) {
		this.socket = socket;
		this.ops = ops;
	}
	
}
