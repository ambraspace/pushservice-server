package com.ambraspace.pushservice.server;

import java.nio.channels.SocketChannel;
import java.util.Date;

public class ConnectedClient {

	private final String clientUID;
	private final SocketChannel connection;
	private Date nextPingTime;
	
	public ConnectedClient(String uid, SocketChannel socketChannel, Date pingTime) {
		this.clientUID = uid;
		this.connection = socketChannel;
		this.nextPingTime = pingTime;
	}
	
	public String getClientUID() {
		return clientUID;
	}

	public SocketChannel getConnection() {
		return connection;
	}

	public Date getNextPingTime() {
		return nextPingTime;
	}

	
	/**
	 * Sets the time when this client should be PINGed (so it stays alive).
	 * NOTICE: This method should only be called when the client is detached
	 * from ConnectionManager.PingTimer's queue (SortedSet).
	 * @param nextPingTime time when this client should be PINGed
	 */
	void setNextPingTime(Date nextPingTime) {
		this.nextPingTime = nextPingTime;
	}
	

	@Override
	public boolean equals(Object obj) {
		if (!getClass().equals(obj.getClass())) {
			return false;
		}
		ConnectedClient c = (ConnectedClient) obj;
		return (clientUID.equals(c.getClientUID()));
	}
	
}
