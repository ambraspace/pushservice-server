package com.ambraspace.pushservice.server;

public class ServerOptions {

	private int clientResponseTimeout = 20; // seconds
	private int delayedMessagesQty = -1; // -1 for all
	private int generalThreadPoolSize = 10;
	private int keepAliveTimeout = 120; // seconds;
	private int maxClients = 1000;
	private int port = 12345;
	private int servicePort = 12346;
	private int timedThreadPoolSize = 40;
	
	
	public ServerOptions() {

	}
	
	
	public ServerOptions(ServerOptions options) {

		this.port = options.getPort();
		this.servicePort = options.getServicePort();
		this.maxClients = options.getMaxClients();
		this.timedThreadPoolSize = options.getTimedThreadPoolSize();
		this.generalThreadPoolSize = options.getGeneralThreadPoolSize();
		this.keepAliveTimeout = options.getKeepAliveTimeout();
		this.clientResponseTimeout = options.getClientResponseTimeout();
		this.delayedMessagesQty = options.getDelayedMessagesQty();
		
	}

	public int getClientResponseTimeout() {
		return clientResponseTimeout;
	}


	public int getDelayedMessagesQty() {
		return delayedMessagesQty;
	}


	public int getGeneralThreadPoolSize() {
		return generalThreadPoolSize;
	}


	public int getKeepAliveTimeout() {
		return keepAliveTimeout;
	}


	public int getMaxClients() {
		return maxClients;
	}
	
	public int getPort() {
		return port;
	}

	public int getServicePort() {
		return servicePort;
	}

	public int getTimedThreadPoolSize() {
		return timedThreadPoolSize;
	}
	
	public void setClientResponseTimeout(int clientResponseTimeout) {
		this.clientResponseTimeout = clientResponseTimeout;
	}
	
	public void setDelayedMessagesQty(int delayedMessagesQty) {
		this.delayedMessagesQty = delayedMessagesQty;
	}
	
	public void setGeneralThreadPoolSize(int generalThreadPoolSize) {
		this.generalThreadPoolSize = generalThreadPoolSize;
	}

	public void setKeepAliveTimeout(int keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
	}

	public void setMaxClients(int maxClients) {
		this.maxClients = maxClients;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setServicePort(int servicePort) {
		this.servicePort = servicePort;
	}

	public void setTimedThreadPoolSize(int threadPoolSize) {
		this.timedThreadPoolSize = threadPoolSize;
	}
	
}
