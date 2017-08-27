package com.ambraspace.pushservice.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ConnectionManager {
	
	private static Logger logger = Logger.getLogger("ConnectionManager");
	
	// Holds all clients sorted by their nextPingTime
	private SortedSet<ConnectedClient> clientSet;
	// Holds all registered clients
	private Map<String, ConnectedClient> clientMap;
	// PingTimer thread which takes care for keeping connections alive
	private PingTimer pingTimer;
	// timed ExecutorService  - all threads get interrupted after specified time
	private ExecutorService timedThreadPool;
	// general purpose ExecutorService (for e.g. MessageProcessor)
	// Make sure that these threads will not run indefinitely
	private ExecutorService generalThreadPool;

	// Server object which this ConnectionMannager will serve
	// We need it for server options and dbManager
	private final Server server;
	
	
	/**
	 * Authenticator task takes a SocketChannel object representing
	 * new connection request. It checks for validity of the connection and,
	 * if successful, it registers the client.
	 */
	private class Authenticator implements Runnable {

		private SocketChannel connection;

		public Authenticator(SocketChannel connection) {
		
			this.connection = connection;
			
		}
		
		@Override
		public void run() {

			BufferedReader input = new BufferedReader(
					new InputStreamReader(
							Channels.newInputStream(connection)));
			PrintWriter output = new PrintWriter(
					Channels.newOutputStream(connection), true);

			String response;
			try {
				response = input.readLine();
				if (response == null) {
					throw new IOException();
				}
				if (!response.startsWith("ApplicationID=")) {
					output.println("ERR");
					throw new IOException();
				}
				String appUID = response.substring(14);
				output.println("OK");
				response = input.readLine();
				if (response == null) {
					throw new IOException();
				}
				if (!response.startsWith("ClientID=")) {
					output.println("ERR");
					throw new IOException();
				}
				String clientUID = response.substring(9);
				if ("NEW".equals(clientUID)) {
					clientUID = server.getDbManager().getNewClientUID(appUID);
					if (clientUID == null) {
						output.println("ERR");
						throw new IOException();
					} else {
						output.println("OK");
						output.println(clientUID);
					}
				} else {
					if (isRegistered(clientUID)) {
						synchronized (ConnectionManager.this) {
							ConnectedClient c = clientMap.get(clientUID);
							unregisterClient(c);
						}
					}
					if (server.getDbManager().isClientAuthorized(appUID, clientUID)) {
						output.println("OK");
					} else {
						output.println("ERR");
						throw new IOException();
					}
				}
				response = input.readLine();
				if (response == null) {
					throw new IOException();
				}
				if (!"TIMEOUT".equals(response)) {
					output.println("ERR");
					throw new IOException();
				}
				output.println(server.getOptions().getKeepAliveTimeout());
				
				registerClient(new ConnectedClient(
						clientUID,
						connection,
						new Date()));
				
			} catch (IOException e) {
				try {
					connection.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
		}
		
	}
	
	
	/**
	 * Pinger tasks takes a client and sends a PING. If the client responds
	 * the Pinger tasks returns it to the queue. If something's wrong with the
	 * client the Pinger task unregisters the client.
	 */
	private class Pinger implements Runnable {

		private ConnectedClient client;

		public Pinger(ConnectedClient client) {

			this.client = client;

		}
		
		@Override
		public void run() {
			
			synchronized (client) {
				
				BufferedReader input = new BufferedReader(new InputStreamReader(
						Channels.newInputStream(client.getConnection())));
				PrintWriter output = new PrintWriter(
						Channels.newOutputStream(client.getConnection()), true);
				
				if (!isRegistered(client.getClientUID())) {
					return;
				}

				try {
					
					output.println("PING");
					String response = null;
					response = input.readLine();
					if (response == null) {
						throw new IOException();
					}
					if (!"PONG".equals(response)) {
						output.println("ERR");
						throw new IOException();
					}
					
					push(client);

				} catch (IOException e) {
					unregisterClient(client);
				}
				
			}

		}
		
	}
	
	/**
	 * PingTimer takes care for keeping connections alive.
	 * It wakes up when it needs to PING a client. Underlying SortedSet keeps
	 * all ConnectedClient objects sorted.
	 */
	private class PingTimer extends Thread {
		
		public PingTimer() {
			setName("ConnectionManager.PingTimer");
			setDaemon(true);
		}

		@Override
		public void run() {

			while(true) {

				if (clientSet.size()>0) {
					
					ConnectedClient cc = null;
					
					synchronized (ConnectionManager.this) {
						/*
						 * Check again because the queue could have been
						 * emptied before we acquired the mutex.
						 */
						if (clientSet.size()>0) {
							cc = clientSet.first();
						} else {
							continue;
						}
					}
					
					long duration = cc.getNextPingTime().getTime() -
							System.currentTimeMillis();
					
					// Sleep until we need to ping the first client
					try {
						TimeUnit.MILLISECONDS.sleep(duration);
					} catch (InterruptedException e) {
						continue;
					}
						
					synchronized (ConnectionManager.this) {
					// Proceed only if the client we've been waiting for is 
					// still there.
						if (clientSet.size()>0 && cc.equals(clientSet.first())) {
							/*
							 * We need to call pop() here, so that PingTimer
							 * can move to the next client.
							 * Pinger task should call push() method.
							 */
							pop(cc);
							timedThreadPool.execute(new Pinger(cc));
						}
					}
					
				} else {
					// If the queue is empty sleep for an hour.
					try {
						TimeUnit.HOURS.sleep(1);
					} catch (InterruptedException e) {
						continue;
					}
				}

			}

		}
		
	}
	
	
	/**
	 * Messenger task takes a client and sends it supplied text message.
	 * If successful, it returns the client to the queue. If not, it
	 * unregisters the client.
	 * The task will return true all false whether the client received the message
	 * or not.
	 */
	private class Messenger implements Callable<Boolean> {

		private ConnectedClient client;
		private String message;
		
		public Messenger(ConnectedClient client, String message) {

			this.client = client;
			this.message = message;
			
		}
		
		
		

		public ConnectedClient getClient() {
			return client;
		}




		@SuppressWarnings("unused")
		public String getMessage() {
			return message;
		}




		@Override
		public Boolean call() {
			
			synchronized (client) {
				
				pop(client);

				if (!isRegistered(client.getClientUID())) {
					return false;
				}
				
				BufferedReader input = new BufferedReader(new InputStreamReader(
						Channels.newInputStream(client.getConnection())));
				PrintWriter output = new PrintWriter(
						Channels.newOutputStream(client.getConnection()), true);
				
				try {
					output.println("MESSAGE");
					output.println(message);
					String response = null;
					try {
						response = input.readLine();
					} catch (IOException err) {
						throw new IOException();
					}
					if (response==null) {
						throw new IOException();
					}
					if (!"OK".equals(response)) {
						output.println("ERR");
						throw new IOException();
					}
					
					push(client);
					
					return true;

				} catch (IOException e) {
					unregisterClient(client);
					return false;
				}
				
			}
				
		}
		
	}
	
	
	/**
	 * MesageProcessor task takes a Message object and for each
	 * recipient it starts separate Messenger task. It waits for all
	 * Messenger task to finish and report success of message delivery.
	 * In the end, it updates database so that failed message delivery
	 * can be tried again when corresponding client reconnects. 
	 */
	private class MessageProcessor implements Runnable {

		private Message message; 
		
		public MessageProcessor(Message message) {

			this.message = message;
			
		}
		
		@Override
		public void run() {
			
			List<Messenger> tasks = new ArrayList<Messenger>();
			List<Future<Boolean>> results;
			
			{
				Iterator<String> i = message.getRecipients().keySet().iterator();
				String clientUID;
				ConnectedClient client;
				while (i.hasNext()) {
					clientUID = i.next();
					client = clientMap.get(clientUID);
					if (client!=null) {
						tasks.add(new Messenger(client, message.getText()));
					}
				}
			}
			
			try {
				results = timedThreadPool.invokeAll(tasks);
			} catch (InterruptedException e) {
				return;
			}
			
			
			{
				Iterator<Future<Boolean>> i = results.iterator();
				Future<Boolean> res;
				Boolean resB;
				while (i.hasNext()) {
					res = i.next();
					try {
						resB = res.get();
					} catch (InterruptedException e) {
						resB = false;
					} catch (ExecutionException e) {
						resB = false;
					}
					message.getRecipients().put(
							tasks.get(results.indexOf(res)).getClient().getClientUID(),
							resB);
				}
			}
				

			server.getDbManager().updateMessageStatus(message);
			
		}
		
	}

	
	/**
	 * DelayedMessagesSender sends delayed messages, i.e. messages
	 * not delivered to a client (client offline or other reasons).
	 * It must also report whether messages are delivered or not.
	 * Number of delayed messages to be sent depends on server configuration.
	 */
	private class DelayedMessagesSender implements Runnable {

		List<DelayedMessage> messages;
		
		public DelayedMessagesSender(List<DelayedMessage> messages) {

			this.messages = messages;
			
		}

		@Override
		public void run() {
			
			List<Messenger> tasks = new ArrayList<Messenger>();
			List<Future<Boolean>> results;
			
			{
				Iterator<DelayedMessage> i = messages.iterator();
				DelayedMessage message;
				ConnectedClient client;
				while (i.hasNext()) {
					message = i.next();
					client = clientMap.get(message.getClientUID());
					if (client!=null) {
						tasks.add(new Messenger(client, message.getText()));
					}
				}
			}

			
			try {
				results = timedThreadPool.invokeAll(tasks);
			} catch (InterruptedException e) {
				return;
			}
			

			Map<String, Boolean> mapping = new HashMap<String, Boolean>();

			{
				Iterator<Future<Boolean>> i = results.iterator();
				Future<Boolean> res;
				Boolean resB;
				while (i.hasNext()) {
					res = i.next();
					try {
						resB = res.get();
					} catch (InterruptedException e) {
						resB = false;
					} catch (ExecutionException e) {
						resB = false;
					}
					mapping.put(
							tasks.get(results.indexOf(res)).getClient().getClientUID(),
							resB);
				}
			}
			
			{
				Iterator<DelayedMessage> i = messages.iterator();
				DelayedMessage message;
				Boolean resB;
				while (i.hasNext()) {
					message = i.next();
					resB = mapping.get(message.getClientUID());
					if (resB == null)
						resB = false;
					message.setSent(resB);
				}
			}
			
			server.getDbManager().updateDelayedMessageStatus(messages);
			
		}
		
	}
	
	
	/**
	 * Instantiates new ConnectionManager object responsible for all
	 * operations on connection requests and ConnectedClient objects.
	 * @param server server which this ConnectionManager will serve
	 */
	public ConnectionManager(Server server) {
		this.server = server;
		timedThreadPool = new TimedFixedThreadPool(
				server.getOptions().getTimedThreadPoolSize(),
				server.getOptions().getClientResponseTimeout(),
				TimeUnit.SECONDS,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setDaemon(true);
						t.setName("TimedThread");
						return t;
					}
				});
		generalThreadPool = Executors.newFixedThreadPool(
				server.getOptions().getTimedThreadPoolSize(),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setDaemon(true);
						return t;
					}
				});
		clientMap = new HashMap<String, ConnectedClient>();
		clientSet = new TreeSet<ConnectedClient>(new PingTimeComparator());
		pingTimer = new PingTimer();
		pingTimer.start();
	}
	
	
	/**
	 * Returns total number of currently registered clients (connections).
	 * @return number of registered clients
	 */
	public synchronized int getClientCount() {
		
		return clientMap.size();

	}
	
	/**
	 * This method accepts SocketChannel object which represents
	 * new connection request. The SocketChannel object and corresponding
	 * client will be checked for validity. If connection is valid, a new
	 * ConnectedClient object will be created, containing client's unique ID,
	 * and this SocketChannel object, and will be added to underlying containers.
	 * @param connection SocketChannel object representing new connection request
	 */
	public void submit(SocketChannel connection) {
		
		timedThreadPool.execute(new Authenticator(connection));
		
	}
	
	
	/**
	 * Registers new ConnectedClient after corresponding client is Authenticated.
	 * This method should only be called by Authenticator thread following
	 * successful authentication procedure.
	 * @param client client to be registered
	 */
	private synchronized void registerClient(ConnectedClient client) {
		
		/*
		 * If the client is already registered, don't do anything.
		 * The new client will not be registered until existing client is
		 * unregistered.
		 * This (rare) situation can happen when a client connects to
		 * the server, while existing connection has not been unregistered (or in
		 * case of client UID spoofing). The only way for a client to connect is to
		 * wait until PingTimer discovers failed connection and unregisters existing
		 * connection associated with the same client UID.
		 */
		if (isRegistered(client.getClientUID())) {
			logger.logp(Level.WARNING, "ConnectionManager", "registerClient()", "Already registered! Skipping.");
			return;
		}

		clientMap.put(client.getClientUID(), client);

		push(client);
		
		List<DelayedMessage> delayedMessages = 
				server.getDbManager().getDelayedMessages(client.getClientUID(),
						server.getOptions().getDelayedMessagesQty());

		if (delayedMessages.size()>0) {
			generalThreadPool.submit(
					new DelayedMessagesSender(delayedMessages));
		}
		
		logger.logp(Level.INFO, "ConnectionManager", "registerClient()", "Client #" + client.getClientUID() + " registered.");
		logger.logp(Level.INFO, "ConnectionManager", "registerClient()", "Total clients: " + getClientCount() + ".");
	}
	
	/**
	 * Unregisters the client. This method should be invoked only when
	 * client connection becomes unusable (the client doesn't respond to PING),
	 * the client's connection is broken or the client doesn't reply as expected
	 * (protocol mismatch). After this method is called, the client will be
	 * permanently removed from underlying containers and client will be considered
	 * offline.
	 * @param client client to be unregistered.
	 */
	private synchronized void unregisterClient(ConnectedClient client) {
		
		pop(client);
		
		clientMap.remove(client.getClientUID());

		/*
		 * Try to close related SocketChannel.
		 */
		try {
			client.getConnection().close();
		} catch (IOException e) {
			logger.logp(Level.SEVERE, "ConnectionManager", "unregisterClient()", "Error closing connection!");
			e.printStackTrace();
		}

		logger.logp(Level.INFO, "ConnectionManager", "unregisterClient()", "Client #" + client.getClientUID() + " unregistered.");
		logger.logp(Level.INFO, "ConnectionManager", "unregisterClient()", "Total clients: " + getClientCount() + ".");
	}

	
	
	/**
	 * Pushes a client into underlying queue which takes care for clients to
	 * be PINGed in timely fashion. This method should be called by tasks that
	 * temporary take a client to communicate with it, after being finished with it.
	 * @param c client to be pushed into the queue
	 */
	private synchronized void push(ConnectedClient c) {
		
		/*
		 * Don't take the client into clientSet if it's not already in clientMap.
		 * clientMap must always contain all registered clients, so if the client
		 * supplied in not there, we won't accept it in clientSet.
		 */
		if (!clientMap.containsKey(c.getClientUID())) {
			logger.logp(Level.WARNING, "ConnectedClient", "push()", "Client #" + c.getClientUID() + " not registered! Skipping.");
			return;
		}

		/*
		 * Makes sure that the client is removed from clientSet before trying to
		 * change its nextPingTime.
		 */
		if (clientSet.contains(c)) {
			clientSet.remove(c);
			logger.logp(Level.WARNING, "ConnectedClient", "push()", "clientSet already contained client #" + c.getClientUID() + ". Removed.");
		}

		c.setNextPingTime(new Date(System.currentTimeMillis() + 
				server.getOptions().getKeepAliveTimeout()*1000));

		/*
		 * The following piece of code makes sure that all registered
		 * clients are evenly spaced in time, so that PingTimer wakes up
		 * periodically and that CPU and memory usage are nice and flat.
		 */
		Date newDate = new Date();
		
		if (!clientSet.isEmpty()) {
			newDate.setTime((clientSet.last().getNextPingTime().getTime()));
		}
		
		long delta = server.getOptions().getKeepAliveTimeout()*1000 /
			getClientCount(); // da li dijeliti sa brojem klijenata u Set-u, a ne Map-i?
	
		if (delta>0) {
			newDate.setTime(newDate.getTime() + delta);
			
			if (newDate.before(c.getNextPingTime())) {
				c.setNextPingTime(newDate);
			}
		}
	
		clientSet.add(c);
		
		/*
		 * Underlying SortedSet is changed, so we need to wake up PingTimer
		 * thread. 
		 */
		pingTimer.interrupt();
		
	}
	
	
	/**
	 * Pops a client from underlying queue.
	 * This method should be called by tasks that temporary take a client
	 * to communicate with it (e.g. Pinger or Messenger tasks).
	 * The removal is necessary so that PingerThread can move to another client.
	 * After being finished the tasks should call push() method to return the
	 * client to the queue.
	 * @param c client to be popped from the queue
	 */
	private synchronized void pop(ConnectedClient c) {
		
		/*
		 * If the client is not in the queue, don't do anything. This can
		 * happen when another task already holds that client.
		 * If underlying queue in not changed no need to interrupt PingTimer.
		 */
		if (!clientSet.contains(c)) {
			logger.logp(Level.WARNING, "ConnectionManager", "pop()", "Client #" + c.getClientUID() + " is not in the Set! Skipping.");
			return;
		}

		clientSet.remove(c);

		/*
		 * This will interrupt PingTimer only if this method is called
		 * by a different thread. Prevents PingerThread to interrupt itself
		 * by calling this method.
		 */
		if (!Thread.currentThread().equals(pingTimer)) {
			pingTimer.interrupt();
		}
		
	}
	
	
	/**
	 * This method is used to check whether a client is already registered.
	 * @param clientUID client's unique ID
	 * @return true if the client is already registered, false othrewise
	 */
	public synchronized boolean isRegistered(String clientUID) {
		if (clientMap.containsKey(clientUID)) {
			return true;
		} else {
			return false;
		}
	}
	
	
	/**
	 * Gets a list of all registered (i.e. online) clients.
	 * @return set of client unique IDs
	 */
	public synchronized Set<String> getRegisteredClients() {
		return new HashSet<String>(clientMap.keySet());
	}
	
	
	/**
	 * Processes Message object by sending a message to all
	 * specified recipients.
	 * This method doesn't check who the sender is or which application
	 * the recipients belong to (because Message object doesn't contain
	 * that information). The job of the user of this method is to check the
	 * validity of data contained within Message object.
	 * @param message a Message object to process
	 */
	public void processMessage(Message message) {
		if (message == null) {
			return;
		}
		generalThreadPool.execute(new MessageProcessor(message));
	}

	
}
