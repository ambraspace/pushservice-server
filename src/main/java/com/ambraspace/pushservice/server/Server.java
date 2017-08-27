package com.ambraspace.pushservice.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Server {
	
	private static Logger logger = Logger.getLogger("Server");
	
	private final ServerOptions options;
	private final DBManager dbManager;
	private final ConnectionManager connectionManager;
	
	
	private class Listener extends Thread {
		
		public Listener() {
			setName("Server.Listener");
		}
		
		@Override
		public void run() {
			
			try (ServerSocketChannel ssc = ServerSocketChannel.open()) {

				ssc.bind(new InetSocketAddress(options.getPort()));

				SocketChannel newConnection = null;
				
				while (!Thread.interrupted()) {

					newConnection = ssc.accept();
					
					if (connectionManager.getClientCount()>=options.getMaxClients()) {
						newConnection.close();
					} else {
						connectionManager.submit(newConnection);
					}
					
				}

			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			
		}
		
		
	}
	
	
	private class ServiceListener extends Thread {
		
		public ServiceListener() {
			setName("Server.ServiceListener");
			setDaemon(true);
		}

		@Override
		public void run() {

			try (ServerSocketChannel ssc = ServerSocketChannel.open()) {
				
				ssc.bind(new InetSocketAddress(options.getServicePort()));
				
				SocketChannel connection = null;
				BufferedReader input = null;
				PrintWriter output = null;
				String data = null;
				
				while (!Thread.interrupted()) {
					connection = ssc.accept();
					input = new BufferedReader(
							new InputStreamReader(
									Channels.newInputStream(connection)));
					output = new PrintWriter(
							Channels.newOutputStream(connection), true);
					data = input.readLine();
					output.println(processData(data));
					connection.close();
				}

			} catch (IOException e) {
				logger.logp(Level.SEVERE, "Server.ServiceListener", "run()", "Error starting ServiceListener.");
			}
			
		}
		

		private String processData(String data) {
			
			JSONObject jSONdata = null;
			try {

				jSONdata = new JSONObject(data);

				switch (jSONdata.getString("type")) {
					case "message":
						Message m = Message.parseJSONMessage(
								jSONdata.getJSONObject("message"));
						logger.logp(Level.INFO, "Server.ServiceListener", "processData()", jSONdata.toString());
						connectionManager.processMessage(m);
						return "OK";
					case "command":
						switch (jSONdata.getString("command")) {
							case "getRegisteredClients":
								Set<String> ids = connectionManager.getRegisteredClients();
								JSONArray arr = new JSONArray(ids);
								return arr.toString();
							default:
								logger.logp(Level.SEVERE, "Server.ServiceListener", "processData()", "Unknown command");
								return "ERR";
						}
					default:
						logger.logp(Level.SEVERE, "Server.ServiceListener", "processData()", "Unknown data type submitted!");
						return "ERR";
				}

			} catch (JSONException e) {
				logger.logp(Level.SEVERE, "Server.ServiceListener", "processData()", "Error processing JSON data!");
				return "ERR";
			}
			
			
		}
		
		
	}

	
	public Server(ServerOptions options, DBManager dbManager) {
		if (options == null) {
			this.options = new ServerOptions();
		} else {
			this.options = options;
		}
		if (dbManager == null) {
			throw new NullPointerException();
		} else {
			this.dbManager = dbManager;
		}
		connectionManager = new ConnectionManager(this);
		new Listener().start();
		new ServiceListener().start();
	}
	
	
	
	public ServerOptions getOptions() {
		// Return a copy, so others don't mess with our settings
		return new ServerOptions(options);
	}



	public DBManager getDbManager() {
		return dbManager;
	}

	

	public static void main(String[] args) {

		String propertiesFileName = System.getProperty("user.dir") +
				File.separator + "pushservice.properties";

		if (args.length > 1) {
			System.err.println("Usage: java -jar Server.jar [properties_file]");
			System.exit(1);
		}

		if (args.length == 1) {
			propertiesFileName = args[0];
		}

		final String OPTION_PORT = "port";
		final String OPTION_SERVICE_PORT = "servicePort";
		final String OPTION_MAX_CLIENTS = "maxClients";
		final String OPTION_TIMED_THREAD_POOL_SIZE = "timedThreadPoolSize";
		final String OPTION_GENERAL_THREAD_POOL_SIZE = "generalThreadPoolSize";
		final String OPTION_KEEP_ALIVE_TIMEOUT = "keepAliveTimeout";
		final String OPTION_CLIENT_RESPONSE_TIMEOUT = "clientResponseTimeout";
		final String OPTION_DELAYED_MESSAGES_QTY = "clientMessagesQty";

		final String DB_HOST = "dbHost";
		final String DB_PORT = "dbPort";
		final String DB_NAME = "dbName";
		final String DB_USER = "dbUser";
		final String DB_PASSWORD = "dbPassword";
		final String ENCRYPTION_KEY = "encryptionKey";

		Properties properties = new Properties();
		ServerOptions serverOptions = new ServerOptions();

		File propertiesFile = new File(propertiesFileName);

		if (!propertiesFile.exists()) {
			try {
				propertiesFile.createNewFile();
			} catch (IOException e) {
				System.err.println("Cannot create properties file!");
				System.exit(1);
			}
		}

		try {

			FileInputStream fin = new FileInputStream(propertiesFile);
			properties.load(fin);
			fin.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		properties.setProperty(OPTION_PORT,
				properties.getProperty(OPTION_PORT, "" + serverOptions.getPort()));
		properties.setProperty(OPTION_SERVICE_PORT,
				properties.getProperty(OPTION_SERVICE_PORT, "" + serverOptions.getServicePort()));
		properties.setProperty(OPTION_MAX_CLIENTS,
				properties.getProperty(OPTION_MAX_CLIENTS, "" + serverOptions.getMaxClients()));
		properties.setProperty(OPTION_TIMED_THREAD_POOL_SIZE,
				properties.getProperty(OPTION_TIMED_THREAD_POOL_SIZE, "" + serverOptions.getTimedThreadPoolSize()));
		properties.setProperty(OPTION_GENERAL_THREAD_POOL_SIZE,
				properties.getProperty(OPTION_GENERAL_THREAD_POOL_SIZE, "" + serverOptions.getGeneralThreadPoolSize()));
		properties.setProperty(OPTION_KEEP_ALIVE_TIMEOUT,
				properties.getProperty(OPTION_KEEP_ALIVE_TIMEOUT, "" + serverOptions.getKeepAliveTimeout()));
		properties.setProperty(OPTION_CLIENT_RESPONSE_TIMEOUT,
				properties.getProperty(OPTION_CLIENT_RESPONSE_TIMEOUT, "" + serverOptions.getClientResponseTimeout()));
		properties.setProperty(OPTION_DELAYED_MESSAGES_QTY,
				properties.getProperty(OPTION_DELAYED_MESSAGES_QTY, "" + serverOptions.getDelayedMessagesQty()));

		properties.setProperty(DB_HOST,
				properties.getProperty(DB_HOST, "localhost"));
		properties.setProperty(DB_PORT,
				properties.getProperty(DB_PORT, "3306"));
		properties.setProperty(DB_NAME,
				properties.getProperty(DB_NAME, "test"));
		properties.setProperty(DB_USER,
				properties.getProperty(DB_USER, "test"));
		properties.setProperty(DB_PASSWORD,
				properties.getProperty(DB_PASSWORD, ""));
		properties.setProperty(ENCRYPTION_KEY,
				properties.getProperty(ENCRYPTION_KEY, "encryption_key"));

		try {

			FileOutputStream fout = new FileOutputStream(propertiesFile);
			properties.store(fout, null);
			fout.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		
		serverOptions.setPort(Integer.parseInt(
				properties.getProperty(OPTION_PORT)));
		serverOptions.setServicePort(Integer.parseInt(
				properties.getProperty(OPTION_SERVICE_PORT)));
		serverOptions.setMaxClients(Integer.parseInt(
				properties.getProperty(OPTION_MAX_CLIENTS)));
		serverOptions.setTimedThreadPoolSize(Integer.parseInt(
				properties.getProperty(OPTION_TIMED_THREAD_POOL_SIZE)));
		serverOptions.setGeneralThreadPoolSize(Integer.parseInt(
				properties.getProperty(OPTION_GENERAL_THREAD_POOL_SIZE)));
		serverOptions.setKeepAliveTimeout(Integer.parseInt(
				properties.getProperty(OPTION_KEEP_ALIVE_TIMEOUT)));
		serverOptions.setClientResponseTimeout(Integer.parseInt(
				properties.getProperty(OPTION_CLIENT_RESPONSE_TIMEOUT)));
		serverOptions.setDelayedMessagesQty(Integer.parseInt(
				properties.getProperty(OPTION_DELAYED_MESSAGES_QTY)));

		DBManager dbManager = null;
		
		try {
			dbManager = new MySQLDBManager(
					properties.getProperty(DB_HOST),
					Integer.parseInt(properties.getProperty(DB_PORT)),
					properties.getProperty(DB_NAME),
					properties.getProperty(DB_USER),
					properties.getProperty(DB_PASSWORD),
					properties.getProperty(ENCRYPTION_KEY));
		} catch (NumberFormatException | InstantiationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		new Server(serverOptions, dbManager);
	
	}
	 
}
