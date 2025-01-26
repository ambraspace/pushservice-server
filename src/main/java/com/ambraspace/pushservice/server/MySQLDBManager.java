package com.ambraspace.pushservice.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MySQLDBManager implements DBManager {
	
	private static Logger logger = Logger.getLogger("MySQLDBManager");

	private final String host;
	private final int port;
	private final String database;
	private final String username;
	private final String password;
	private final String encPwd;
	
	private Connection connection;
	
	public MySQLDBManager(String host, int port, String database,
			String username, String password, String encPwd) throws InstantiationException {

			this.host = host;
			this.port = port;
			this.database = database;
			this.username = username;
			this.password = password;
			this.encPwd = encPwd;
			
			try {
				establishConnection();
			} catch (SQLException e) {
				throw new InstantiationException();
			}
			
	}
	
	private void establishConnection() throws SQLException {
		
		try {
			if (connection == null || (!connection.isValid(5))) {
				connection = DriverManager.getConnection("jdbc:mysql://" +
						this.host +
						(this.port <= 0 ? "/" : ":"+port+"/") +
						this.database + "?user=" +
						this.username + "&password=" +
						this.password);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			}
		} catch (SQLException e) {
			logger.logp(Level.SEVERE, "MySQLDBManager", "establishConnection()", "Error establishing connection!");
			throw new SQLException("Error establishing connection!");
		}

	}
	
	@Override
	public synchronized String getNewClientUID(String appUID) {
        try {
    		establishConnection();
        	connection.setAutoCommit(false);
        	Statement stmt = connection.createStatement();
        	stmt.execute("SELECT id FROM applications WHERE uid=\"" + appUID +
        			"\" AND disabled=0");
        	ResultSet rs = stmt.getResultSet();
        	rs.next();
        	Long appID = rs.getLong("id");
        	stmt.execute("SELECT HEX(AES_ENCRYPT(UUID(),SHA2(\"" +
        			encPwd + "\",512)))");
        	rs = stmt.getResultSet();
        	rs.next();
        	String clientUID = rs.getString(1);
        	stmt.execute("INSERT INTO clients(uid, application_id) VALUES(\"" +
        			clientUID + "\", " + appID + ")");
        	connection.commit();
        	return clientUID;
        } catch (SQLException e) {
        	logger.logp(Level.SEVERE, "MySQLDBManager", "getNewClientID()","SQLException caught!");
        	return null;
        }
	}

	@Override
	public synchronized boolean isClientAuthorized(String appUID, String clientUID) {
    	try {
    		establishConnection();
			Statement stmt = connection.createStatement();
			stmt.execute(
					"SELECT COUNT(clients.uid) FROM clients " +
					"JOIN applications ON clients.application_id=applications.id " +
					"WHERE clients.uid=\"" + clientUID + "\" " +
					"AND applications.uid = \"" + appUID + "\" " +
					"AND clients.disabled=0 AND applications.disabled=0");
			ResultSet rs = stmt.getResultSet();
			rs.next();
			int result = rs.getInt(1);
			if (result == 1) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	@Override
	public synchronized Message getMessage(long messageID) {
		try {
			establishConnection();
			Statement stmt = connection.createStatement();
			stmt.execute(
					"SELECT messages.id id, messages.text text, messages.date date, " +
							"clients.uid uid, " +
							"client_messages.delivered delivered " +
					"FROM messages " +
					"LEFT JOIN client_messages ON messages.id=client_messages.message_id " +
					"LEFT JOIN clients ON client_messages.client_id=clients.id "+
					"WHERE messages.id=" + messageID + " AND clients.disabled=0");
			ResultSet rs = stmt.getResultSet();
			Message ret = new Message();
			if (rs.next()) {
				ret.setId(rs.getLong("id"));
				ret.setText(rs.getString("text"));
				ret.setDateSent(rs.getDate("date"));
				do {
					ret.getRecipients().put(rs.getString("uid"), rs.getBoolean("delivered"));
				} while (rs.next());
				return ret;
			} else {
				return null;
			}
		} catch (SQLException e) {
			logger.logp(Level.SEVERE, "MySQLDBManager", "getMessage()", "SQLException caught!");
			return null;
		}
	}

	@Override
	public synchronized void updateMessageStatus(Message message) {
        try {
    		establishConnection();
        	connection.setAutoCommit(false);
        	Statement stmt = connection.createStatement();
        	for (String uid : message.getRecipients().keySet()) {
        		try {
	            	logger.logp(Level.INFO, "MySQLDBManager", "updateMessageStatus()", "SELECT clients.id id FROM clients " +
	        				"WHERE clients.uid=\"" + uid + "\" AND " +
	        				"clients.disabled=0");
	        		stmt.execute("SELECT clients.id id FROM clients " +
	        				"WHERE clients.uid=\"" + uid + "\" AND " +
	        				"clients.disabled=0");
	        		ResultSet rs = stmt.getResultSet();
	        		rs.next();
	        		long clientID = rs.getLong("id");
	            	logger.logp(Level.INFO, "MySQLDBManager", "updateMessageStatus()", "UPDATE client_messages SET delivered=" +
	        				(message.getRecipients().get(uid) ? 1 : 0) +
	        				" WHERE message_id=" + message.getId() + 
	        				" AND client_id=" + clientID);
	        		stmt.executeUpdate("UPDATE client_messages SET delivered=" +
	        				(message.getRecipients().get(uid) ? 1 : 0) +
	        				" WHERE message_id=" + message.getId() + 
	        				" AND client_id=" + clientID);
        		} catch (SQLException err) {
        			logger.logp(Level.SEVERE, "MySQLDBManager", "updateMessageStatus()","Error updating record!");
        			continue;
        		}
			}
        	connection.commit();
        } catch (SQLException e) {
        	logger.logp(Level.SEVERE, "MySQLDBManager", "updateMessageStatus()","SQLException caught!");
        }
	}

	@Override
	public synchronized List<DelayedMessage> getDelayedMessages(String clientUID, int limit) {

		try {
			establishConnection();
			Statement stmt = connection.createStatement();
			stmt.execute(
					"SELECT id, text, date FROM " +
					"(SELECT messages.id id, messages.text text, messages.date date, client_messages.delivered delivered " +
					"FROM client_messages " +
					"LEFT JOIN messages ON client_messages.message_id=messages.id " +
					"LEFT JOIN clients ON client_messages.client_id=clients.id "+
					"WHERE clients.uid=\"" + clientUID + "\" AND clients.disabled=0 " +
					"ORDER BY client_messages.message_id DESC " + (limit < 1 ? "" : "LIMIT " + limit) + ") msgs " +
					"WHERE delivered=0 ORDER BY id ASC");
			ResultSet rs = stmt.getResultSet();
			List<DelayedMessage> retVal = new ArrayList<DelayedMessage>(); 
			while (rs.next()) {
				DelayedMessage dm = new DelayedMessage(
						rs.getLong("id"),
						clientUID,
						rs.getString("text"),
						rs.getDate("date"),
						false);
				retVal.add(dm);
			}
			return retVal;
		} catch (SQLException e) {
	    	logger.logp(Level.SEVERE, "MySQLDBManager", "getDelayedMessages()","SQLException caught!");
	    	e.printStackTrace();
			return null;
		}
	}

	@Override
	public synchronized void updateDelayedMessageStatus(List<DelayedMessage> messages) {

		if (messages == null || messages.size()==0) {
			return;
		}
		
		try {
			establishConnection();
			connection.setAutoCommit(false);
			Statement stmt = connection.createStatement();
			Iterator<DelayedMessage> i=messages.iterator();
			while (i.hasNext()) {
				DelayedMessage d = i.next();
				try {
					ResultSet rs = stmt.executeQuery("SELECT id FROM clients WHERE uid=\"" + 
							d.getClientUID() + "\"");
					rs.next();
					Long clientID = rs.getLong("id");
					stmt.execute("UPDATE client_messages SET delivered=" +
							(d.isSent() ? 1 : 0) + " " + 
							"WHERE message_id=" + d.getMessageID() + " AND " +
							"client_id=" + clientID);
				} catch (SQLException err) {
					logger.logp(Level.SEVERE, "MySQLDBManager", "updateDelayedMessageStatus()","Error updating.");
					continue;
				}
			}
			connection.commit();
		} catch (SQLException e) {
			logger.logp(Level.SEVERE, "MySQLDBManager", "updateDelayedMessageStatus()","SQLException caught!");
		}
	}

}
