package com.ambraspace.pushservice.server;

import java.util.Date;

public class DelayedMessage {

	
	private long messageID;
	private String clientUID;
	private String text;
	private Date dateSent;
	boolean sent;
	
	
	public DelayedMessage(long messageID, String clientUID, String text, Date dateSent, boolean sent) {

		this.messageID = messageID;
		this.clientUID = clientUID;
		this.text = text;
		this.dateSent = dateSent;
		this.sent = sent;
		
	}


	public long getMessageID() {
		return messageID;
	}


	public String getClientUID() {
		return clientUID;
	}


	public String getText() {
		return text;
	}

	
	public Date getDateSent() {
		return dateSent;
	}

	
	public boolean isSent() {
		return sent;
	}

	
	public void setSent(boolean sent) {
		this.sent = sent;
	}
	
}
