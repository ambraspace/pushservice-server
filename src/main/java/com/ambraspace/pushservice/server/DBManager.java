package com.ambraspace.pushservice.server;

import java.util.List;

public interface DBManager {
	
	public String getNewClientUID(String appUID);
	
	public boolean isClientAuthorized(String appUID, String ClientUID);
	
	public Message getMessage(long messageID);
	
	public void updateMessageStatus(Message message);
	
	public List<DelayedMessage> getDelayedMessages(String clientUID, int limit);
	
	public void updateDelayedMessageStatus(List<DelayedMessage> messages);

}
