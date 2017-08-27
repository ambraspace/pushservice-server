package com.ambraspace.pushservice.server;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Message {

	private long id;
	private String text;
	private Date dateSent;
	private Map<String, Boolean> clientUIDs = new HashMap<String, Boolean>();
	
	
	
	public Message() {
		
	}
	
	
	
	public long getId() {
		return id;
	}



	public String getText() {
		return text;
	}



	public Date getDateSent() {
		return dateSent;
	}



	public Map<String, Boolean> getRecipients() {
		return clientUIDs;
	}



	public void setId(long id) {
		this.id = id;
	}



	public void setText(String text) {
		this.text = text;
	}



	public void setDateSent(Date dateSent) {
		this.dateSent = dateSent;
	}



	public static Message parseJSONMessage(JSONObject jSONMessage) {
		
		Message ret = new Message();

		try {
			ret.setId(jSONMessage.getLong("id"));
			ret.setText(jSONMessage.getString("text"));
			ret.setDateSent(new Date(jSONMessage.getLong("date")));
			JSONArray tos = jSONMessage.getJSONArray("to");
			Iterator<Object> i = tos.iterator();
			while (i.hasNext()) {
				String uid = (String) i.next();
				ret.getRecipients().put(uid, false);
			}
			return ret;
		} catch (JSONException e) {
			return null;
		}
		
	}
	
	

	public static Message parseJSONMessage(String JSONMessage) {
	
		try {
			return parseJSONMessage(new JSONObject(JSONMessage));
		} catch (JSONException e) {
			return null;
		}
		
	}

}
