package com.ambraspace.pushservice.server;

import java.util.Comparator;

public class PingTimeComparator implements Comparator<ConnectedClient> {

	@Override
	public int compare(ConnectedClient o1, ConnectedClient o2) {

		if (o1.getNextPingTime().equals(o2.getNextPingTime())) {
			return o1.getClientUID().compareTo(o2.getClientUID());
		} else {
			return o1.getNextPingTime().compareTo(o2.getNextPingTime());
		}
	}

}
