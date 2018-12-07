package com.abds.prototype1;

import redis.clients.jedis.Jedis;

public class QueueAdapter {

	private final String waitQueue = "waitQueue";
	private final String workQueue = "workQueue";

	Jedis instance = null;

	public QueueAdapter(Jedis jedis) {
		this.instance = jedis;
	}

	public boolean sendJobToWaitQueue(String uri) {
		try {
			instance.lpush(waitQueue, uri);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}