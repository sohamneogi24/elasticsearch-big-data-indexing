package com.queue.demo;


import org.apache.http.HttpHost;

import org.apache.http.entity.ContentType;

import org.apache.http.nio.entity.NStringEntity;



import java.io.IOException;

import java.util.Collections;

import java.util.Map;



import org.apache.http.HttpEntity;

import org.elasticsearch.client.Response;

import org.elasticsearch.client.ResponseException;

import org.elasticsearch.client.RestClient;

import org.json.JSONObject;

import org.springframework.boot.SpringApplication;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import redis.clients.jedis.Jedis;


@SpringBootApplication

public class QueueDequeueApplication {



	static Jedis jedis = null;

	private final static String waitQueue = "waitQueue";

	private final static String workQueue = "workQueue";



	public static void main(String[] args) throws IOException {

		SpringApplication.run(QueueDequeueApplication.class, args);

		jedis = RedisConfiguration.getJedis();

		process();

	}



	private static void process() throws IOException {

		RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

		while (true) {

			String answer = jedis.brpoplpush(waitQueue, workQueue, 0);

			System.out.println(answer);

			try {

				Map<String, String> plan = null;

				HttpEntity entity = null;

				JSONObject o = null;

				Response indexResponse = null;

				String[] arrOfStr = answer.split("_");

				if (arrOfStr.length == 2) {

					plan = jedis.hgetAll(answer);

					o = new JSONObject(plan);

					entity = new NStringEntity(o.toString(), ContentType.APPLICATION_JSON);

					indexResponse = restClient.performRequest("PUT", "/example.com/plan/" + plan.get("objectId"),

							Collections.<String, String>emptyMap(), entity);

				} else {

					if (answer.contains("planCostShares")) {

						plan = jedis.hgetAll(answer);

						o = new JSONObject(plan);

						entity = new NStringEntity(o.toString(), ContentType.APPLICATION_JSON);

						indexResponse = restClient.performRequest("PUT",

								"/example.com/membercostshare/" + plan.get("objectId") + "?parent=" + arrOfStr[1],

								Collections.<String, String>emptyMap(), entity);

					} else if (answer.contains("linkedPlanServices")) {

						String[] arr = answer.split("_");

						if (arr[4].equals("planservice")) {

							plan = jedis.hgetAll(answer);

							o = new JSONObject(plan);

							entity = new NStringEntity(o.toString(), ContentType.APPLICATION_JSON);

							indexResponse = restClient.performRequest("PUT",

									"/example.com/planservice/" + plan.get("objectId") + "?parent=" + arrOfStr[1],

									Collections.<String, String>emptyMap(), entity);

						}

						else if (arr[4].equals("planserviceCostShares")) {

							plan = jedis.hgetAll(answer);

							o = new JSONObject(plan);

							entity = new NStringEntity(o.toString(), ContentType.APPLICATION_JSON);

							indexResponse = restClient.performRequest("PUT",

									"/example.com/costshare/" + plan.get("objectId") + "?parent=" + arrOfStr[3],

									Collections.<String, String>emptyMap(), entity);



						} else if (arr[4].contains("linkedService")) {

							plan = jedis.hgetAll(answer);

							o = new JSONObject(plan);

							entity = new NStringEntity(o.toString(), ContentType.APPLICATION_JSON);

							indexResponse = restClient.performRequest("PUT",

									"/example.com/service/" + plan.get("objectId") + "?parent=" + arrOfStr[3],

									Collections.<String, String>emptyMap(), entity);

						}

					}

				}

				jedis.rpop(workQueue);

			} catch (ResponseException e) {

				String test = jedis.brpoplpush(workQueue, waitQueue, 0);

				System.out.println(test);

			}



		}



	}



}

