package com.example.mongo.util;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDb {

	private static MongoClient client;

	public static MongoClient getClient() {
		try {
			if (client == null) {
				client = new MongoClient("localhost", 27017);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return client;
	}

	public static void shutdownMongoDB() {
		client.close();
		client = null;
	}

	public static DB getDb() {
		return MongoDb.getClient().getDB("db");
	}

	public static DB getTestDb() {
		return MongoDb.getClient().getDB("test");
	}

	// getDB() is deperecated so alternate implementation to get database
	public static MongoDatabase getTestMongoDb() {
		return MongoDb.getClient().getDatabase("test");
	}

}