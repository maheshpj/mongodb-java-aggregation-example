package com.example.mongo.util;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;

public final class Util {

	public static void printOutput(DBCollection coll, List<DBObject> pipeline) {
		AggregationOutput output = coll.aggregate(pipeline);
		Iterator<DBObject> it = output.results().iterator();
		while (it.hasNext()) {
			DBObject out = it.next();
			System.out.println(out.toString());
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void printOutput(MongoCollection coll, List<DBObject> pipeline) {
		AggregateIterable<Document> output = coll.aggregate(pipeline);
		Iterator<Document> it = output.iterator();
		while (it.hasNext()) {
			Document out = it.next();
			System.out.println(out.toString());
		}
	}
}
