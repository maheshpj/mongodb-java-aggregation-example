package com.example.mongo.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.example.mongo.util.MongoDb;
import com.example.mongo.util.TestData;
import com.example.mongo.util.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Aggregation {

	public static void main(String[] args) {
		TestData.countriesTestData();

		DB db = MongoDb.getDb();
		DBCollection countries = db.getCollection("countries");

		System.out.println("\nNumber of countries: " + countries.count());

		System.out.println("\nNumber of large countries: "
				+ countries.count(new BasicDBObject("area", new BasicDBObject(
						"$gt", 130000))));

		// distinct
		System.out.println("\nContinents in use: "
				+ countries.distinct("continent.name"));

		// aggregation framework
		List<String> continentList = Arrays.asList(new String[] { "Africa",
				"Europe", "Asia" });
		DBObject match = new BasicDBObject("$match", new BasicDBObject(
				"continent.name", new BasicDBObject("$in", continentList)));

		DBObject projectFields = new BasicDBObject("continent.name", 1);
		projectFields.put("area", 1);
		projectFields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", projectFields);

		DBObject groupFields = new BasicDBObject("_id", "$continent.name");
		groupFields.put("average", new BasicDBObject("$avg", "$area"));
		DBObject group = new BasicDBObject("$group", groupFields);

		List<DBObject> pipeline = new ArrayList<DBObject>();
		pipeline.add(match);
		pipeline.add(project);
		pipeline.add(group);

		System.out.println("\nAggregation output ::");
		Util.printOutput(countries, pipeline);

		MongoDb.shutdownMongoDB();
	}
}