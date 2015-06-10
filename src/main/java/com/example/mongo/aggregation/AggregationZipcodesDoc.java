package com.example.mongo.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.example.mongo.util.MongoDb;
import com.example.mongo.util.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author MaheshJadhav
 * AggregationZipcodes.java
 * 
 * To import zipcodes use zips.json file from resources folder and use below command
 * \MongoDB\bin>mongoimport -d test -c zipcodes --file zips.json
 * 
 * Source: http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/
 */
public class AggregationZipcodesDoc {

	public static void main(String[] args) {
		MongoDatabase db = MongoDb.getTestMongoDb();
		MongoCollection<Document> zipcodes = db.getCollection("zipcodes");

		// count
		System.out.println("\nNumber of zipcodes: " + zipcodes.count());

		statesWithPopulationsOver10Million(zipcodes);
		averageCityPopulationByState(zipcodes);
		largestAndSmallestCitiesByState(zipcodes);
		
		MongoDb.shutdownMongoDB();
	}
	
	/*
	 [
	   { $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
	   { $match: { totalPop: { $gte: 10*1000*1000 } } }
	 ]
	 */
	private static void statesWithPopulationsOver10Million(MongoCollection<Document> zipcodes) {
		DBObject match = new BasicDBObject("$match", new BasicDBObject(
				"total_pop", new BasicDBObject("$gte", 10000000)));

		DBObject groupFields = new BasicDBObject("_id", "$state");
		groupFields.put("total_pop", new BasicDBObject("$sum", "$pop"));
		DBObject group = new BasicDBObject("$group", groupFields);

		List<DBObject> pipeline = new ArrayList<DBObject>();
		pipeline.add(group);
		pipeline.add(match);

		System.out
				.println("\nStatesWithPopulationsOver10Million Aggregation output ::");

		Util.printOutput(zipcodes, pipeline);
	}
	
	/*
	 [
   		{"$group" => {_id: {state: "$state", city: "$city"}, pop: {"$sum" => "$pop"}}},
	  	{"$group" => {_id: "$_id.state", avg_city_pop: {"$avg" => "$pop"}}},
	  	{"$sort" => {avg_city_pop: -1}},
	  	{"$limit" => 3}
	 ]
	 */
	private static void averageCityPopulationByState(MongoCollection<Document> zipcodes) {
		DBObject idFields = new BasicDBObject("state", "$state");
		idFields.put("city", "$city");
		DBObject groupFields1 = new BasicDBObject("_id", idFields);
		groupFields1.put("pop", new BasicDBObject("$sum", "$pop"));
		DBObject group1 = new BasicDBObject("$group", groupFields1);

		DBObject groupFields2 = new BasicDBObject("_id", "$_id.state");
		groupFields2.put("avg_city_pop", new BasicDBObject("$avg", "$pop"));
		DBObject group2 = new BasicDBObject("$group", groupFields2);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
				"avg_city_pop", -1));

		DBObject limit = new BasicDBObject("$limit", 3);

		List<DBObject> pipeline = new ArrayList<DBObject>();
		pipeline.add(group1);
		pipeline.add(group2);
		pipeline.add(sort);
		pipeline.add(limit);

		System.out
				.println("\nAverageCityPopulationByState Aggregation output ::");

		Util.printOutput(zipcodes, pipeline);
	}

	/*
	 [
	   { $group:
	      {
	        _id: { state: "$state", city: "$city" },
	        pop: { $sum: "$pop" }
	      }
	   },
	   { $sort: { pop: 1 } },
	   { $group:
	      {
	        _id : "$_id.state",
	        biggestCity:  { $last: "$_id.city" },
	        biggestPop:   { $last: "$pop" },
	        smallestCity: { $first: "$_id.city" },
	        smallestPop:  { $first: "$pop" }
	      }
	   },
	
	  // the following $project is optional, and
	  // modifies the output format.
	
	  { $project:
	    { _id: 0,
	      state: "$_id",
	      biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" },
	      smallestCity: { name: "$smallestCity", pop: "$smallestPop" }
	    }
	  }
	] 
	 */
	private static void largestAndSmallestCitiesByState(MongoCollection<Document> zipcodes) {
		DBObject idFields = new BasicDBObject("state", "$state");
		idFields.put("city", "$city");
		DBObject groupFields1 = new BasicDBObject("_id", idFields);
		groupFields1.put("pop", new BasicDBObject("$sum", "$pop"));
		DBObject group1 = new BasicDBObject("$group", groupFields1);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("pop", 1));

		DBObject groupFields2 = new BasicDBObject("_id", "$_id.state");
		groupFields2
				.put("biggestCity", new BasicDBObject("$last", "$_id.city"));
		groupFields2.put("biggestPop", new BasicDBObject("$last", "$pop"));
		groupFields2.put("smallestCity", new BasicDBObject("$first",
				"$_id.city"));
		groupFields2.put("smallestPop", new BasicDBObject("$first", "$pop"));
		DBObject group2 = new BasicDBObject("$group", groupFields2);

		DBObject projFields = new BasicDBObject("_id", 0);
		projFields.put("state", new BasicDBObject("state", "$_id"));
		DBObject biggestCityFields = new BasicDBObject("name", "$biggestCity");
		biggestCityFields.put("pop", "$biggestPop");
		projFields.put("biggestCity", biggestCityFields);
		DBObject smallestCityFields = new BasicDBObject("name", "$smallestCity");
		smallestCityFields.put("pop", "$smallestPop");
		projFields.put("smallestCity", smallestCityFields);
		DBObject project = new BasicDBObject("$project", projFields);

		List<DBObject> pipeline = new ArrayList<DBObject>();
		pipeline.add(group1);
		pipeline.add(sort);
		pipeline.add(group2);
		pipeline.add(project);

		System.out
				.println("\nLargestAndSmallestCitiesByState Aggregation output ::");

		Util.printOutput(zipcodes, pipeline);
	}
}
