package com.example.mongo.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class TestData {

	public static void main(String[] args) {
		TestData.countriesTestData();
		TestData.olympicMedalsTestData();
		TestData.megaCitiesTestData();

		System.out.println("Successfuly loaded test data.");
	}

	public static void countriesTestData() {
		DB db = MongoDb.getDb();

		DBCollection continents = db.getCollection("continents");
		continents.drop();

		DBObject africa = (DBObject) JSON.parse("{_id : 1, name : 'Africa'}");
		DBObject asia = (DBObject) JSON.parse("{_id : 2, name : 'Asia'}");
		DBObject europe = (DBObject) JSON.parse("{_id : 3, name : 'Europe'}");
		DBObject northAmerica = (DBObject) JSON
				.parse("{_id : 4, name : 'North America'}");
		DBObject southAmerica = (DBObject) JSON
				.parse("{_id : 5, name : 'South America'}");
		DBObject australia = (DBObject) JSON
				.parse("{_id : 6, name : 'Australia'}");
		DBObject antarctica = (DBObject) JSON
				.parse("{_id : 7, name : 'Antarctica'}");

		continents.insert(africa);
		continents.insert(asia);
		continents.insert(europe);
		continents.insert(northAmerica);
		continents.insert(southAmerica);
		continents.insert(australia);
		continents.insert(antarctica);

		DBCollection countries = db.getCollection("countries");
		countries.drop();

		countries.insert(populateCountryInfo("Australia", 2966200, 21884000,
				australia));
		countries.insert(populateCountryInfo("Gabon", 103347, 1475000, africa));
		countries.insert(populateCountryInfo("Gambia", 4361, 1705000, africa));
		countries
				.insert(populateCountryInfo("Georgia", 26900, 4382100, europe));
		countries.insert(populateCountryInfo("Germany", 137847, 82046000,
				europe));
		countries.insert(populateCountryInfo("Ghana", 92098, 23837000, africa));
		countries
				.insert(populateCountryInfo("Greece", 50949, 11257285, europe));
		countries.insert(populateCountryInfo("Japan", 145925, 126659683, asia));
		countries.insert(populateCountryInfo("New Zealand", 104454, 4320300,
				australia));
		countries.insert(populateCountryInfo("Serbia", 34116, 7120666, europe));
		countries
				.insert(populateCountryInfo("Vietnam", 128565, 90388000, asia));

	}

	public static void olympicMedalsTestData() {

		DB db = MongoDb.getDb();

		DBCollection medals = db.getCollection("medals");
		medals.drop();

		medals.insert(populateCountryMedals("United States", 976, 758, 666));
		medals.insert(populateCountryMedals("Soviet Union", 395, 319, 296));
		medals.insert(populateCountryMedals("Great Britain", 236, 272, 272));
		medals.insert(populateCountryMedals("France", 202, 223, 246));
		medals.insert(populateCountryMedals("Germany", 174, 182, 217));
		medals.insert(populateCountryMedals("Italy", 198, 166, 185));
		medals.insert(populateCountryMedals("Sweden", 143, 164, 176));
		medals.insert(populateCountryMedals("East Germany", 153, 129, 127));
		medals.insert(populateCountryMedals("China", 201, 144, 128));
		medals.insert(populateCountryMedals("Russia", 133, 122, 142));
		medals.insert(populateCountryMedals("Hungary", 167, 144, 165));
		medals.insert(populateCountryMedals("Australia", 138, 153, 177));

	}

	public static void megaCitiesTestData() {

		DB db = MongoDb.getDb();

		DBCollection megaCities = db.getCollection("megacities");
		megaCities.drop();

		insertInMegacities(
				megaCities,
				"{name : 'Argentina', cities : [ {name : 'Buenos Aires', population : 14300000, growth : 1.00 }]}");
		insertInMegacities(
				megaCities,
				"{name : 'Bangladesh', cities : [ {name : 'Dhaka', population : 14000000, growth : 4.10}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Brazil', cities : [ {name : 'S?o Paulo', population : 21100000, growth : 1.40}, "
						+ "{name : 'Rio de Janeiro', population : 12700000}]}");
		insertInMegacities(
				megaCities,
				"{name : 'China', cities : [ {name : 'Shanghai', population : 25300000, growth : 2.20}, "
						+ "{name : 'Beijing', population : 16400000, growth : 2.70}, {name : 'Tianjin', "
						+ "population : 12938224}, {name : 'Guangzhou', population : 12700800, growth : 4.00}, "
						+ "{name : 'Shenzhen', population : 12506000}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Egypt', cities : [ {name : 'Cairo', population : 19439541, growth : 2.60}]}");
		insertInMegacities(
				megaCities,
				"{name : 'France', cities : [ {name : 'Paris', population : 10600000, growth : 1.00}]}");
		insertInMegacities(
				megaCities,
				"{name : 'India', cities : [ {name : 'Delhi', population : 23000000, growth : 4.60}, "
						+ "{name : 'Mumbai', population : 20800000, growth : 2.90}, {name : 'Kolkata', "
						+ "population : 15700000, growth : 2.00}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Indonesia', cities : [ {name : 'Jakarta', population : 28019545, growth : 2.20}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Iran', cities : [ {name : 'Tehran', population : 13500000, growth : 2.60}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Japan', cities : [ {name : 'Tokyo', population : 35682460, growth : 0.60}, "
						+ "{name : 'Osaka', population : 16800000, growth : 0.15}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Mexico', cities : [ {name : 'Mexico City', population : 23200000, growth : 2.00}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Nigeria', cities : [ {name : 'Lagos', population : 12700000, growth : 3.20}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Pakistan', cities : [ {name : 'Karachi', population : 23500000, growth : 4.90}, "
						+ "{name : 'Lahore', population : 12500000, growth : 2.00}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Philippines', cities : [ {name : 'Manila', population : 20700000, growth : 2.50}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Russia', cities : [ {name : 'Moscow', population : 16200000, growth : 0.20}]}");
		insertInMegacities(
				megaCities,
				"{name : 'South Korea', cities : [ {name : 'Seoul', population : 25600000, growth : 1.40}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Thailand', cities : [ {name : 'Bangkok', population : 14565547, growth : 0.90}]}");
		insertInMegacities(
				megaCities,
				"{name : 'Turkey', cities : [ {name : 'Istanbul', population : 13850000, growth : 2.80}]}");
		insertInMegacities(
				megaCities,
				"{name : 'United Kingdom', cities : [ {name : 'London', population : 12600000, growth : 0.70}]}");
		insertInMegacities(
				megaCities,
				"{name : 'United States', cities : [ {name : 'New York', population : 19831858, growth : 0.30}, "
						+ "{name : 'Los Angeles', population : 13052921, growth : 1.11}]}");
	}

	private static void insertInMegacities(DBCollection megaCities, String json) {
		megaCities.insert((DBObject) JSON.parse(json));
	}

	private static BasicDBObject populateCountryInfo(String name, int area,
			int population, DBObject continent) {
		BasicDBObject bdbo = new BasicDBObject("name", name)
				.append("area", area).append("population", population)
				.append("continent", continent);

		System.out.println(bdbo);
		return bdbo;
	}

	private static BasicDBObject populateCountryMedals(String name, int gold,
			int silver, int bronze) {
		BasicDBList medals = new BasicDBList();
		medals.add(bronze);
		medals.add(silver);
		medals.add(gold);
		return new BasicDBObject("name", name).append("medals", medals);
	}

}