package br.com.fabriciodev.benchmark;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class PerformanceTestMongoDB extends BasePerformanceTest {

	@Test
	public void mongodb() throws UnknownHostException, MongoException {
		Mongo mongo = new Mongo();

		DB db = mongo.getDB("bench");

		DBCollection collection = db.getCollection("person");

		collection.drop();

		for (Person person : getPersons()) {
			BasicDBObject dbObject = new BasicDBObject();
			dbObject.put("id", person.getId());
			dbObject.put("name", person.getName());
			dbObject.put("birthDate", person.getBirthDate());
			dbObject.put("salary", person.getSalary());

			collection.insert(dbObject);
		}
	}

	@Test
	public void mongodbBatch() throws UnknownHostException, MongoException {
		Mongo mongo = new Mongo();

		DB db = mongo.getDB("bench");

		DBCollection collection = db.getCollection("person");

		collection.drop();

		List<DBObject> list = new ArrayList<DBObject>();
		for (Person person : getPersons()) {
			BasicDBObject dbObject = new BasicDBObject();
			dbObject.put("id", person.getId());
			dbObject.put("name", person.getName());
			dbObject.put("birthDate", person.getBirthDate());
			dbObject.put("salary", person.getSalary());
			
			list.add(dbObject);
		}

		collection.insert(list);
	}
}
