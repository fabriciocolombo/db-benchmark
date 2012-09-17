package br.com.fabriciodev.benchmark;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
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

	@Test
	public void mongodbAsync() throws InterruptedException, ExecutionException {
		com.allanbank.mongodb.Mongo mongo = MongoFactory.create("mongodb://localhost:27017/db?maxConnectionCount=10");

		MongoDatabase db = mongo.getDatabase("bench");

		MongoCollection collection = db.getCollection("person");

		collection.drop();

		for (Person person : getPersons()) {
			Document document = BuilderFactory.start().add("id", person.getId()).add("name", person.getName())
					.add("birthDate", person.getBirthDate()).add("salary", person.getSalary()).build();

			collection.insertAsync(document);
		}
	}

	@Test
	public void mongodbBatchAsync() {
		com.allanbank.mongodb.Mongo mongo = MongoFactory.create("mongodb://localhost:27017/db?maxConnectionCount=10");

		MongoDatabase db = mongo.getDatabase("bench");

		MongoCollection collection = db.getCollection("person");

		collection.drop();

		List<Document> list = new ArrayList<Document>();

		for (Person person : getPersons()) {
			Document document = BuilderFactory.start().add("id", person.getId()).add("name", person.getName())
					.add("birthDate", person.getBirthDate()).add("salary", person.getSalary()).build();

			list.add(document);
		}

		collection.insertAsync(list.toArray(new DocumentAssignable[] {}));
	}
}
