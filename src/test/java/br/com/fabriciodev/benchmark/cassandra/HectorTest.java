package br.com.fabriciodev.benchmark.cassandra;

import java.util.Arrays;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;

import br.com.fabriciodev.benchmark.BasePerformanceTest;
import br.com.fabriciodev.benchmark.Person;

public class HectorTest extends BasePerformanceTest {

	private ThriftColumnFamilyTemplate<Long, String> template;

	// @Test
	public void insert() {
		ColumnFamilyUpdater<Long, String> insert = template.createUpdater();

		for (Person person : getPersons()) {

			insert.addKey(person.getId());
			insert.setLong("id", person.getId());
			insert.setString("name", person.getName());
			insert.setDate("birthDate", person.getBirthDate());
			insert.setDouble("salary", person.getSalary());

			template.update(insert);
		}
	}

	public void createSchema(Cluster cluster) {
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
				CassandraConnection.KEY_SPACE,
				CassandraConnection.COLUMN_FAMILY_NAME,
				ComparatorType.BYTESTYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
				CassandraConnection.KEY_SPACE, ThriftKsDef.DEF_STRATEGY_CLASS,
				1, Arrays.asList(cfDef));
		// Add the schema to the cluster.
		// "true" as the second param means that Hector will block until all
		// nodes see the change.
		cluster.addKeyspace(newKeyspace, true);
	}

	@Test
	public void insertBatch() {
		Mutator<Long> mutator = template.createMutator();

		for (Person person : getPersons()) {
			mutator.addInsertion(person.getId(),
					CassandraConnection.COLUMN_FAMILY_NAME,
					HFactory.createColumn("id", person.getId()));
			mutator.addInsertion(person.getId(),
					CassandraConnection.COLUMN_FAMILY_NAME,
					HFactory.createColumn("name", person.getName()));
			mutator.addInsertion(person.getId(),
					CassandraConnection.COLUMN_FAMILY_NAME,
					HFactory.createColumn("birthDate", person.getBirthDate()));
			mutator.addInsertion(person.getId(),
					CassandraConnection.COLUMN_FAMILY_NAME,
					HFactory.createColumn("salary", person.getSalary()));

			if (mutator.getPendingMutationCount() % 50000 == 0) {
				mutator.execute();
			}
		}

		mutator.execute();
	}

	@Before
	public void setUp() {
		super.setUp();

		Cluster cluster = HFactory.getOrCreateCluster(
				CassandraConnection.CLUSTER_NAME, "localhost:9160");

		KeyspaceDefinition keyspaceDef = cluster
				.describeKeyspace(CassandraConnection.KEY_SPACE);

		if (keyspaceDef != null) {
			cluster.truncate(CassandraConnection.KEY_SPACE,
					CassandraConnection.COLUMN_FAMILY_NAME);
		} else {
			createSchema(cluster);
		}

		Keyspace ksp = HFactory.createKeyspace(CassandraConnection.KEY_SPACE,
				cluster);

		template = new ThriftColumnFamilyTemplate<Long, String>(ksp,
				CassandraConnection.COLUMN_FAMILY_NAME, LongSerializer.get(),
				StringSerializer.get());
	}

}
