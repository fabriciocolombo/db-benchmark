package br.com.fabriciodev.benchmark.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import br.com.fabriciodev.benchmark.Person;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.mapping.Mapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraConnection {
	public static String CLUSTER_NAME = "test-cluster";
	public static String KEY_SPACE = "LearningKeyspace";
	public static String COLUMN_FAMILY_NAME = "person";

	public static ColumnFamily<Long, String> CF_DATA = ColumnFamily.newColumnFamily(COLUMN_FAMILY_NAME,
			LongSerializer.get(), StringSerializer.get());

	private static AstyanaxContext<Cluster> clusterContext;
	private static Cluster cluster;
	private static Keyspace keySpace;
	private static Mapping<Person> mapper;

	static {
		safeInit();
	}

	public Keyspace getKeySpace() {
		if (clusterContext == null) {
			safeInit();
		}

		return keySpace;
	}

	public Mapping<Person> getMapper() {
		return mapper;
	}

	private static void safeInit() {
		try {
			init();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	private static void init() throws ConnectionException {
		clusterContext = new AstyanaxContext.Builder()
				.forCluster(CLUSTER_NAME)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl(CLUSTER_NAME).setMaxConnsPerHost(1).setSeeds(
								"localhost:9160")).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());
		clusterContext.start();

		cluster = clusterContext.getEntity();
		// cluster.dropKeyspace(KEY_SPACE);

		mapper = Mapping.make(Person.class);

		List<KeyspaceDefinition> keySpaces = cluster.describeKeyspaces();

		boolean needsCreate = true;
		for (KeyspaceDefinition keyspaceDefinition : keySpaces) {
			if (keyspaceDefinition.getName().equals(KEY_SPACE)) {
				needsCreate = false;
				break;
			}
		}

		if (needsCreate) {
			createSchemaDefinition(cluster);
		}
		keySpace = cluster.getKeyspace(KEY_SPACE);

		keySpace.truncateColumnFamily(CF_DATA);
	}

	private static void createSchemaDefinition(Cluster cluster) throws ConnectionException {
		Map<String, String> stratOptions = new HashMap<String, String>();
		stratOptions.put("replication_factor", "1");

		KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();
		ksDef.setName(KEY_SPACE).setStrategyOptions(stratOptions).setStrategyClass("SimpleStrategy");

		ColumnFamilyDefinition cfDef = cluster.makeColumnFamilyDefinition().setName(CF_DATA.getName())
				.setComparatorType("UTF8Type").setKeyValidationClass("LongType");

		// cfDef.addColumnDefinition(cluster.makeColumnDefinition().setName("status").setValidationClass("UTF8Type")
		// .setIndex("test_status_index", "KEYS"));
		// cfDef.addColumnDefinition(cluster.makeColumnDefinition().setName("dateCreated").setValidationClass("LongType")
		// .setIndex("test_dateCreated_index", "KEYS"));
		//
		ksDef.addColumnFamily(cfDef);

		cluster.addKeyspace(ksDef);
	}

}
