package br.com.fabriciodev.benchmark.cassandra;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Iterator;

import org.junit.Test;

import br.com.fabriciodev.benchmark.BasePerformanceTest;
import br.com.fabriciodev.benchmark.Person;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class AstyanaxTest extends BasePerformanceTest {

	private CassandraConnection cassandraConnection = new CassandraConnection();

	@Test
	public void insert() throws Exception {
		System.out.println("begin " + Calendar.getInstance().getTime());

		MutationBatch mutationBatch = cassandraConnection.getKeySpace().prepareMutationBatch();

		for (Person person : getPersons()) {
			ColumnListMutation<String> columnListMutation = mutationBatch.withRow(CassandraConnection.CF_DATA,
					person.getId());
			cassandraConnection.getMapper().fillMutation(person, columnListMutation);

			mutationBatch.execute();
		}

		System.out.println("end " + Calendar.getInstance().getTime());
	}

	@Test
	public void insertBatch() throws Exception {
		System.out.println("begin " + Calendar.getInstance().getTime());

		MutationBatch mutationBatch = cassandraConnection.getKeySpace().prepareMutationBatch();

		for (Person person : getPersons()) {
			ColumnListMutation<String> columnListMutation = mutationBatch.withRow(CassandraConnection.CF_DATA,
					person.getId());
			cassandraConnection.getMapper().fillMutation(person, columnListMutation);
		}

		mutationBatch.execute();

		System.out.println("end " + Calendar.getInstance().getTime());
	}
	
	@Test
	public void insertWithCheck() throws Exception {
		insertBatch();

		OperationResult<Rows<Long, String>> rows = cassandraConnection.getKeySpace()
				.prepareQuery(CassandraConnection.CF_DATA).getAllRows().execute();

		Iterator<Row<Long, String>> iterator = rows.getResult().iterator();

		int i = 0;
		while (iterator.hasNext()) {
			iterator.next();
			i++;
		}
		assertEquals(RECORD_COUNT, i);
	}
}
