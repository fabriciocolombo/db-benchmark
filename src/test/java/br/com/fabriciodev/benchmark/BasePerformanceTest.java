package br.com.fabriciodev.benchmark;

import java.util.List;

import org.junit.Before;

public class BasePerformanceTest {

	private List<Person> persons;

	public static int RECORD_COUNT = 100000;

	@Before
	public void setUp() {
		persons = new RepositoryPerson().load(RECORD_COUNT);
	}

	public List<Person> getPersons() {
		return persons;
	}

}
