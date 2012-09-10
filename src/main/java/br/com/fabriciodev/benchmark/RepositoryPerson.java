package br.com.fabriciodev.benchmark;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class RepositoryPerson {

	public List<Person> load(int limit) {
		List<Person> result = new ArrayList<Person>();

		for (int i = 1; i <= limit; i++) {
			result.add(createPerson(i));
		}
		return result;
	}

	private Person createPerson(Integer id) {
		Person person = new Person();

		person.setId(Long.valueOf(id));

		populate(person);

		return person;
	}

	private void populate(Person person) {
		if (person.getId() % 5 == 0) {
			person.setValues("Thomas", newDate(1980, 1, 15), 1500d);
		} else if (person.getId() % 4 == 0) {
			person.setValues("Mark", newDate(1985, 7, 10), 2000d);
		} else if (person.getId() % 3 == 0) {
			person.setValues("David", newDate(1990, 9, 20), 1200d);
		} else if (person.getId() % 2 == 0) {
			person.setValues("Michael", newDate(1987, 12, 21), 3300d);
		} else {
			person.setValues("Oliver", newDate(1988, 8, 18), 2700d);
		}
	}

	private Date newDate(int year, int month, int day) {
		Calendar date = Calendar.getInstance();

		date.set(year, month, day);

		return date.getTime();
	}
}
