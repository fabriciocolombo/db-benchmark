package br.com.fabriciodev.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

public class PerformanceTestJDBC extends BasePerformanceTest {

	private void runWithJDBC(String url, String userName, String password) {
		Connection connection;
		try {
			connection = DriverManager.getConnection(url, userName, password);

			connection.prepareStatement("delete from Person").execute();

			PreparedStatement statement = connection
					.prepareStatement("Insert into Person (id, name, birthdate, salary) values (?, ?, ?, ?)");

			connection.setAutoCommit(false);
			try {
				for (Person person : getPersons()) {
					statement.setLong(1, person.getId());
					statement.setString(2, person.getName());
					statement.setDate(3, new java.sql.Date(person
							.getBirthDate().getTime()));
					statement.setDouble(4, person.getSalary());
					statement.execute();
				}

				connection.commit();
			} catch (Exception e) {
				connection.rollback();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void runWithJDBCBatch(String url, String userName, String password) {
		Connection connection;
		try {
			connection = DriverManager.getConnection(url, userName, password);

			connection.prepareStatement("delete from Person").execute();

			PreparedStatement statement = connection
					.prepareStatement("Insert into Person (id, name, birthdate, salary) values (?, ?, ?, ?)");

			connection.setAutoCommit(false);
			try {
				for (Person person : getPersons()) {
					statement.setLong(1, person.getId());
					statement.setString(2, person.getName());
					statement.setDate(3, new java.sql.Date(person
							.getBirthDate().getTime()));
					statement.setDouble(4, person.getSalary());
					statement.addBatch();
				}
				statement.executeBatch();
				connection.commit();
			} catch (Exception e) {
				connection.rollback();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// @Test
	public void mySql() {
		runWithJDBC("jdbc:mysql://localhost/bench", "root", "123");
	}

	@Test
	public void postgresql() {
		runWithJDBC("jdbc:postgresql:bench", "postgres", "123");
	}

	@Test
	public void postgresqlBatch() {
		runWithJDBCBatch("jdbc:postgresql:bench", "postgres", "123");
	}

}
