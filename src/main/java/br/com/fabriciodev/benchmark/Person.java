package br.com.fabriciodev.benchmark;

import java.util.Date;

import com.netflix.astyanax.mapping.Id;

public class Person {

	@Id
	private Long id;
	private String name;
	private Date birthDate;
	private Double salary;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Person [id=");
		builder.append(id);
		builder.append(", name=");
		builder.append(name);
		builder.append(", birthDate=");
		builder.append(birthDate);
		builder.append(", salary=");
		builder.append(salary);
		builder.append("]");
		return builder.toString();
	}
	
	public void setValues(String name, Date birthDate, Double salary){
		this.name = name;
		this.birthDate = birthDate;
		this.salary = salary;
	}

}
