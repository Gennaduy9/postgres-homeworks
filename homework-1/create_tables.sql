CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar NOT NULL,
	last_name varchar NOT NULL,
	title varchar,
	birth_date varchar,
	notes varchar
);

CREATE TABLE customers
(
	customer_id varchar PRIMARY KEY,
	company_name varchar NOT NULL,
	contact_name varchar NOT NULL
);

CREATE TABLE orders
(
	order_id int PRIMARY KEY,
	customer_id varchar REFERENCES customers(customer_id) NOT NULL,
	employee_id int REFERENCES employees(employee_id) NOT NULL,
	order_date varchar,
	ship_city varchar
);