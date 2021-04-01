CREATE TABLE Business (
	business_id char(22),
	name VARCHAR NOT NULL,
	address VARCHAR NOT NULL,
	city VARCHAR NOT NULL,
	state VARCHAR NOT NULL,
	postal_code char(5) NOT NULL,
	latitude REAL,
	longitude REAL,
	is_open BOOLEAN NOT NULL,
	stars REAL NOT NULL,
	review_count INTEGER DEFAULT 0,
	num_tips INTEGER DEFAULT 0,
	num_checkins INTEGER DEFAULT 0,
	PRIMARY KEY(business_id)
);

CREATE TABLE UserTable (
	user_id char(22),
	name VARCHAR NOT NULL,
	average_stars REAL,
	latitude REAL,
	longitude REAL,
	cool INTEGER DEFAULT 0,
	fans INTEGER DEFAULT 0,
	funny INTEGER DEFAULT 0,
	tipcount INTEGER DEFAULT 0,
	total_likes INTEGER DEFAULT 0,
	useful INTEGER DEFAULT 0,
	yelping_since timestamp NOT NULL,
	PRIMARY KEY(user_id)
);

CREATE TABLE Tip (
	user_id char(22),
	business_id char(22),
	date TIMESTAMP,
	text VARCHAR,
	likes INTEGER NOT NULL,
	PRIMARY KEY(date, user_id, business_id),
	FOREIGN KEY(user_id) REFERENCES UserTable(user_id),
	FOREIGN KEY(business_id) REFERENCES Business(business_id)
);

CREATE TABLE CheckIn (
	business_id char(22),
	checkin_date TIMESTAMP,
	PRIMARY KEY(checkin_date,business_id),
	FOREIGN KEY(business_id) REFERENCES Business(business_id)
);

CREATE TABLE WeekHours (
	business_id char(22),
	day VARCHAR,
	hours VARCHAR, 
	PRIMARY KEY(business_id, day),
	FOREIGN KEY(business_id) REFERENCES Business(business_id)
);

CREATE TABLE BusinessCategories (
	business_id char(22),
	category VARCHAR,
	PRIMARY KEY(business_id, category),
	FOREIGN KEY(business_id) REFERENCES Business(business_id)
);

CREATE TABLE Friendship (
	first_user_id char(22),
	second_user_id char(22),
	PRIMARY KEY(first_user_id, second_user_id),
	FOREIGN KEY(first_user_id) REFERENCES UserTable (user_id),
	FOREIGN KEY(second_user_id) REFERENCES UserTable (user_id)
);

CREATE TABLE additional_attribute (
	business_id char(22),
	attribute_name VARCHAR,
	PRIMARY KEY(business_id, attribute_name),
	FOREIGN KEY(business_id) REFERENCES Business (business_id)
);
