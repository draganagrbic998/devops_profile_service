CREATE TABLE IF NOT EXISTS profiles (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	phone_number VARCHAR(255) NOT NULL,
	sex VARCHAR(255) NOT NULL,
	birth_date DATE NOT NULL,
	username VARCHAR(255) NOT NULL UNIQUE,
	biography VARCHAR(255) NOT NULL,
	private BOOLEAN NOT NULL,
	work_experiences JSON DEFAULT '[]',
	educations JSON DEFAULT '[]',
	skills JSON DEFAULT '[]',
	interests JSON DEFAULT '[]',
    connections JSON DEFAULT '[]',
    connection_requests JSON DEFAULT '[]',
    blocked_profiles JSON DEFAULT '[]',
    block_post_notifications BOOLEAN DEFAULT FALSE,
    block_message_notifications BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS notifications (
	id SERIAL PRIMARY KEY,
	recipient_id INT NOT NULL,
	message VARCHAR(255) NOT NULL
);
