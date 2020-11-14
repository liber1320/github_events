CREATE TABLE IF NOT EXISTS actors (
	id int identity(0,1) PRIMARY KEY, 
	actor_id int8 NOT NULL, 
	actor_login varchar NOT NULL,
	UNIQUE(actor_id, actor_login)
);

CREATE TABLE IF NOT EXISTS repos (
	id int identity(0,1) PRIMARY KEY, 
	repo_id int8 NOT NULL, 
	repo_name varchar NOT NULL,
	UNIQUE(repo_id, repo_name)
);

CREATE TABLE IF NOT EXISTS events_dict (
	id int identity(0,1) PRIMARY KEY, 
	event varchar NOT NULL, 
	UNIQUE(event)
);

CREATE TABLE IF NOT EXISTS events (
	id int identity(0,1) PRIMARY KEY, 
	actor_id int NOT NULL, 
	repo_id int NOT NULL,
	event_id int NOT NULL,
	time TIMESTAMP sortkey
);
