CREATE TABLE IF NOT EXISTS events_staging (
	created_at VARCHAR,
	actor_id int8,
	actor_login VARCHAR,
	repo_id int8,
	repo_name VARCHAR,
	type VARCHAR
);

CREATE TABLE IF NOT EXISTS events_dict_staging (
	event VARCHAR
);

CREATE TABLE IF NOT EXISTS actors_staging (
	actor_id int8, 
	actor_login varchar
);

CREATE TABLE IF NOT EXISTS repos_staging (
	repo_id int8, 
	repo_name varchar
);

CREATE TABLE IF NOT EXISTS events_dict2_staging (
	event varchar
);

