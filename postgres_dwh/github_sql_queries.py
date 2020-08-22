# DROP TABLES

actor_table_drop = "DROP TABLE IF EXISTS actors"
repo_table_drop = "DROP TABLE IF EXISTS repos"
event_table_drop = "DROP TABLE IF EXISTS events"
event_dict_table_drop = "DROP TABLE IF EXISTS events_dict"

# CREATE TABLES
actor_table_create = ("CREATE TABLE IF NOT EXISTS actors (id SERIAL PRIMARY KEY, actor_id varchar, actor_login varchar, UNIQUE(actor_id, actor_login))")

repo_table_create = ("CREATE TABLE IF NOT EXISTS repos (id SERIAL PRIMARY KEY, repo_id varchar, repo_name varchar, UNIQUE(repo_id, repo_name))")

event_dict_table_create = ("CREATE TABLE IF NOT EXISTS events_dict (id SERIAL PRIMARY KEY, event varchar, UNIQUE(event))")

event_table_create = ("CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, actor_id int, repo_id int, event_id int, time timestamp)")

# INSERT RECORDS
actor_table_insert = ("INSERT INTO actors (actor_id, actor_login ) \
                       VALUES(%s, %s) ON CONFLICT DO NOTHING")

repo_table_insert =  ("INSERT INTO repos (repo_id, repo_name) \
                       VALUES(%s, %s) ON CONFLICT DO NOTHING")

event_table_insert = ("INSERT INTO events (actor_id, repo_id, event_id, time) \
                       VALUES(%s, %s, %s, %s) ON CONFLICT DO NOTHING")

event_dict_table_insert = ("INSERT INTO events_dict (event) \
                            VALUES(%s) ON CONFLICT DO NOTHING")

# FIND actor
actor_select = ("SELECT id FROM actors WHERE actor_id=%s and actor_login=%s")

# FIND repo
repo_select = ("SELECT id FROM repos WHERE repo_id=%s and repo_name=%s")

# FIND event
event_dict_select = ("SELECT id FROM events_dict WHERE event=%s")

# QUERY LISTS
create_table_queries = [actor_table_create, repo_table_create, event_table_create, event_dict_table_create]
drop_table_queries = [actor_table_drop, repo_table_drop, event_table_drop, event_dict_table_drop]