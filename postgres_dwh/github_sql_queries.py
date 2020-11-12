# DROP TABLES
actor_table_drop = "DROP TABLE IF EXISTS actors"
repo_table_drop = "DROP TABLE IF EXISTS repos"
event_table_drop = "DROP TABLE IF EXISTS events"
event_dict_table_drop = "DROP TABLE IF EXISTS events_dict"

# CREATE TABLES
actor_table_create = ("CREATE TABLE IF NOT EXISTS actors \
                     (id SERIAL PRIMARY KEY, \
                      actor_id varchar NOT NULL,\
                      actor_login varchar NOT NULL,\
                      display_login varchar NOT NULL,\
                      DWH_START DATE NOT NULL, \
                      DWH_END DATE NOT NULL, \
                      UNIQUE(actor_id, actor_login, DWH_START))")

repo_table_create = ("CREATE TABLE IF NOT EXISTS repos \
                    (id SERIAL PRIMARY KEY, \
                    repo_id varchar NOT NULL,\
                    repo_name varchar NOT NULL,\
                    DWH_START DATE NOT NULL, \
                    DWH_END DATE NOT NULL, \
                    UNIQUE(repo_id, repo_name))")

event_dict_table_create = ("CREATE TABLE IF NOT EXISTS events_dict \
                    (id SERIAL PRIMARY KEY, \
                    event varchar NOT NULL,\
                    DWH_START DATE NOT NULL, \
                    DWH_END DATE NOT NULL, \
                    UNIQUE(event))")

event_table_create = ("CREATE TABLE IF NOT EXISTS events \
                    (id SERIAL PRIMARY KEY, \
                    actor_id int NOT NULL,\
                    repo_id int NOT NULL,\
                    event_id int NOT NULL,\
                    time timestamp NOT NULL,\
                    DWH_START DATE NOT NULL, \
                    DWH_END DATE NOT NULL, \
                    UNIQUE(actor_id, repo_id, time))")

# INSERT RECORDS
actor_table_insert = ("INSERT INTO actors \
                     (actor_id, actor_login, display_login, DWH_START, DWH_END) \
                      VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

repo_table_insert =  ("INSERT INTO repos \
                     (repo_id, repo_name, DWH_START, DWH_END) \
                      VALUES(%s, %s, %s, %s) ON CONFLICT DO NOTHING")

event_table_insert = ("INSERT INTO events \
                     (actor_id, repo_id, event_id, time, DWH_START, DWH_END) \
                      VALUES(%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

event_dict_table_insert = ("INSERT INTO events_dict \
                          (event, DWH_START, DWH_END) \
                           VALUES(%s, %s, %s) ON CONFLICT DO NOTHING")

# UPDATE actors
actor_table_update = ("UPDATE actors \
                       SET DWH_END = %s  \
                       WHERE actor_id=%s and actor_login=%s")

# FIND actor id
actor_select = ("SELECT id \
                 FROM actors \
                 WHERE actor_id=%s and actor_login=%s")

# FIND actor display login
actor_login_select = ("SELECT actor_id, actor_login, display_login \
                       FROM actors \
                       WHERE actor_id=%s and actor_login=%s")

# FIND repo id
repo_select = ("SELECT id \
                FROM repos \
                WHERE repo_id=%s and repo_name=%s")

# FIND event id
event_dict_select = ("SELECT id \
                      FROM events_dict \
                      WHERE event=%s")

# QUERY LISTS
create_table_queries = [actor_table_create, repo_table_create, event_table_create, event_dict_table_create]
drop_table_queries = [actor_table_drop, repo_table_drop, event_table_drop, event_dict_table_drop]