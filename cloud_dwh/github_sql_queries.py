import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_events_dict_table_drop = "DROP TABLE IF EXISTS events_dict_staging"
actor_table_drop = "DROP TABLE IF EXISTS actors"
repo_table_drop = "DROP TABLE IF EXISTS repos"
event_table_drop = "DROP TABLE IF EXISTS events"
event_dict_table_drop = "DROP TABLE IF EXISTS events_dict"
staging_actor_table_drop = "DROP TABLE IF EXISTS actors_staging"
staging_repo_table_drop = "DROP TABLE IF EXISTS repos_staging"
staging_event_dict_table2_drop = "DROP TABLE IF EXISTS events_dict2_staging"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE events_staging 
(
created_at VARCHAR,
actor_id VARCHAR,
actor_login VARCHAR,
repo_id VARCHAR,
repo_name VARCHAR,
type VARCHAR
)
""")

staging_events_dict_table_create= ("""
CREATE TABLE events_dict_staging 
(
event VARCHAR
)
""")

staging_actor_table_create = ("""
CREATE TABLE actors_staging 
(
actor_id varchar, 
actor_login varchar
)
""")

staging_repo_table_create = ("""
CREATE TABLE repos_staging 
(
repo_id varchar, 
repo_name varchar
)
""")

staging_event_dict_table2_create = ("""
CREATE TABLE events_dict2_staging 
(
event varchar
)
""")

actor_table_create = ("""
CREATE TABLE actors 
(
id int identity(0,1) PRIMARY KEY, 
actor_id varchar, 
actor_login varchar,
UNIQUE(actor_id, actor_login)
)
""")

repo_table_create = ("""
CREATE TABLE repos 
(
id int identity(0,1) PRIMARY KEY, 
repo_id varchar, 
repo_name varchar,
UNIQUE(repo_id, repo_name)
)
""")

event_dict_table_create = ("""
CREATE TABLE events_dict 
(
id int identity(0,1) PRIMARY KEY, 
event varchar, 
UNIQUE(event)
)
""")

event_table_create = ("""
CREATE TABLE events 
(
id int identity(0,1) PRIMARY KEY, 
actor_id int, 
repo_id int,
event_id int,
time TIMESTAMP sortkey
)
""")

# STAGING COPIED TABLES
staging_events_copy = ("""
copy events_staging from {}
    iam_role '{}'
    format as PARQUET
""").format( config.get("S3", "EVENTS"), ARN)

staging_events_dict_copy = ("""
    copy events_dict_staging from {}
    iam_role '{}'
    format as CSV
""").format(config.get("S3", "EVENTS_DICT"), ARN)

# STAGING TABLES UPSERT
actor_staging_table_insert = ("""
insert into actors_staging
(select distinct
    actor_id,
    actor_login
from events_staging)
""")

actor_staging_table_delete = ("""
delete from actors_staging
using actors
where actors_staging.actor_id = actors.actor_id 
and actors_staging.actor_login = actors.actor_login
""")


repo_staging_table_insert = ("""
insert into repos_staging
(select distinct
    repo_id,
    repo_name
from events_staging)
""")

repo_staging_table_delete = ("""
delete from repos_staging
using repos
where repos_staging.repo_id = repos.repo_id 
and repos_staging.repo_name = repos.repo_name
""")

event_dict_staging_table_insert = ("""
insert into events_dict2_staging 
(select distinct
    event
from events_dict_staging)
""")

event_dict2_staging_table_delete = ("""
delete from events_dict2_staging
using events_dict
where events_dict2_staging.event = events_dict.event 
""")

# DWH TABLES
actor_table_insert = ("""
insert into actors (actor_id, actor_login)
(select distinct
    actor_id,
    actor_login
from actors_staging)
""")

repo_table_insert = ("""
insert into repos(repo_id, repo_name)
(select distinct
    repo_id,
    repo_name
from repos_staging)
""")

event_dict_table_insert = ("""
insert into events_dict (event)
(select distinct
    event
from events_dict2_staging)
""")

event_table_insert = ("""
insert into events 
(actor_id, repo_id, event_id, time)
    (select
        a.id,
        r.id,
        ed.id,
        TO_TIMESTAMP(e.created_at,'YYYY-MM-DDTHH:MI:SSZ')
    from events_staging e
    left join actors a 
        on e.actor_id = a.actor_id 
        and e.actor_login = a.actor_login
    left join repos r 
        on e.repo_id = r.repo_id 
        and e.repo_name = r.repo_name
    left join events_dict ed 
        on e.type = ed.event 
    )
""")

# QUERY LISTS
create_staging_table_queries = [staging_events_table_create, staging_events_dict_table_create, staging_actor_table_create, staging_repo_table_create, staging_event_dict_table2_create]
create_table_queries = [actor_table_create, repo_table_create, event_dict_table_create, event_table_create]

drop_staging_table_queries = [staging_events_table_drop, staging_events_dict_table_drop, staging_actor_table_drop, staging_repo_table_drop, staging_event_dict_table2_drop]
drop_table_queries = [ actor_table_drop, repo_table_drop, event_dict_table_drop, event_table_drop]

copy_table_queries = [staging_events_copy, staging_events_dict_copy]
insert_table_queries = [actor_table_insert, repo_table_insert, event_dict_table_insert, event_table_insert]

insert_staging_queries = [actor_staging_table_insert, repo_staging_table_insert, event_dict_staging_table_insert]
delete_staging_queries = [actor_staging_table_delete, repo_staging_table_delete, event_dict2_staging_table_delete]