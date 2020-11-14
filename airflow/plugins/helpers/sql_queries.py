class SqlQueries:
                actor_staging_table_insert = ("""
                        select distinct
                                actor_id,
                                actor_login
                        from events_staging
                """)

                repo_staging_table_insert = ("""
                        select distinct
                                repo_id,
                                repo_name
                        from events_staging
                """)

                event_dict_staging_table_insert = ("""
                        select distinct
                                event
                        from events_dict_staging
                """)

                event_table_insert = ("""
                insert into events (actor_id, repo_id, event_id, time)
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
                                        on e.type = ed.event )
                """)