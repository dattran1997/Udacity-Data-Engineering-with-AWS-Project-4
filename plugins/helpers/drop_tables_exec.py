drop_table_queries = [
    "DROP TABLE IF EXISTS staging_events;",
    "DROP TABLE IF EXISTS staging_songs;",
    "DROP TABLE IF EXISTS songplays;",
    "DROP TABLE IF EXISTS users;",
    "DROP TABLE IF EXISTS songs;",
    "DROP TABLE IF EXISTS artists;",
    "DROP TABLE IF EXISTS time;"
]

def drop_tables_func(redshift_hook):
    
    for query in drop_table_queries:
        redshift_hook.run(query)