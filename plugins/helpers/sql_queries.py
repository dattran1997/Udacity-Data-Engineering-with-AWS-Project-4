class SqlQueries:
    songplay_table_insert =  ("""
        INSERT INTO songplay (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        SELECT
            timestamp 'epoch' + e.ts/1000 * interval '1 second',
            CAST(e.user_id AS int),
            e.level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.location,
            e.user_agent
        FROM staging_events e
        JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
        WHERE e.page = 'NextSong';
    """)

    user_table_insert = ("""
        INSERT INTO users (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )
        SELECT
            DISTINCT user_id,
            first_name,
            last_name,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong' AND user_id IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT
            DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
        INSERT INTO artists (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
        SELECT
            DISTINCT artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT
            DISTINCT start_time,
            EXTRACT(hour from start_time),
            EXTRACT(day from start_time),
            EXTRACT(week from start_time),
            EXTRACT(month from start_time),
            EXTRACT(year from start_time),
            TO_CHAR(start_time, 'Day')
        FROM songplay
    """)