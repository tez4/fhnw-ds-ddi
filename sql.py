import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# connect to postgres
conn = psycopg2.connect(host='localhost', user='postgres', password='postgres')
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# drop database if it exists
cur.execute("DROP DATABASE IF EXISTS blog;")

# create database
cur.execute("""
    CREATE DATABASE blog
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
    """)

# commit changes to database
conn.commit()

# close communication with the database
cur.close()
conn.close()

# connect to database
conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
cur = conn.cursor()

# create tables
cur.execute("DROP TABLE IF EXISTS public.test;")
cur.execute('CREATE TABLE IF NOT EXISTS public.test (data "varchar");')
cur.execute("INSERT INTO public.test (data) VALUES ('dff');")

# commit changes to database
conn.commit()

# close communication with the database
cur.close()
conn.close()
