import psycopg2
from io import StringIO
from pymongo import MongoClient
from create_data import *
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database(post_number):
    df_posts, df_tags, df_comments, post_documents = create_data(post_number)

    # create SQL database ---------------------------------------------------------------------------------

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
        TABLESPACE = pg_default
        CONNECTION LIMIT = -1;
        """)

    # commit changes to database and close communication with the database
    conn.commit()
    cur.close()
    conn.close()

    # connect to database
    conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
    cur = conn.cursor()

    # create tables
    buffer = StringIO()
    df_posts.to_csv(buffer, header=False, index = False)
    buffer.seek(0)

    cur.execute("DROP TABLE IF EXISTS public.posts;")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.posts 
            (
                post_id "numeric" PRIMARY KEY,
                title "varchar",
                description "varchar",
                url "varchar",
                likes "numeric",
                author "varchar",
                email "varchar",
                date "timestamp"
            );''')
    cur.copy_from(buffer, 'public.posts', sep=",")


    buffer = StringIO()
    df_comments.to_csv(buffer, header=False, index = False)
    buffer.seek(0)

    cur.execute("DROP TABLE IF EXISTS public.comments;")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.comments 
            (
                comment_id "numeric" PRIMARY KEY,
                post_id "numeric",
                comment_author "varchar",
                comment_author_email "varchar",
                comment_date "timestamp",
                comment_text "varchar",
                comment_likes "numeric",
                CONSTRAINT fk_post
                    FOREIGN KEY(post_id) 
                        REFERENCES public.posts(post_id)
            );''')
    cur.copy_from(buffer, 'public.comments', sep=",")
    
    
    buffer = StringIO()
    df_tags.to_csv(buffer, header=False, index = False)
    buffer.seek(0)

    cur.execute("DROP TABLE IF EXISTS public.tags;")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.tags 
            (
                tag_id "numeric" PRIMARY KEY,
                post_id "numeric",
                tag "varchar",
                CONSTRAINT fk_post
                    FOREIGN KEY(post_id) 
                        REFERENCES public.posts(post_id)
            );''')
    cur.copy_from(buffer, 'public.tags', sep=",")

    # create views
    cur.execute('''
        CREATE MATERIALIZED VIEW public.mv_one_blogpost
        AS
            SELECT p.post_id, p.title, p.description, p.url, p.likes, p.author, p.email, p.date,
			c.comment_author, c.comment_author_email, c.comment_date, c.comment_text, c.comment_likes,
			t.tag
            FROM public.posts p
            LEFT JOIN public.comments c
            ON (p.post_id = c.post_id)
            LEFT JOIN public.tags t
            ON (p.post_id = t.post_id)
        WITH DATA
    ;''')

    cur.execute('''
        CREATE MATERIALIZED VIEW public.mv_tags
        AS
            SELECT p.post_id, p.title, p.description, p.url, p.likes, p.author, p.email, p.date,
			t.tag
            FROM public.tags t
            LEFT JOIN public.posts p
            ON (t.post_id = p.post_id)
        WITH DATA
    ;''')

    # create indexes
    cur.execute('''
        CREATE INDEX idx_one_blogpost
        ON public.mv_one_blogpost USING btree
        (post_id DESC NULLS LAST)
    ;''')

    cur.execute('''
        CREATE INDEX idx_tags
        ON public.tags
        (tag DESC NULLS LAST)
    ;''')

    cur.execute('''
        CREATE INDEX idx_mv_tags
        ON public.mv_tags
        (tag DESC NULLS LAST)
    ;''')

    # commit changes to database and close communication with the database
    conn.commit()
    cur.close()
    conn.close()

    # create mongodb database ----------------------------------------------------------------------------
    client = MongoClient('mongodb+srv://mongodb:mongodb@cluster0.sahpd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

    db = client['blog']
    blogposts = db['blogposts']

    if blogposts.drop():
        print('Deleted')
    else:
        print('Not Present')

    blogposts.insert_many(post_documents)
