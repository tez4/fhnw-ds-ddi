import regex as re
import pandas as pd
import random
import sqlite3
import psycopg2
from io import StringIO
from faker import Faker
from pymongo import MongoClient
from collections import defaultdict
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = sqlite3.connect('test_database')
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS people (first_name text, last_name text, occupation text, dob date, country text)')
conn.commit()

fake = Faker()
fake_data = defaultdict(list)

data = []

j = 0
t = 0

df_tags = pd.DataFrame(columns=['tag_id', 'post_id','tag'])
df_comments = pd.DataFrame(columns=[
        'comment_id', 'post_id', 'comment_author', 'comment_author_email', 'comment_date', 'comment_text', 'comment_likes'
    ])

for i in range(10):
    post_id = i
    post_author = fake.user_name()
    post_author_email = fake.email()
    post_date = fake.date_time_this_decade()
    post_title =  fake.text(25)
    post_description = fake.text(75)
    post_url =  fake.url()
    post_likes = random.randint(0,50000)
    
    tags = []
    possible_tags = ['#' + k for k in re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<>?\n\t\s]', post_title) if len(k) >= 1]
    for k in range(random.randint(0, len(possible_tags))):
        tags.append(possible_tags[k])

    # create sql tag df
    for tag in tags:
        df_tags = df_tags.append({'tag_id': t, 'post_id': i,'tag': tag}, ignore_index=True)
        t += 1

    

    comments = []
    for k in range(random.randint(0,21)):
        j += 1
        comment_id = j
        comment_author = fake.user_name()
        comment_author_email = fake.email()
        comment_date = fake.date_time_this_decade()
        comment_text = fake.text(random.randint(5,50))
        comment_likes = random.randint(0,5000)

        comments.append({
            'comment_id': comment_id,
            'comment_author': comment_author,
            'comment_author_email': comment_author_email,
            'comment_date': comment_date,
            'comment_text': comment_text,
            'comment_likes': comment_likes,
        })

        # create sql comment df
        df_comments = df_comments.append({
                'comment_id': comment_id,
                'post_id': i,
                'comment_author': comment_author,
                'comment_author_email': comment_author_email,
                'comment_date': comment_date,
                'comment_text': comment_text,
                'comment_likes': comment_likes
            },
            ignore_index=True
        )

        
    # create the json
    if len(tags) == 0 and len(comments) == 0:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'author': post_author,
            'email': post_author_email,
            'date': post_date,
        })
    elif len(comments) == 0:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'author': post_author,
            'email': post_author_email,
            'date': post_date,
            'tags': tags,
        })
    elif len(tags) == 0:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'author': post_author,
            'email': post_author_email,
            'date': post_date,
            'comments': comments,
        })
    else:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'author': post_author,
            'email': post_author_email,
            'date': post_date,
            'tags': tags,
            'comments': comments,
        })




    # create sql posts df
    fake_data["post_id"].append(i)
    fake_data["title"].append( fake.text(25) )
    fake_data["description"].append( fake.text(75) )
    fake_data["url"].append( fake.url() )
    fake_data["likes"].append(random.randint(1,50000))
    fake_data["by_user"].append( fake.user_name() )


print(data[1])


df_fake_data = pd.DataFrame(fake_data)

buffer = StringIO()
df_fake_data.to_csv(buffer, header=False)
buffer.seek(0)

dict_fake_data = df_fake_data.to_dict('records')
print(df_fake_data.head())
# print(dict_fake_data)

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
# df_fake_data.to_sql('people', conn, if_exists='replace', index = False)

cur.execute("DROP TABLE IF EXISTS public.test;")
cur.execute('CREATE TABLE IF NOT EXISTS public.test (_id "varchar",post_id "varchar",title "varchar",description "varchar",url "varchar",likes "varchar",by_user "varchar");')
cur.copy_from(buffer, 'public.test', sep=",")


# cur.execute("INSERT INTO public.test (data) VALUES ('dff');")

# commit changes to database
conn.commit()

# close communication with the database
cur.close()
conn.close()



# c.execute('''  
# SELECT * FROM people
#           ''')

# for row in c.fetchall():
#     print (row)



# create mongodb database ----------------------------------------------------------------------------
client = MongoClient('mongodb+srv://mongodb:mongodb@cluster0.sahpd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

db = client['blog']
blogposts = db['blogposts']

blogposts.insert_many(dict_fake_data)