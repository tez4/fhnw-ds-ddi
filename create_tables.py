import pandas as pd
from faker import Faker
from collections import defaultdict
import sqlite3
import random
from pymongo import MongoClient
import regex as re

conn = sqlite3.connect('test_database')
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS people (first_name text, last_name text, occupation text, dob date, country text)')
conn.commit()

fake = Faker()
fake_data = defaultdict(list)

data = []

j = 0
for i in range(10):
    post_id = i
    post_author = fake.user_name()
    post_title =  fake.text(25)
    post_description = fake.text(75)
    post_url =  fake.url()
    post_likes = random.randint(0,50000)
    
    tags = []
    possible_tags = ['#' + i for i in re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<>?\n\t\s]', post_title) if len(i) >= 1]
    for i in range(random.randint(0, len(possible_tags))):
        tags.append(possible_tags[i])

        # create sql tag df


    comments = []
    for i in range(random.randint(0,21)):
        j += 1
        comment_id = j
        comment_text = fake.text(random.randint(5,50))

        comments.append({
            'comment_id': comment_id,
            'comment_text': comment_text,
        })

        # create sql comment df


        
    # create the json
    data.append({

    })

    if len(tags) > 0:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'by_user': post_author,
            'comments': comments,
            'tags': tags,
        })
    else:
        data.append({
            '_id': post_id,
            'title': post_title,
            'description': post_description,
            'url': post_url,
            'likes': post_likes,
            'by_user': post_author,
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
dict_fake_data = df_fake_data.to_dict('records')
print(df_fake_data.head())
# print(dict_fake_data)

# create SQL database ---------------------------------------------------------------------------------
# df_fake_data.to_sql('people', conn, if_exists='replace', index = False)

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