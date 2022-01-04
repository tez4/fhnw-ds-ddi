from create_database import *


create_database(100)

# mongodb queries -------------------------------------------------------------------------------------------------------------------
client = MongoClient('mongodb+srv://mongodb:mongodb@cluster0.sahpd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
db = client['blog']
blogposts = db['blogposts']


cursor = blogposts.find({'post_id' : 42})
# cursor = blogposts.aggregate([{'$match' : {'post_id' : 42}}])

print(cursor)

# for doc in cursor:
#     print(doc)
#     print("{title}: {url}".format(**doc))


# SQL queries -----------------------------------------------------------------------------------------------------------------------

def create_df(cur):
    results = cur.fetchall()
    df_results = pd.DataFrame.from_dict(results, orient= 'columns')
    column_names = [desc[0] for desc in cur.description]
    df_results = df_results.set_axis(column_names, axis=1)
    return df_results

# connect to database
conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
cur = conn.cursor()

# create database
cur.execute("""
        SELECT *
        FROM public.posts p
        LEFT JOIN public.comments c
        ON (p.post_id = c.post_id)
        LEFT JOIN public.tags t
        ON (p.post_id = t.post_id)
        WHERE p.post_id = 42
    """)
df_results = create_df(cur)

# close communication with the database
cur.close()
conn.close()
