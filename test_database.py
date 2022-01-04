from create_database import *
import timeit

df_time = pd.DataFrame(columns=[
        'posts',
        'language',
        'query',
        'time'
    ])
create_database(100)

# mongodb queries -------------------------------------------------------------------------------------------------------------------

def get_mongodb_time(type_of_query, query, query_name, df_time, num_of_posts):
    client = MongoClient('mongodb+srv://mongodb:mongodb@cluster0.sahpd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    db = client['blog']
    blogposts = db['blogposts']

    # get the executeion time of the query
    if type_of_query == 'find':
        starttime = timeit.default_timer()
        cursor = blogposts.find(query)
        time = timeit.default_timer() - starttime
    elif type_of_query == 'aggregate':
        starttime = timeit.default_timer()
        cursor = blogposts.aggregate(query)
        time = timeit.default_timer() - starttime
    # cursor = blogposts.aggregate([{'$match' : {'post_id' : 42}}])

    # append time measurement to dataframe
    df_time = df_time.append({
                'posts': num_of_posts,
                'language': 'mongodb',
                'query': query_name,
                'time': time
            },
            ignore_index=True
        )

    return (cursor, df_time)

    # print(cursor)

    # for doc in cursor:
    #     print(doc)
    #     print("{title}: {url}".format(**doc))

cursor, df_time = get_mongodb_time('find',{'post_id' : 42}, 'one blogpost', df_time, 100)


# SQL queries -----------------------------------------------------------------------------------------------------------------------

def create_df(cur):
    results = cur.fetchall()
    df_results = pd.DataFrame.from_dict(results, orient= 'columns')
    column_names = [desc[0] for desc in cur.description]
    df_results = df_results.set_axis(column_names, axis=1)
    return df_results


def get_sql_time(query, query_name, df_time, num_of_posts):
    # connect to database
    conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
    cursor = conn.cursor()

    # create database
    starttime = timeit.default_timer()
    cursor.execute(query)
    time = timeit.default_timer() - starttime

    # append time measurement to dataframe
    df_time = df_time.append({
                'posts': num_of_posts,
                'language': 'sql',
                'query': query_name,
                'time': time
            },
            ignore_index=True
        )

    # df_results = create_df(cursor)

    # close communication with the database
    cursor.close()
    conn.close()

    return (cursor, df_time)


cursor, df_time = get_sql_time(
    """
    SELECT *
    FROM public.posts p
    LEFT JOIN public.comments c
    ON (p.post_id = c.post_id)
    LEFT JOIN public.tags t
    ON (p.post_id = t.post_id)
    WHERE p.post_id = 42
    """,
    'one blogpost',
    df_time,
    100
)

print(df_time)
