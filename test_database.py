from create_database import *
import timeit

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
                'language': 'MongoDB',
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


# SQL queries -----------------------------------------------------------------------------------------------------------------------

def create_df(cur):
    results = cur.fetchall()
    df_results = pd.DataFrame.from_dict(results, orient= 'columns')
    column_names = [desc[0] for desc in cur.description]
    df_results = df_results.set_axis(column_names, axis=1)
    return df_results


def get_sql_time(query, query_params, query_name, df_time, num_of_posts):
    # connect to database
    conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
    cursor = conn.cursor()

    # create database
    starttime = timeit.default_timer()
    cursor.execute(query, query_params)
    time = timeit.default_timer() - starttime

    # append time measurement to dataframe
    df_time = df_time.append({
                'posts': num_of_posts,
                'language': 'SQL',
                'query': query_name,
                'time': time
            },
            ignore_index=True
        )

    df_results = create_df(cursor)

    # close communication with the database
    cursor.close()
    conn.close()

    return (df_results, df_time)

def run_sql_query(query):
    # connect to database
    conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
    cursor = conn.cursor()

    # create database
    cursor.execute(query)
    df_results = create_df(cursor)

    # close communication with the database
    cursor.close()
    conn.close()

    return (df_results)

# general logic -----------------------------------------------------------------------------------------------------------------------

df_time = pd.DataFrame(columns=[
        'posts',
        'language',
        'query',
        'time'
    ])

kk = [100,200,400,800,1600,3200,6400,12500,25000,50000,100000]

for post_number in [100,200,400,800,1600,3200]:
    create_database(post_number)

    random_post_ids = [random.randint(0,post_number) for i in range(5)]

    # query one blogpost
    for random_post_id in random_post_ids:
        df_results, df_time = get_sql_time(
            """
            SELECT *
            FROM public.posts p
            LEFT JOIN public.comments c
            ON (p.post_id = c.post_id)
            LEFT JOIN public.tags t
            ON (p.post_id = t.post_id)
            WHERE p.post_id = %s
            """,
            (random_post_id,),
            'one blogpost',
            df_time,
            post_number
        )

        cursor, df_time = get_mongodb_time('find',{'post_id' : random_post_id}, 'one blogpost', df_time, post_number)

    # query all blogposts of often used tag
    df_tags = run_sql_query(
        """
        SELECT COUNT(tag_id) count, tag
        FROM public.tags
        GROUP BY tag
        ORDER BY count DESC, tag
        LIMIT 5
        """)
    
    for tag in [tag for tag in df_tags['tag']]:
        
        df_results, df_time = get_sql_time(
            """
            SELECT *
            FROM public.tags t
            LEFT JOIN public.posts p
            ON (t.post_id = p.post_id)
            WHERE tag = %s
            """,
            (tag,),
            'by tag',
            df_time,
            post_number
        )

        cursor, df_time = get_mongodb_time('find',{'tags': {'$all': [tag]}}, 'by tag', df_time, post_number)
    
    # query all comments of a certain user
    df_comments = run_sql_query(
        """
        SELECT COUNT(comment_id) count, comment_author
        FROM public.comments
        GROUP BY comment_author
        ORDER BY count DESC, comment_author
        LIMIT 5
        """)
    
    for author in [author for author in df_comments['comment_author']]: 
        print(author)

        df_results, df_time = get_sql_time(
            """
            SELECT *
            FROM public.comments c
            WHERE comment_author = %s
            """,
            (author,),
            'by comment author',
            df_time,
            post_number
        )

        cursor, df_time = get_mongodb_time('find',{'comments.comment_author': author}, 'by comment author', df_time, post_number)


df_time.to_csv('time.csv', index=False)

print(df_time)
