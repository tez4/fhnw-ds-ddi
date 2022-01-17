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
    try:
        df_results = df_results.set_axis(column_names, axis=1)
    # this means that the dataframe is empty and it can't add any columns
    except ValueError:
        pass
    return df_results

def get_sql_time(queries, queries_params, query_name, df_time, num_of_posts, language):
    # connect to database
    conn = psycopg2.connect(dbname='blog', host='localhost', user='postgres', password='postgres')
    cursor = conn.cursor()

    # create database
    for i in range(len(queries)):
        starttime = timeit.default_timer()
        cursor.execute(queries[i], queries_params[i])
        time = timeit.default_timer() - starttime
        
        df_results = create_df(cursor)

        # append time measurement to dataframe
        df_time = df_time.append({
                    'posts': num_of_posts,
                    'language': language,
                    'query': query_name,
                    'time': time
                },
                ignore_index=True
            )

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

for post_number in [100,200,400,800,1600,3200,6400]:
    create_database(post_number)

    for j in range(20):
        random_post_ids = [random.randint(0,post_number) for i in range(5)]

        # query one blogpost
        for random_post_id in random_post_ids:
            df_results, df_time = get_sql_time(
                ["""
                SELECT *
                FROM public.posts p
                LEFT JOIN public.comments c
                ON (p.post_id = c.post_id)
                LEFT JOIN public.tags t
                ON (p.post_id = t.post_id)
                WHERE p.post_id = %s
                """],
                [(random_post_id,)],
                'one blogpost',
                df_time,
                post_number,
                'SQL with joins'
            )

            df_results, df_time = get_sql_time(
                ["""
                SELECT *
                FROM public.mv_one_blogpost
                WHERE post_id = %s
                """],
                [(random_post_id,)],
                'one blogpost',
                df_time,
                post_number,
                'SQL Materialized View'
            )
            
            df_results, df_time = get_sql_time(
                [
                    """
                    SELECT *
                    FROM public.posts
                    WHERE post_id = %s
                    """,
                    """
                    SELECT *
                    FROM public.comments
                    WHERE post_id = %s
                    """,
                    """
                    SELECT *
                    FROM public.tags
                    WHERE post_id = %s
                    """
                ],
                [
                    (random_post_id,),
                    (random_post_id,),
                    (random_post_id,)
                ],
                'one blogpost',
                df_time,
                post_number,
                'SQL without joins'
            )

            cursor, df_time = get_mongodb_time('find',{'post_id' : random_post_id}, 'one blogpost', df_time, post_number)

            # ids = []
            # for doc in cursor:
            #     ids.append(doc['post_id'])

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
                ["""
                SELECT *
                FROM public.tags t
                LEFT JOIN public.posts p
                ON (t.post_id = p.post_id)
                WHERE tag = %s
                """],
                [(tag,)],
                'by tag',
                df_time,
                post_number,
                'SQL'
            )

            df_results, df_time = get_sql_time(
                ["""
                SELECT *
                FROM public.mv_tags
                WHERE tag = %s
                """],
                [(tag,)],
                'by tag',
                df_time,
                post_number,
                'SQL Materialized View'
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

            df_results, df_time = get_sql_time(
                ["""
                SELECT *
                FROM public.comments c
                WHERE comment_author = %s
                """],
                [(author,)],
                'by comment author',
                df_time,
                post_number,
                'SQL'
            )

            df_results, df_time = get_sql_time(
                ["""
                SELECT *
                FROM public.mv_comments
                WHERE comment_author = %s
                """],
                [(author,)],
                'by comment author',
                df_time,
                post_number,
                'SQL with Index'
            )

            cursor, df_time = get_mongodb_time('find',{'comments.comment_author': author}, 'by comment author', df_time, post_number)


df_time.to_csv('output/time.csv', index=False)

print(df_time)
