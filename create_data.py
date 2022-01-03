import regex as re
import pandas as pd
import random
from faker import Faker

fake = Faker()

def create_data(post_number):

    # create dataframe columns
    df_tags = pd.DataFrame(columns=[
        'tag_id',
        'post_id',
        'tag'
    ])
    df_comments = pd.DataFrame(columns=[
            'comment_id',
            'post_id',
            'comment_author',
            'comment_author_email',
            'comment_date',
            'comment_text',
            'comment_likes'
        ])

    df_posts = pd.DataFrame(columns=[
            'post_id',
            'title',
            'description',
            'url',
            'likes',
            'author',
            'email',
            'date'
        ])

    j = 0
    t = 0
    post_documents = []
    for i in range(post_number):

        # create post fields
        post_id = i
        post_author = fake.user_name()
        post_author_email = fake.email()
        post_date = fake.date_time_this_decade()
        post_title =  fake.text(25)
        post_description = fake.text(75)
        post_url =  fake.url()
        post_likes = random.randint(0,50000)
        
        # create tag fields
        tags = []
        possible_tags = ['#' + k for k in re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<>?\n\t\s]', post_title) if len(k) >= 1]
        
        # create mongodb tag json
        for k in range(random.randint(0, len(possible_tags))):
            tags.append(possible_tags[k])

        # create sql tag df
        for tag in tags:
            df_tags = df_tags.append({'tag_id': t, 'post_id': i,'tag': tag}, ignore_index=True)
            t += 1

        # create comment fields
        comments = []
        for k in range(random.randint(0,21)):
            comment_id = j
            comment_author = fake.user_name()
            comment_author_email = fake.email()
            comment_date = fake.date_time_this_decade()
            comment_text = fake.text(random.randint(5,50))
            comment_likes = random.randint(0,5000)

            j += 1

            # create mongodb comment json
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

        # create the post json
        if len(tags) == 0 and len(comments) == 0:
            post_documents.append({
                'title': post_title,
                'description': post_description,
                'url': post_url,
                'likes': post_likes,
                'author': post_author,
                'email': post_author_email,
                'date': post_date,
            })
        elif len(comments) == 0:
            post_documents.append({
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
            post_documents.append({
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
            post_documents.append({
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

        # create sql post df
        df_posts = df_posts.append({
                'post_id': post_id,
                'title': post_title,
                'description': post_description,
                'url': post_url,
                'likes': post_likes,
                'author': post_author,
                'email': post_author_email,
                'date': post_date,
            },
            ignore_index=True
        )
    
    return (df_posts, df_tags, df_comments, post_documents)
