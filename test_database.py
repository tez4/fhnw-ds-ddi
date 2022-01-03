from create_database import *

create_database(100)

client = MongoClient('mongodb+srv://mongodb:mongodb@cluster0.sahpd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
db = client['blog']
blogposts = db['blogposts']


cursor = blogposts.aggregate([{'$match' : {'post_id' : 42}}])

print(cursor)

for doc in cursor: 
    print("{title}: {url}".format(**doc))