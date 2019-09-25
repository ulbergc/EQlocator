import os
import pymongo
from bson.json_util import dumps

client=pymongo.MongoClient('10.0.0.5', 27017)
change_stream = client.db0.foo.watch()
for change in change_stream:
	print(dumps(change))
	print('')
