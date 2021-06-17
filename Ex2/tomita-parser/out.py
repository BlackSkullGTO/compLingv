from pymongo import MongoClient, ASCENDING, DESCENDING
client = MongoClient()
database = client.news_database
sentences = database.sentences
f = open('database.txt', 'w')
for x in sentences.find( {} ).sort([('_id', ASCENDING)]):
    f.write('Текст: ' + x['text'] + '; Имя: ' + x['name'])
f.close()
