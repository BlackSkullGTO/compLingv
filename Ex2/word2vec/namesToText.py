from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient()
database = client.news_database
sentences = database.sentences

f = open('names.txt', 'w')
textOld = ''
textNew = ''

for x in sentences.find( {} ).sort([('name', ASCENDING)]):
    textNew = x['name'].lower()
    if textOld != textNew:
        f.write(textNew + '\n')
        print('The name is added')
    textOld = textNew
    
f.close()
