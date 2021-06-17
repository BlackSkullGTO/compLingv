import pymongo, os
from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient()
database = client.news_database
news = database.news
database.sentences.drop
sentences = database.sentences
os.system('cd /home/vagrant/tomita-parser/build/bin/')

for x in news.find( ):
    finput = open('input.txt', 'w')
    finput.write(x['text'])
    finput.close()
    
    os.system('./tomita-parser config.proto')
    
    foutput = open('output.txt', 'r').readlines()
        
    for j in range(len(foutput)):
        if foutput[j].find('Tomita_News') > -1:
            if len(foutput[j-1]) > 10:
                sentence = {
                "text": foutput[j-1],
                "name": foutput[j+2][9:]
                }
                sentences.insert_one(sentence)
                print('Sentence ' + sentence['text'])
                print('with name ' + sentence['name'] + '\n')
            
f = open('database.txt', 'w')
for x in sentences.find( {} ).sort([('_id', ASCENDING)]):
    f.write('Текст: ' + x['text'] + '; Имя: ' + x['name'])
f.close()
