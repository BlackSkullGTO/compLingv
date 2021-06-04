import requests, pymongo
from bs4 import BeautifulSoup
from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient()
database = client.news_database
news = database.news

f1 = open('input.txt', 'w')
f2 = open('database.txt', 'w')

url = 'https://bloknot-volgograd.ru/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')
bigline = soup.find('ul', class_='bigline')
newsLine = bigline.find_all('li')
headlines = bigline.find_all('a', class_='sys')
newsTimes = bigline.find_all('span', class_='botinfo')
tags = bigline.find_all('a', class_='cat')

for i in range(0, len(headlines)):
    headline = headlines[i].text #1
    
    site = url + headlines[i].get('href')[1:] #2
    
    newsText = newsLine[i].find('p').text #3
    
    newsTimes[i] = newsTimes[i].text
    spaceIndex = newsTimes[i].rfind(' ')
    newsTime = newsTimes[i][:spaceIndex].strip() #4
    comCount = newsTimes[i][spaceIndex+1:].strip() #5
    
    tag = tags[i].text #6   
    
    newsId = newsLine[i].get('id') #7 для сравнения новости
    newsId = newsId[newsId.rfind('_')+1:]
    
    news_ = {
    "_id":newsId,
    "headline":headline,
    "text":newsText,
    "site":site,
    "time":newsTime,
    "comment count":comCount,
    "tag":tag
    }

    if news.find_one({'_id': newsId}) is None:
        news.insert_one(news_)
        print('Added news with id = ' + newsId)
    else:
        news.update_one({'_id': newsId}, {'$set':{"time":newsTime, "comment count":comCount}})
        print('News with ID = ' + newsId + ' is updated')
    
for x in news.find( {} ).sort([('_id', ASCENDING)]):
    f2.write('ID: ' + x['_id'] + '; Заголовок: ' + x['headline'] + '; Текст: ' + x['text'] + '; Ссылка: ' + x['site'])
    f2.write('; Время: ' + x['time'] + '; Колличесвто комментариев: ' + x['comment count'] + '; Тэг: ' + x['tag'] + '.\n')
    f1.write(x['_id'] + '__' + x['headline'] + '.\n')
    
f1.close()
f2.close()
