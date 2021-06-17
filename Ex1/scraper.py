import requests, pymongo
from bs4 import BeautifulSoup
from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient()
database = client.news_database
news = database.news

f = open('database.txt', 'w')

url = 'https://bloknot-volgograd.ru/'


for i in range(1, 1000+1):
    url2 = url + '?PAGEN_1=' + str(i)
    response = requests.get(url2)
    soup = BeautifulSoup(response.text, 'lxml')
    bigline = soup.find('ul', class_='bigline')
    newsLine = bigline.find_all('li')
    headlines = bigline.find_all('a', class_='sys')
    newsTimes = bigline.find_all('span', class_='botinfo')
    tags = bigline.find_all('a', class_='cat')

    print(str(i) + ' / 1000')

    for j in range(0, len(headlines)):
        headline = headlines[j].text.strip() #1
        
        site = url + headlines[j].get('href')[1:] #2
        
        response2 = requests.get(site)
        soup2 = BeautifulSoup(response2.text, 'lxml')
        newsText = soup2.find('div', class_='news-text')
        newsText = newsText.text.replace('\n','')
        newsText = newsText.strip() #3
        
        newsTimes[j] = newsTimes[j].text
        spaceIndex = newsTimes[j].rfind(' ')
        newsTime = newsTimes[j][:spaceIndex].strip() #4
        comCount = newsTimes[j][spaceIndex+1:].strip() #5
        
        tag = tags[j].text #6   
        
        newsId = newsLine[j].get('id') #7 для сравнения новости
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
            news.update_one({'_id': newsId}, {'$set':{"time":newsTime, "comment count":comCount, "text":newsText}})
            print('News with ID = ' + newsId + ' is updated')
   
for x in news.find( {} ).sort([('_id', ASCENDING)]):
    f.write('ID: ' + x['_id'] + '; Заголовок: ' + x['headline'] + '; Текст: ' + x['text'] + '; Ссылка: ' + x['site'])
    f.write('; Время: ' + x['time'] + '; Колличество комментариев: ' + x['comment count'] + '; Тэг: ' + x['tag'] + '.\n\n')

f.close()
