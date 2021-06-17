from pymongo import MongoClient, ASCENDING, DESCENDING
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Word2Vec
from pymorphy2 import MorphAnalyzer
from nltk.corpus import stopwords
import re, string, datetime, pymorphy2

def remove_punctuation(text):

    """ 
    Удаление пунктуации из текста
    """ 
    return text.translate(str.maketrans('', '', string.punctuation)).lower()
    
def remove_linebreaks(text): 

    """ 
    Удаление разрыва строк из текста
    """ 
    return text.strip()

client = MongoClient()
database = client.news_database
news = database.news
sentences = database.sentences

patterns = "[A-Za-z0-9!#$%&'()*+,./:;<=>?@[\]^_`{|}~—\"\«»-]+"
stopwords_ru = stopwords.words("russian")
morph = MorphAnalyzer()

f = open('input.txt', 'w')
i = 0
for x in news.find( {} ):
    i += 1
    line = x['text']
    line = re.sub(patterns, ' ', line)
    line = re.sub(r'\n|\r|\t', ' ', line)
    line = re.sub(r'\s+', ' ', line)
    for token in line.split():
        if token and token not in stopwords_ru:
            token = token.strip()
            token = morph.normal_forms(token)[0]
            f.write(token+' ')
   
    if i == 2500:
        break
f.close()


spark = SparkSession.builder.appName("SimpleApplication").getOrCreate()

# Построчная загрузка файла в RDD
input_file = spark.sparkContext.textFile('input.txt')

print('Подготовка данных 1')

prepared = input_file.map(lambda x: (remove_punctuation(x))).map(lambda x: (remove_linebreaks(x)))
prepared = prepared.map(lambda x: ([x]))

prepared_df = prepared.toDF().selectExpr('_1 as text')

print('Подготовка данных 2')

# Разбить на токены
tokenizer = Tokenizer(inputCol='text', outputCol='words')
words = tokenizer.transform(prepared_df)
words.show()

# Удалить стоп-слова
stop_words = StopWordsRemover.loadDefaultStopWords('russian')
remover = StopWordsRemover(inputCol='words', outputCol='filtered', stopWords=stop_words)
filtered = remover.transform(words)

# Вывести стоп-слова для русского языка
#print(stop_words)

# Вывести таблицу filtered
#filtered.show()

# Вывести столбец таблицы words с токенами до удаления стоп-слов
#words.select('words').show(truncate=False, vertical=True)

# Вывести столбец "filtered" таблицы filtered с токенами после удаления стоп-слов
#filtered.select('filtered').show(truncate=False, vertical=True)

# Посчитать значения TF
vectorizer = CountVectorizer(inputCol='filtered', outputCol='raw_features').fit(filtered)
featurized_data = vectorizer.transform(filtered)
featurized_data.cache()
vocabulary = vectorizer.vocabulary

# Вывести таблицу со значениями частоты встречаемости термов.
featurized_data.show()

# Вывести столбец "raw_features" таблицы featurized_data
featurized_data.select('raw_features').show(truncate=False, vertical=True)

# Вывести список термов в словаре
print(vocabulary)


# Посчитать значения DF
idf = IDF(inputCol='raw_features', outputCol='features')
idf_model = idf.fit(featurized_data)
rescaled_data = idf_model.transform(featurized_data)

# Вывести таблицу rescaled_data
rescaled_data.show()

# Вывести столбец "features" таблицы featurized_data
rescaled_data.select('features').show(truncate=False, vertical=True)

# Построить модель Word2Vec
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol='words', outputCol='result')
model = word2Vec.fit(words)
w2v_df = model.transform(words)
w2v_df.show()

f = open('/home/vagrant/Cursovaya/Ex2/names.txt', 'r')
for line in f:
    #Ввод слова
    _words = line.replace("\n", "")
    _words = _words.replace(" ", "_")
    words = _words.split("_")
    for word in words:
    #Проверка на вхождение
        intext=False
        for list in rescaled_data.select('filtered').collect():
            if list[0].count(word)!=0:intext=True
    #Вывод
        if intext:
            print(_words)
            synonyms = model.findSynonyms(word, 10)
            synonyms.show()
        else:
            print(_words)
            print("Synonims is not found")
        print("\n\n\n\n\nEND")
f.close()

spark.stop()
