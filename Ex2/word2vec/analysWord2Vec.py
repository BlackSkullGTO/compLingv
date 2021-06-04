from pymongo import MongoClient, ASCENDING, DESCENDING
from pyspark.sql import SparkSession 
from pyspark.ml.feature import Tokenizer 
from pyspark.ml.feature import StopWordsRemover 
from pyspark.ml.feature import CountVectorizer 
from pyspark.ml.feature import IDF 
from pyspark.ml.feature import Word2Vec

client = MongoClient()
database = client.news_database
news = database.news
sentences = database.sentences

f = open('output.txt', 'r')

for line in f:    
    if line.find('id = ')>=0:
        sentenceId = line[line.find('id = ')+5:].rstrip()
        
        sentenceText = news.find_one({'_id': sentenceId})
        if sentenceText is None:
            print('Проблемы')
        else:
            sentences_ = {
            "_id":sentenceId,
            "text":sentenceText['headline']
            }
            if sentences.find_one({'_id': sentenceId}) is None:
                sentences.insert_one(sentences_)
                print('Added sentence with id = ' + sentenceId)
            else:
                print('Sentence with id = ' + sentenceId + ' is already exist')

f.close()
f = open('input.txt', 'w')

sentenceTexts = sentences.find({})
for sentenceText in sentenceTexts:
    f.write(sentenceText['text']+'\n')
    
f.close()

spark = SparkSession\
  .builder\
  .appName("SimpleApplication")\
  .getOrCreate()

# Построчная загрузка файла в RDD 
input_file = spark.sparkContext.textFile('input.txt') 

print(input_file.collect()) 
prepared = input_file.map(lambda x: ([x])) 
df = prepared.toDF() 
prepared_df = df.selectExpr('_1 as text') 

# Разбить на токены
tokenizer = Tokenizer(inputCol='text', outputCol='words') 
words = tokenizer.transform(prepared_df) 

# Удалить стоп-слова
stop_words = StopWordsRemover.loadDefaultStopWords('russian') 
remover = StopWordsRemover(inputCol='words', outputCol='filtered', stopWords=stop_words) 
filtered = remover.transform(words) 

# Вывести стоп-слова для русского языка
print(stop_words) 

# Вывести таблицу filtered 
filtered.show() 

# Вывести столбец таблицы words с токенами до удаления стоп-слов
words.select('words').show(truncate=False, vertical=True) 

# Вывести столбец "filtered" таблицы filtered с токенами после удаления стоп-слов
filtered.select('filtered').show(truncate=False, vertical=True) 

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

spark.stop()
