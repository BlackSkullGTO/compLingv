from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk import FreqDist, classify, NaiveBayesClassifier
from pymongo import MongoClient, ASCENDING, DESCENDING
import re, string, random, csv

def remove_noise(tweet_tokens,stop_words=()):

	cleaned_tokens=[]

	for token, tag in pos_tag(tweet_tokens, lang='rus'):
		token=re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|"\"(?:%[0-9a-fA-F][0-9a-fA-F]))+','',token)
		token=re.sub("(@[A-Za-z0-9_]+)","",token)
		if tag.startswith("NN"):
			pos='n'
		elif tag.startswith('VB'):
			pos='v'
		else:
			pos='a'
		lemmatizer=WordNetLemmatizer()
		token=lemmatizer.lemmatize(token,pos)
		if len(token)>0 and token not in string.punctuation and token.lower() not in stop_words: 
			cleaned_tokens.append(token.lower())
	return cleaned_tokens

def get_all_words(cleaned_tokens_list):
	for tokens in cleaned_tokens_list:
		for token in tokens:
			yield token

def get_tweets_for_model(cleaned_tokens_list):
	for tweet_tokens in cleaned_tokens_list:
		yield dict([token,True]for token in tweet_tokens)

client = MongoClient()
database = client.news_database
sentences = database.sentences

stop_words=stopwords.words('russian')

positive_tweet_tokens = []
negative_tweet_tokens = []

with open('positive2.csv',encoding='utf-8',newline='') as csvfile:
   reader = csv.DictReader(csvfile, delimiter=';')
   i = 0
   for row in reader:
       i+=1
       if i == 20000:
            break
       tweet = row['tweet']
       tokens = word_tokenize(tweet)
       positive_tweet_tokens.append(tokens)

with open('negative2.csv',encoding='utf-8',newline='') as csvfile:
   reader = csv.DictReader(csvfile, delimiter=';')
   i = 0
   for row in reader:
       i+=1
       if i == 20000:
            break
       tweet = row['tweet']
       tokens = word_tokenize(tweet)
       negative_tweet_tokens.append(tokens)

positive_cleaned_tokens_list = []
negative_cleaned_tokens_list = []

for tokens in positive_tweet_tokens:
    positive_cleaned_tokens_list.append(remove_noise(tokens, stop_words))
for tokens in negative_tweet_tokens:
    negative_cleaned_tokens_list.append(remove_noise(tokens, stop_words))

all_pos_words = get_all_words(positive_cleaned_tokens_list)

freq_dist_pos=FreqDist(all_pos_words)
print(freq_dist_pos.most_common(10))

positive_tokens_for_model=get_tweets_for_model(positive_cleaned_tokens_list)
negative_tokens_for_model=get_tweets_for_model(negative_cleaned_tokens_list)

positive_dataset=[(tweet_dict,"Positive")for tweet_dict in positive_tokens_for_model]
negative_dataset=[(tweet_dict,"negative")for tweet_dict in negative_tokens_for_model]

dataset=positive_dataset+negative_dataset

random.shuffle(dataset)

train_data=dataset[:35000]
test_data=dataset[35001:]

classifier=NaiveBayesClassifier.train(train_data)
print("Accuracy is:", classify.accuracy(classifier, test_data))
print(classifier.show_most_informative_features(10))

sentenceTexts = sentences.find({})
f = open('output.txt', 'w')
i = 0
for sentenceText in sentenceTexts:
    i += 1
    custom_tweet=sentenceText['text']
    print(custom_tweet)
    custom_tokens = remove_noise(word_tokenize(custom_tweet))
    print(classifier.classify(dict([token,True]for token in custom_tokens)))
    _tonals = {
    "name": sentenceTexts['name'],
    "sentence": sentenceTexts['text'],
    "tonal": classifier.classify(dict([token,True]for token in custom_tokens))
    }
    f.write('name: ' + _tonals['name'] + ' sentence: ' + _tonals['sentene'] + ' tonal: ' + _tonals['tonal'] + '\n\n')
    tonals.insert_one(_tonals)
    print('\n\n\n')
    if i == 500:
        break
f.close()
