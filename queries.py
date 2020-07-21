import datetime
import os
import shutil
import string
from elasticsearch import Elasticsearch, exceptions
import json
import time
import nltk
import write_to_csv as write
import data_visualization as visualize
from nltk.util import ngrams
from nltk.corpus import stopwords
from collections import Counter

stop_words = stopwords.words('english')

DOMAIN = "localhost"
PORT = 9200

path = "results/"
query_response = []
all_tweets = []
tweets_per_month = {}
tweets_per_location = {}
tweets_per_day = {}
tweets_per_user = {}
tweets_per_hour = {}
tweets_per_hour_per_day = {}
total_tweets_per_day = {}

def create_es_client():
    # concatenate a string for the client's host paramater
    host = str(DOMAIN) + ":" + str(PORT)

    # declare an instance of the Elasticsearch library
    client = Elasticsearch(host, timeout=5000, max_retries=1000, retry_on_timeout=True)

    try:
        # use the JSON library's dump() method for indentation
        info = json.dumps(client.info(), indent=4)

    except exceptions.ConnectionError as err:

        # print ConnectionError for Elasticsearch
        print("\nElasticsearch info() ERROR:", err)
        print("\nThe client host:", host, "is invalid or cluster is not running")

        # change the client's value to 'None' if ConnectionError
        client = None

    return client


def get_date(timestamp):
    # Sun Dec 01 08:56:40 +0000 2019
    date = {}
    t = timestamp.split(" ")
    date['day'] = t[0]
    date['month'] = t[1]
    date['day_n'] = t[2]
    date['hour'] = (t[3].split(":"))[0]
    date['year'] = t[5]

    return date


def process_hour(timestamp, date_dict):
    # Sun Dec 01 08:56:40 +0000 2019
    date = get_date(timestamp)
    key = date['month'] + " " + date['year']

    # tweets per day
    c_day = date['day_n'] + " " + date['day']
    if key in date_dict:
        if c_day in date_dict[key]:
            date_dict[key][c_day] += 1
        else:
            date_dict[key][c_day] = 1
    else:
        date_dict[key] = {c_day: 1}

    return date_dict


def process_date(hits):
    global total_tweets_per_day
    # Sun Dec 01 08:56:40 +0000 2019
    for item in hits:
        timestamp = item['_source']['created_at']
        process_hour(timestamp, total_tweets_per_day)

    #print(total_tweets_per_day)


def process_hits(hits, index):
    global tweets_per_day
    for item in hits:
        # print(json.dumps(item, indent=2))

        tweets_per_month[index] += 1

        user = item['_source']['user']
        username = user['screen_name']
        location = user['location']

        if username in tweets_per_user:
            tweets_per_user[username] += 1
        else:
            tweets_per_user[username] = 1

        if location in tweets_per_location:
            tweets_per_location[location] += 1
        else:
            tweets_per_location[location] = 1

        # TWEETS PER HOUR
        created_at = item['_source']['created_at']
        tweets_per_day = process_hour(created_at, tweets_per_day)

def get_text_and_date(hits):
    global all_tweets

    for item in hits:
        text_and_date = []
        user_id = item['_source']['user']['id_str']
        text = item['_source']['text']
        date = item['_source']['created_at']
        tweet_id = item['_id']

        text_and_date.append(user_id)
        text_and_date.append(text)
        text_and_date.append(date)
        text_and_date.append(tweet_id)

        all_tweets.append(text_and_date)


def search(client, search_body, query_num):
    global query_response
    global total_tweets_per_day

    # get all of the indices on the Elasticsearch cluster
    all_indices = client.indices.get_alias("*")

    for num, index in enumerate(all_indices):
        if "all_" in index:
            continue
        if "." in index:
            continue

        resp = client.search(
            index=index,
            # doc_type="_doc",
            scroll="30m",
            # search_type="scan",
            size=1000,
            body=search_body
        )

        scroll_id = resp['_scroll_id']

        scroll_size = len(resp['hits']['hits'])

        # Start scrolling
        while scroll_size > 0:
            "Scrolling..."
            # Before scroll, process current batch of hits

            if query_num == 1:
                process_hits(resp['hits']['hits'], index)

            if query_num == 2 or query_num == 3:
                get_text_and_date(resp['hits']['hits'])

            if query_num == 4:
                process_date(resp['hits']['hits'])

            resp = client.scroll(scroll_id=scroll_id, scroll='30m')

            query_response.append(resp['hits']['hits'])
            # Update the scroll ID
            scroll_id = resp['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(resp['hits']['hits'])


def filter_date_v2(client, query_num):
    search_body = {
        "query": {
            "match_all": {
            }
        }
    }

    search(client, search_body, query_num)


def filter_date(client, day, query_num):
    search_body = {
        "query": {
            "match_phrase": {
                "created_at": day
            }
        }
    }
    search(client, search_body, query_num)


def match_date(client, query_num):
    all_tweets_per_day = {}
    all_indices = client.indices.get_alias("*")
    for num, index in enumerate(all_indices):
        if "all_" in index:
            continue
        if "." in index:
            continue

        date_time_obj = datetime.datetime.strptime(index, '%Y-%m')
        month = date_time_obj.strftime("%b")
        for i in range(00, 31):
            day = month + " " + '%02d' % i
            print("Counting in " + day)
            filter_date(client, day, query_num)
            all_tweets_per_day[day] = len(query_response)
            print(all_tweets_per_day)
            query_response.clear()

    print(all_tweets_per_day)


def filter_keyword(client, keyword, query_num):
    search_body = {
        "query": {
            "match": {
                "text": {
                    "query": keyword
                }
            }
        }
    }
    search(client, search_body, query_num)


def filter_phrase(client, phrase, query_num):
    search_body = {
        "query": {
            "match_phrase": {
                "text": phrase
            }
        }
    }
    search(client, search_body, query_num)


def filter_list(client, keyword_list, query_num):
    keyword_list = keyword_list.replace(" ", " OR ")
    search_body = {
        "query": {
            "query_string": {
                "text": keyword_list
            }
        }
    }
    search(client, search_body, query_num)


def initialise_dicts(client):
    # get all of the indices on the Elasticsearch cluster
    global tweets_per_day
    global tweets_per_location
    global tweets_per_user
    global tweets_per_month

    all_indices = client.indices.get_alias("*")
    for num, index in enumerate(all_indices):
        tweets_per_month[index] = 0

    tweets_per_day.clear()
    tweets_per_user.clear()
    tweets_per_location.clear()


def analyse_keyword(client, keyword, query_num):
    k_list = keyword.split(" ")
    if len(k_list) > 1:
        # print("Filtering phrase")
        filter_phrase(client, keyword, query_num)
    else:
        # print("Filtering keyword")
        filter_keyword(client, keyword, query_num)


def term_occurence(all_tweets, keyword, year, month, day, hour):
    myfolder = "results/"

    all_unigrams = []
    all_bigrams = []
    all_trigrams = []

    n_words = 20

    for item in all_tweets:
        tweet = item[1]

        #tokenize tweet
        tokens = nltk.word_tokenize(tweet)

        # convert to lower case
        tokens = [w.lower() for w in tokens]

        # remove punctuation from each word
        table = str.maketrans('', '', string.punctuation)
        stripped = [w.translate(table) for w in tokens]

        # remove remaining tokens that are not alphabetic
        words = [word for word in stripped if word.isalpha()]

        # filter out stop words
        words = [w for w in words if not w in stop_words]

        no_words = ['https', 'rt']
        words = [w for w in words if not w in no_words]

        all_unigrams.extend(ngrams(words, 1))
        all_bigrams.extend(ngrams(words, 2))
        all_trigrams.extend(ngrams(words, 3))

    word_freq = Counter(all_unigrams)
    common_words = word_freq.most_common(n_words)
    filename = "unigrams" + ".csv"
    write.list_to_csv(myfolder, common_words, filename, keyword, year, month, day, hour)

    bigram_freq = Counter(all_bigrams)
    common_bigrams = bigram_freq.most_common(n_words)
    filename = "bigrams" + ".csv"
    write.list_to_csv(myfolder, common_bigrams, filename, keyword, year, month, day, hour)

    trigram_freq = Counter(all_trigrams)
    common_trigrams = trigram_freq.most_common(n_words)
    filename = "trigrams" + ".csv"
    write.list_to_csv(myfolder, common_trigrams, filename, keyword, year, month, day, hour)


def filter_by_date(tweets, year_to_filter, month_to_filter=-1, day_to_filter=-1, hour_to_filter=-1):
    filtered_tweets = []
    for item in tweets:
        date = get_date(item[2])
        year = date['year']
        month = date['month']
        day_n = date['day_n']
        hour = date['hour']

        y_append = 1
        m_append = 1
        d_append = 1
        h_append = 1

        if year_to_filter != -1 and year != year_to_filter:
            y_append = 0

        if month_to_filter != -1 and month != month_to_filter:
            m_append = 0

        if day_to_filter != -1 and day_n != day_to_filter:
            d_append = 0

        if hour_to_filter != -1 and hour != hour_to_filter:
            h_append = 0

        if y_append and m_append and d_append and h_append:
            filtered_tweets.append(item)

    return filtered_tweets


def term_occurence_over_time(keyword, year=-1, month=-1, day=-1, hour=-1):
    global all_tweets
    filtered_tweets = filter_by_date(all_tweets, year, month, day, hour)

    print("Total tweets in the selected period: " + str(len(filtered_tweets)))
    term_occurence(filtered_tweets, keyword, year, month, day, hour)


def remove(path):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        print("Dir not found")


def check_folder(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except OSError as e:  ## if failed, report it back to the user ##
        print("Error: %s - %s." % (e.filename, e.strerror))


def write_results(keyword):
    local_path = path
    check_folder(local_path)

    write.sorted_dict(local_path, "tweets_per_month", tweets_per_month, keyword)
    write.sorted_dict(local_path, "tweets_per_location", tweets_per_location, keyword)
    write.sorted_dict(local_path, "tweets_per user", tweets_per_user, keyword)
    write.tweets_per_day(local_path, tweets_per_day, keyword)
    # write.tweets_per_hour(myfolder, tweets_per_hour_per_day, keyword)


def check_year(year):
    if year == "n":
        return -1
    return year


def check_month(month):
    if month == "n":
        return -1
    return month


def check_day(day):
    if day == "n":
        return -1
    return day


def check_hour(hour):
    if hour == "n":
        return -1
    return hour


def initialise_tweets():
    global all_tweets
    all_tweets = []
    total_tweets_per_day.clear()

def query_analyse(keyword=None):
    client = create_es_client()

    if client is not None:
        initialise_dicts(client)
        start_time = time.time()
        analyse_keyword(client, keyword, 1)
        write_results(keyword)

        # visualize.tweets_per_day(0, keyword)
        end_time = time.time()
        # print("\n\n\n\nQuery duration: " + str(end_time - start_time) + " seconds")


def compare_keywords():
    visualize.visualize.tweets_per_day("more_keywords")


def query_occurence(keyword, year=-1, month=-1, day=-1, hour=-1):
    client = create_es_client()

    if client is not None:
        initialise_dicts(client)
        year = check_year(year)
        month = check_month(month)
        day = check_day(day)
        hour = check_hour(hour)
        initialise_tweets()
        analyse_keyword(client, keyword, 2)
        term_occurence_over_time(keyword, year, month, day, hour)


def query_get_text(keyword, year=-1, month=-1, day=-1, hour=-1):
    client = create_es_client()

    if client is not None:
        initialise_tweets()
        analyse_keyword(client, keyword, 3)
        year = check_year(year)
        month = check_month(month)
        day = check_day(day)
        hour = check_hour(hour)
        filtered_tweets = filter_by_date(all_tweets,year, month, day, hour)
        write.text_to_csv(path, filtered_tweets, keyword)


def query_get_all_tweets_per_day():
    client = create_es_client()

    if client is not None:
        initialise_tweets()
        start_time = time.time()
        filter_date_v2(client, 4)
        write.total_tweets_per_day(path, total_tweets_per_day)
        end_time = time.time()
        print("\n\n\n\nQuery duration: " + str(end_time - start_time) + " seconds")

