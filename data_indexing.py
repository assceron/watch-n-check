import datetime
import gzip
import json
import os
import time

from elasticsearch import Elasticsearch, helpers

rem_dir = '/research/remote/isar/petabyte/users/damiano/fake-news/twitter-data-2020/'

client = Elasticsearch("localhost:9200", timeout=30, max_retries=10, retry_on_timeout=True)

total_tweets = 0

def is_valid_tweet(doc):
    if "text" not in doc:
        return False
    if doc['lang'] == 'null' or doc['lang'] == 'None' or doc['lang'] != 'en':
        return False
    return True


# Create a function to see if the tweet is a retweet
def is_RT(tweet):
    if 'retweeted_status' not in tweet:
        return False
    else:
        return True


# Create a function to see if the tweet is a reply to a tweet of #another user, if so return said user.
def is_Reply_to(tweet):
    if 'in_reply_to_screen_name' not in tweet:
        return False
    else:
        return tweet['in_reply_to_screen_name']


keys = ['text', 'user', 'created_at', 'source']


def filterKeys(tweet):
    new_tweet = {}
    for key in keys:
        if key not in tweet:
            continue
        new_tweet[key] = tweet[key]

    new_tweet["text"] = new_tweet["text"].lower()
    new_tweet["timestamp"] = time.mktime(
        datetime.datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y").timetuple())
    new_tweet["retweeted_status"] = is_RT(tweet)
    new_tweet["in_reply_to_screen_name"] = is_Reply_to(tweet)

    return new_tweet


def bulk_index(data, index, num):
    try:
        print("\nAttempting to index the list of docs using helpers.bulk() at num: " + str(num)
              + " at index " + str(index))
        # use the helpers library's Bulk API to index list of Elasticsearch docs
        resp = helpers.bulk(
            client,
            data,
            index=index,
            doc_type="_doc"
        )

        # print the response returned by Elasticsearch
        # print("helpers.bulk() RESPONSE:", resp)
        print("helpers.bulk() RESPONSE:", json.dumps(resp, indent=4))

    except Exception as err:
        # print any errors returned w
        ## Prerequisiteshile making the helpers.bulk() API call
        print("Elasticsearch helpers.bulk() ERROR:", err)
        quit()


def index_file(docs, index):
    # call the function to get the string data containing docs
    # docs = open(file, encoding="utf8", errors='ignore')
    global total_tweets
    # define an empty list for the Elasticsearch docs
    tweets_data = []
    for num, doc in enumerate(docs):
        try:
            # convert the string to a dict object
            tweet = json.loads(doc)
            if not is_valid_tweet(tweet):
                continue

            total_tweets += 1
            # filter the keys of interest
            tweet = filterKeys(tweet)

            # add a dict key called "_id" if you'd like to specify an ID for the doc
            tweet["_id"] = total_tweets

            # append the dict object to the list []
            tweets_data.append(tweet)

            # indexing every 10000 documents
            if num != 0 and num % 5000 == 0:
                # attempt to index the dictionary entries using the helpers.bulk() method
                bulk_index(tweets_data, index, num)
                # flush buffer
                tweets_data.clear()

        except ValueError as e:
            # it'll raise this exception in the end of the file
            print(e)

        except json.decoder.JSONDecodeError as err:
            # print the errors
            print("ERROR for num:", num, "-- JSONDecodeError:", err, "for doc:",
                  doc)

    # indexing of the last part of the file
    if tweets_data:
        bulk_index(tweets_data, index, num)


def get_index_name(filename):
    name = filename.split(".")
    full_date = name[2].split("-")
    year = full_date[0]
    month = full_date[1]

    return str(year) + "-" + str(month)


start = time.time()
for subdir, dirs, files in os.walk(rem_dir):
    print("SUBDIR: " + subdir)
    for file in files:
        print("INDEXING FILE: " + file)
        with gzip.open(os.path.join(subdir, file), 'rb') as s_file:
            # print(get_index_name(file))
            index_file(s_file, get_index_name(file))
            print("Total tweets:" + str(total_tweets))

end = time.time()
print("Time to perform the script: " + str(end - start))
