from elasticsearch import Elasticsearch, exceptions
import json
import time
import pprint

DOMAIN = "localhost"
PORT = 9200

tweets = []
tweets_per_month = {}
tweets_per_location = {}
tweets_per_day = {}
tweets_per_user = {}
tweets_per_hour = {}
tweets_per_hour_per_day = {}


def create_es_client():
    # concatenate a string for the client's host paramater
    host = str(DOMAIN) + ":" + str(PORT)

    # declare an instance of the Elasticsearch library
    client = Elasticsearch(host, timeout=30, max_retries=10, retry_on_timeout=True)

    try:
        # use the JSON library's dump() method for indentation
        info = json.dumps(client.info(), indent=4)

        # pass client object to info() method
        # print("Elasticsearch client info():", info)

    except exceptions.ConnectionError as err:

        # print ConnectionError for Elasticsearch
        print("\nElasticsearch info() ERROR:", err)
        print("\nThe client host:", host, "is invalid or cluster is not running")

        # change the client's value to 'None' if ConnectionError
        client = None

    return client


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


# Create function for taking the most used Tweet sources off the #source column
def reckondevice(tweet):
    if 'iPhone' in tweet['source'] or ('iOS' in tweet['source']):
        return 'iPhone'
    elif 'Android' in tweet['source']:
        return 'Android'
    elif 'Mobile' in tweet['source'] or ('App' in tweet['source']):
        return 'Mobile device'
    elif 'Mac' in tweet['source']:
        return 'Mac'
    elif 'Windows' in tweet['source']:
        return 'Windows'
    elif 'Bot' in tweet['source']:
        return 'Bot'
    elif 'Web' in tweet['source']:
        return 'Web'
    elif 'Instagram' in tweet['source']:
        return 'Instagram'
    elif 'Blackberry' in tweet['source']:
        return 'Blackberry'
    elif 'iPad' in tweet['source']:
        return 'iPad'
    elif 'Foursquare' in tweet['source']:
        return 'Foursquare'
    else:
        return '-'


'''    
def analyse(client, keyword):
    # get all of the indices on the Elasticsearch cluster
    all_indices = client.indices.get_alias("*")

    # iterate over the list of Elasticsearch indices
    for num, index in enumerate(all_indices):
        response = text_filter_with_score(client, index, keyword)
        tweets_in_index = response['hits']['hits']

        # number of tweets per month
        tweets_per_month[index] = response['hits']['total']['value']

        for tweet in tweets_in_index:
            user = tweet['_source']['user']
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

    print(tweets_per_user, json.dumps(tweets_per_user, indent=4))
    print(tweets_per_location, json.dumps(tweets_per_location, indent=4))
        print(dict_giorno_ora_num)
        plt.bar(dict_giorno_ora_num.keys(), dict_giorno_ora_num.values(), color='y')
        plt.show()
    
    
        ## Convert ElasticSearch Response to pd data frame
        tweets = Select.from_dict(tweets_data).to_pandas()
    
        print(tweets.head)
        print("Tweets number: " + str(tweets.size))
        start = time.time()
    
        # See the percentage of tweets from the initial set that are #retweets:
        RT_tweets = tweets[tweets['retweeted_status'] == True]
        print("The percentage of retweets is" + str({round(len(RT_tweets) / len(tweets) * 100)}) + "% of all the tweets")
    
        end = time.time()
        print("Time to perform the query: " + str(end - start))
    '''


def process_hour(timestamp):
    # Sun Dec 01 08:56:40 +0000 2019
    t = timestamp.split(" ")
    day = t[0]
    month = t[1]
    day_n = t[2]
    hour = (t[3].split(":"))[0]
    year = t[5]

    key = month + " " + year

    # tweets per day
    c_day = day + " " + day_n
    if key in tweets_per_day:
        if c_day in tweets_per_day[key]:
            tweets_per_day[key][c_day] += 1
        else:
            tweets_per_day[key][c_day] = 1
    else:
        tweets_per_day[key] = {c_day: 1}

    # tweets per hour
    if key in tweets_per_hour:
        if hour in tweets_per_hour[key]:
            tweets_per_hour[key][hour] += 1
        else:
            tweets_per_hour[key][hour] = 1
    else:
        tweets_per_hour[key] = {hour: 1}

    '''
    #Save tweets per hours, per day
    c_day = day + " " + day_n
    if key not in tweets_per_hour_per_day:
        tweets_per_hour_per_day[key] = {c_day: {hour: 1}}
    else:
        if c_day not in tweets_per_hour_per_day[key]:
            tweets_per_hour_per_day[key] = {c_day: {hour: 1}}
        else:
            if hour not in tweets_per_hour_per_day[key][c_day]:
                tweets_per_hour_per_day[key] = {c_day: {hour: 1}}
            else:
                tweets_per_hour_per_day[key][c_day][hour] += 1
            
    print(tweets_per_hour_per_day)
    '''


def process_hits(hits, index):
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
        process_hour(created_at)


def search(client, search_body):
    # get all of the indices on the Elasticsearch cluster
    all_indices = client.indices.get_alias("*")

    for num, index in enumerate(all_indices):
        if "all_" in index:
            continue

        resp = client.search(
            index=index,
            # doc_type="_doc",
            scroll="2m",
            # search_type="scan",
            size=1000,
            body=search_body
        )
        scroll_id = resp['_scroll_id']

        scroll_size = len(resp['hits']['hits'])

        # print("Scroll size:" + str(scroll_size))

        # Start scrolling
        while scroll_size > 0:
            "Scrolling..."

            # Before scroll, process current batch of hits
            process_hits(resp['hits']['hits'], index)

            resp = client.scroll(scroll_id=scroll_id, scroll='2m')

            # Update the scroll ID
            scroll_id = resp['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(resp['hits']['hits'])


def match_all(client):
    search_body = {
        "query": {
            "match_all": {
            }
        }
    }
    search(client, search_body)


def filter_with_score(client, keyword):
    search_body = {
        "query": {
            "match": {
                "text": {
                    "query": keyword
                }
            }
        }
    }
    search(client, search_body)


def initialise_dicts(client):
    # get all of the indices on the Elasticsearch cluster
    all_indices = client.indices.get_alias("*")
    for num, index in enumerate(all_indices):
        tweets_per_month[index] = 0


def print_sorted_dict(d, limit=-1):
    count = 0
    if limit == -1:
        for k in sorted(d, key=d.get, reverse=True):
            if d[k] != 0:
                print(k, d[k])
    else:
        for k in sorted(d, key=d.get, reverse=True):
            count += 1
            if count > limit:
                break
            print(k, d[k])


def print_nested_dict(d):
    for month, hours in d.items():
        print("\n" + month)

        print(json.dumps(hours, indent=2, sort_keys=True))


def start_query(which_query, keyword=None):
    client = create_es_client()

    if client is not None:
        initialise_dicts(client)
        start_time = time.time()

        if which_query == "filter":
            filter_with_score(client, keyword)
            print("Month - #Tweets")
            print_sorted_dict(tweets_per_month)
            print("\n\n\n\nLocation - #Tweets")
            print_sorted_dict(tweets_per_location, 10)
            print("\n\n\n\n")
            print("User - #Tweets")
            print_sorted_dict(tweets_per_user, 10)

            print("\nDay - #Tweets")
            print_nested_dict(tweets_per_day)

            print("\nHour - #Tweets")
            print_nested_dict(tweets_per_hour)
            # print_nested_dict(tweets_per_hour_per_day)

        if which_query == "match all":
            match_all(client)

        end_time = time.time()
        print("\n\n\n\nQuery duration: " + str(end_time - start_time) + " seconds")
