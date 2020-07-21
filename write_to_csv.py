import collections
import os
import datetime
import pandas as pd
import numpy as np


def check_folder(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except OSError as e:  ## if failed, report it back to the user ##
        print("Error: %s - %s." % (e.filename, e.strerror))


def sorted_dict(path, filename, my_dict, keyword):

    with open(path + "/" + filename + ".csv", 'w', encoding="utf-8") as f:
        f.write("keyword:%s\n" % keyword)
        for key in sorted(my_dict, key=my_dict.get, reverse=True):
            if my_dict[key] != 0:
                f.write("%s,%s\n" % (key, my_dict[key]))

def total_tweets_per_day(path, d):
    try:
        new_path = path + "/total_tweets_per_day/"
        check_folder(new_path)
        for month, days in d.items():
            month_path = new_path + month
            print(month_path)
            #check_folder(month_path)
            with open(month_path + ".csv", 'w', encoding="utf-8") as f:
                days = collections.OrderedDict(sorted(days.items()))
                for day in days:
                    #format = '%a %b %d %H:%M:%S %z %Y'
                    #Dec 2019 01 Sun -> 01/12/2019
                    date = datetime.datetime.strptime(month + " " + day, '%b %Y %d %a').strftime('%m/%d/%Y')
                    f.write("%s,%s\n" % (date, d[month][day]))
    except OSError:
        print("Creation of the directory %s failed" % new_path)


def merge_pdf(path, keyword):
    # csv_to_return
    list_csv = []
    for subdir, dirs, files in os.walk(path):
        for file in files:
            if keyword == file.split(".")[0]:
                list_csv.append(pd.read_csv(os.path.join(subdir, file), header=0))
    combined_csv = pd.concat(list_csv)
    return combined_csv

def tweets_per_day(path, d, keyword):
    try:

        new_path = path + "/tweets_per_day/"
        check_folder(new_path)

        for month, days in d.items():
            month_path = new_path + month + "/"
            check_folder(month_path)

            with open(month_path + keyword + ".csv", 'w', encoding="utf-8") as f:
                #f.write("keyword: %s\n" % keyword)
                #f.write("%s\n" % month)
                f.write("keyword,date,num_tweets\n")
                days = collections.OrderedDict(sorted(days.items()))
                for day in days:
                    #format = '%a %b %d %H:%M:%S %z %Y'
                    #Dec 2019 01 Sun -> 01/12/2019
                    date = datetime.datetime.strptime(month + " " + day, '%b %Y %d %a').strftime('%m/%d/%Y')
                    f.write("%s,%s,%s\n" % (keyword, date, d[month][day]))

        merded_csv = merge_pdf(new_path, keyword)
        merged_path = new_path + "/combined/"
        check_folder(merged_path)
        merded_csv.to_csv(merged_path+keyword+".csv", index=False, encoding='utf-8-sig')

    except OSError:
        print("Creation of the directory %s failed" % new_path)


def tweets_per_hour(path, d, keyword):
    try:
        new_path = path + "/tweets_per_hour/"
        os.mkdir(new_path)
        for month, days in d.items():
            for day, hours in days.items():
                month_path = new_path + month
                if not os.path.exists(month_path):
                    os.mkdir(month_path)
                with open(month_path + "/" + day + ".csv", 'w', encoding="utf-8") as f:
                    f.write("keyword: %s\n" % keyword)
                    f.write("%s\n" % month)
                    f.write("%s\n" % day)
                    for hour in sorted(hours):
                        f.write("%s,%s\n" % (hour, d[month][day][hour]))

    except OSError:
        print("Creation of the directory %s failed" % new_path)

def list_to_csv(path, my_list, list_name, keyword, year, month, day, hour):
    try:
        new_path = path + "/ngrams/" + keyword
        check_folder(new_path)

        df = pd.DataFrame(my_list, columns=["key", "value"])
        df['year'] = year
        df['month'] = month
        df['day'] = day
        df['hour'] = hour

        df.to_csv(new_path + "/" + list_name, index=False)

    except OSError:
        print("Creation of the directory %s failed" % new_path)


def transform_date(date_string):
    split_date = date_string.split(" ")
    day = split_date[2]
    month = split_date[1]
    year = split_date[5]


def text_to_csv(path, my_list, list_name):
    local_path = path + "/sample_tweets/"
    check_folder(local_path)
    df = pd.DataFrame(my_list, columns=["user_id", "text", "created_at", "tweet_id"])
    df['label'] = " "
    df['comment'] = " "
    # get day in format (Sun Dec 22 06:53:54 +0000 2019 --> gg/mm/aaaa )
    df['created_at'] = pd.to_datetime(df['created_at'], format='%a %b %d %H:%M:%S %z %Y')

    size = min(5, df.size)
    if size > 0:
        replace = True  # with replacement
        fn = lambda obj: obj.loc[np.random.choice(obj.index, size, replace), :]
        df = df.sort_values(by='created_at').groupby(df["created_at"].dt.date, as_index=False).apply(fn)

        # dropping ALL duplicte values
        df.drop_duplicates(subset="text", inplace=True)
        df.to_csv(local_path + "/" + list_name + ".csv", encoding="utf-8-sig", index=False)
        print("File " + list_name + ".csv" + " created")
