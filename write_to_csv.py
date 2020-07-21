import collections
import os

import pandas as pd


def sorted_dict(path, filename, my_dict, keyword):
    with open(path + "/" + filename + ".csv", 'w', encoding="utf-8") as f:
        f.write("keyword:%s\n" % keyword)
        for key in sorted(my_dict, key=my_dict.get, reverse=True):
            if my_dict[key] != 0:
                f.write("%s,%s\n" % (key, my_dict[key]))


def tweets_per_day(path, d, keyword):
    try:
        new_path = path + "/tweets_per_day/"
        os.mkdir(new_path)
        for month, days in d.items():
            with open(new_path + month + ".csv", 'w', encoding="utf-8") as f:
                f.write("keyword: %s\n" % keyword)
                f.write("%s\n" % month)
                days = collections.OrderedDict(sorted(days.items()))
                for day in days:
                    f.write("%s,%s\n" % (day, d[month][day]))
    except OSError:
        print("Creation of the directory %s failed" % new_path)
    else:
        print("Successfully created the directory %s " % new_path)


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
    else:
        print("Successfully created the directory %s " % new_path)


def list_to_csv(path, my_list, list_name, year, month, day, hour):
    new_path = path + "/ngrams"
    if not os.path.exists(new_path):
        os.mkdir(new_path)

    df = pd.DataFrame(my_list, columns=["key", "value"])
    df['year'] = year
    df['month'] = month
    df['day'] = day
    df['hour'] = hour

    df.to_csv(new_path + "/" + list_name, index=False)
