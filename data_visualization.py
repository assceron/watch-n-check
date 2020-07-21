import csv
import pandas as pd
import matplotlib.pyplot as plt
import os
import datetime

def get_selected_time(year, month, day, hour):
    time_string = " "
    if year != -1:
        time_string += "y:" + str(year) + " "
    if month != -1:
        time_string += "m:" + str(month) + " "
    if day != -1:
        time_string += "d:" + str(day) + " "
    if hour != -1:
        time_string += "h: " + str(hour)

    return time_string


def plot_csv(filename):
    # skiprows=1 will skip first line and try to read from second line
    df1 = pd.read_csv(filename)

    df1['key'] = df1['key'].str.replace('\W', ' ')

    df1['value'] = pd.to_numeric(df1['value'])

    df1.astype({'value': 'int32'}).dtypes

    df1.plot(x='key', y='value', kind='barh')
    plt.title(
        get_selected_time(df1['year'].values[0], df1['month'].values[0], df1['day'].values[0], df1['hour'].values[0]))
    plt.show()


def tweets_per_month(filename):
    x = []
    y = []
    keyword = ""
    with open(filename, 'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        for row in plots:
            if "keyword" in row[0]:
                split_list = row[0].split(":")
                keyword = split_list[1]
            else:
                x.append(row[0])
                y.append(int(row[1]))

    plt.bar(x, y, color='skyblue')

    title = "Keyword: " + keyword + "\n"
    title += "Tweets per month"
    plt.title(title)

    plt.xlabel('Month')
    plt.ylabel('#Tweets')
    plt.show()


def tweets_per_day(path):
    for entry in os.listdir(path):
        x = []
        y = []
        keyword = " "
        if os.path.isfile(os.path.join(path, entry)):
            with open(os.path.join(path, entry), "r") as csv_file:
                plots = csv.reader(csv_file, delimiter=',')
                month = ""
                for row in plots:
                    if len(row) == 1:  # Nov 2019
                        # start of a new month
                        if "keyword" in row[0]:
                            split_list = row[0].split(":")
                            keyword = split_list[1]
                        else:
                            month = row[0]

                    elif len(row) > 1:
                        x.append(row[0])
                        y.append(int(row[1]))

                plt.plot(x, y)

                title = "Keyword: " + keyword + "\n"
                title += "Tweets per per day in " + month
                plt.title(title)

                plt.xlabel('Day')
                plt.ylabel('#Tweets')
                plt.xticks(rotation=90)
                plt.show()


def tweets_per_hour(path):
    for subdir, dirs, files in os.walk(path):
        keyword = ""
        for file in files:
            x = []
            y = []
            if os.path.isfile(os.path.join(subdir, file)):
                with open(os.path.join(subdir, file), "r") as csv_file:
                    plots = csv.reader(csv_file, delimiter=',')
                    day = ""
                    for row in plots:
                        if len(row) == 1:  # Nov 2019
                            if "keyword" in row[0]:
                                split_list = row[0].split(":")
                                keyword = split_list[1]
                            else:
                                # start of a new month
                                day += row[0] + " "

                        elif len(row) > 1:
                            x.append(row[0])
                            y.append(int(row[1]))

                    print(day)
                    plt.plot(x, y)

                    title = "Keyword: " + keyword + "\n"
                    title += "Tweets per per hour in " + day
                    plt.title(title)

                    plt.xlabel('Hour')
                    plt.ylabel('#Tweets')
                    plt.xticks(rotation=90)
                    plt.show()


if __name__ == '__main__':
    if os.path.exists("C:\master_thesis/results/ngrams"):
        for file in os.listdir("C:\master_thesis/results/ngrams"):
            if file.endswith(".csv"):
                plot_csv(os.path.join("C:\master_thesis/results/ngrams", file))

    tweets_per_month("C:\master_thesis/results/tweets_per_month.csv")
    tweets_per_day("C:\master_thesis/results/tweets_per_day")
    tweets_per_hour("C:\master_thesis/results/tweets_per_hour")