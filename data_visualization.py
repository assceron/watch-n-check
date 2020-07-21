import csv
import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates
from IPython.display import display

import datetime

from matplotlib import ticker

SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIGGER_SIZE = 40
plt.rc('font', size=BIGGER_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=BIGGER_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=BIGGER_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=BIGGER_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=BIGGER_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=BIGGER_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

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


def plot_tweet_per_day(path, month=""):
    x = []
    y = []
    with open(path, "r") as csv_file:
        plots = csv.reader(csv_file, delimiter=',')
        for row in plots:
            keyword = row[0]
            x.append(row[1])
            y.append(int(row[2]))

        plt.plot(x, y, label=keyword)

        title = "Tweets per day in " + month
        plt.title(title)

        plt.xlabel('Day')
        plt.ylabel('#Tweets')
        plt.xticks(rotation=90)


def plot_tweet_per_day_combined(ax, keyword, csv_path, fig, separate):
    df = pd.read_csv(csv_path, parse_dates=['date'], index_col=['date'])
    df = df.sort_values(by='date')

    # Create figure and plot space
    # Add x-axis and y-axis
    ax.plot(df.index.values, df['num_tweets'], label=keyword)
    ax.legend(loc="upper left")

    # Define the date format
    date_form = mdates.DateFormatter("%b %Y")
    ax.xaxis.set_major_formatter(date_form)
    # Ensure a major tick for each month using (interval=1)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_minor_locator(mdates.DayLocator())

    # Set title and labels for axes
    ax.set(xlabel="Date",
           ylabel="Number of tweets",
           label=keyword,
           title="Tweets over time ")


def graph_tweets_per_day(path, separate, month, keyword_to_filter):
    if separate == "more_keywords":
        entry = keyword_to_filter + ".csv"
        if os.path.isfile(os.path.join(path, entry)):
            plot_tweet_per_day(os.path.join(path, entry))
            plt.legend()
            plt.show()
    else:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry), month):
                plot_tweet_per_day(os.path.join(path, entry))


def tweets_per_day(separate, keyword=""):
    path = "./results/tweets_per_day/"
    if os.path.exists(path):
        for dir in os.listdir(path):
            graph_tweets_per_day(path + dir, separate, dir, keyword)
            if separate == "more_keywords":
                plt.legend()
                plt.show()


def combined_tweets_per_day(keyword, separate="one_keyword"):
    path = "./results/tweets_per_day/combined/"

    if os.path.exists(path):
        fig = plt.figure(figsize=(40, 30))
        ax = fig.add_subplot()
        for files in os.listdir(path):
            if separate == 'one_keyword' and keyword == files.split(".")[0]:
                plot_tweet_per_day_combined(ax, keyword, path + files, fig, separate)

            if separate == "more_keywords":
                if "ipynb_checkpoints" in files:
                    continue
                plot_tweet_per_day_combined(ax, files.split(".")[0], path + files, fig, separate)
        plt.show()


def log_combined_tweets_per_day(keyword, separate="one_keyword"):
    path = "./results/tweets_per_day/combined/"

    if os.path.exists(path):
        fig = plt.figure(figsize=(40, 30))
        ax = fig.add_subplot()
        for files in os.listdir(path):
            if separate == 'one_keyword' and keyword == files.split(".")[0]:
                plot_tweet_per_day_combined(ax, keyword, path + files, fig, separate)

            if separate == "more_keywords":
                if "ipynb_checkpoints" in files:
                    continue
                plot_tweet_per_day_combined(ax, files.split(".")[0], path + files, fig, separate)
        plt.yscale('log')
        plt.show()


def compare_charts():
    tweets_per_day(0)

def plot_csv(filename, keyword, key, fontsize, figsize):
    # skiprows=1 will skip first line and try to read from second line
    df1 = pd.read_csv(filename)

    df1[key] = df1['key'].str.replace('\W', ' ')

    df1['frequency'] = pd.to_numeric(df1['value'])

    df1.astype({'frequency': 'int32'}).dtypes

    df1.plot(x=key, y='frequency', kind='barh', width=.9, figsize=figsize)
    plt.tick_params(axis='x', labelsize=fontsize)
    plt.tick_params(axis='y', labelsize=fontsize)
    title = "keyword: " + keyword + " " + get_selected_time(df1['year'].values[0], df1['month'].values[0], df1['day'].values[0], df1['hour'].values[0])
    plt.title(title, fontdict={'fontsize': 35})
    plt.show()


def occurent_word(keyword_filter):
    path = "./results/ngrams/"
    if os.path.exists(path):
        for dir in os.listdir(path):
            if dir == keyword_filter:
                plot_csv(path+dir+"/unigrams.csv", keyword_filter, "unigram",20, (11.7,10))
                plot_csv(path + dir + "/bigrams.csv",keyword_filter, "bi-gram", 30, (20, 15))
                plot_csv(path + dir + "/trigrams.csv", keyword_filter, "tri-gram", 35, (29, 25))


def sample_tweets(keyword_filter):
    path = "./results/sample_tweets/"
    if os.path.exists(path):
        for dir in os.listdir(path):
            dir_split = dir.split(".")
            if dir_split[0] == keyword_filter:
                pd.set_option('display.max_colwidth', None)
                df = pd.read_csv(os.path.join(path, dir), header=None, names=["user_id", "text", "created_at", "tweet_id"])
                display(df)


def show_period(year, month, day, hour):
    print("Selected period: ")
    if year != 'n':
        print(year)
    if month != 'n':
        print(month)
    if day != 'n':
        print(day)
    if hour != 'n':
        print(hour)

#if __name__ == '__main__':
    '''
    if os.path.exists("C:\master_thesis/results/ngrams"):
        for file in os.listdir("C:\master_thesis/results/ngrams"):
            if file.endswith(".csv"):
                plot_csv(os.path.join("C:\master_thesis/results", file))
    
     
    tweets_per_month("C:\master_thesis/results/tweets_per_month.csv")
    tweets_per_day("C:\master_thesis/results/tweets_per_day")
    compare_charts()
    #tweets_per_hour("C:\master_thesis/results/tweets_per_hour")
    '''
    # tweets_per_day("one_keyword", "bushfires")
    # tweet_month_by_month("results/tweets_per_day/Dec 2019/bush.csv")

#combined_tweets_per_day("", "more_keywords")
#log_combined_tweets_per_day("", "more_keywords")

#for file in os.listdir("C:\master_thesis/results/ngrams"):
#    occurent_word("bushfires")
#sample_tweets("bush")

#occurent_word("bushfires")