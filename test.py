import json
import math
import mysql.connector
from _collections import defaultdict
import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import scipy.signal
from scipy.signal import savgol_filter

mydb = mysql.connector.connect(host="localhost",
                               user="root",
                               passwd="",
                               database="schema1",
                               use_pure=False)

sql = ""
start_date = datetime.datetime(2020, 3, 24, 22, 0, 0)
end_date = datetime.datetime(2020, 3, 25, 15, 0, 0)
interval = 30


def get_hashtag_discrete_graph(start_date, end_date, interval):
    time_dict = {}
    current_start_date = start_date
    current_end_date = current_start_date + datetime.timedelta(0, interval * 60)
    while current_start_date <= end_date:
        ht_dict = defaultdict(lambda: 0)

        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       passwd="",
                                       database="schema1",
                                       use_pure=False)
        mycursor = mydb.cursor()
        # sql ="select * from twitter_table_test where twitter_table_test.created_at between '2020-03-15 21:20:00' and '2020-03-15 21:40:00' "
        sql = "select * from twitter_table_test7 where twitter_table_test7.created_at between '" + current_start_date.strftime(
            "%Y-%m-%d %H:%M:%S") + "' and '" + current_end_date.strftime("%Y-%m-%d %H:%M:%S") + "'"
        mycursor.execute(sql)
        result = mycursor.fetchall()

        for row in result:
            # for each row in the result
            data = json.loads(row[2])
            hashtags = data["hashtags"]
            for ht in hashtags:
                ht_dict[ht] += 1

        time_dict[current_start_date] = ht_dict
        current_start_date = current_start_date + datetime.timedelta(0, interval * 60)
        current_end_date = current_start_date + datetime.timedelta(0, interval * 60)
    return time_dict


def sort_hashtags_dict(ht_dict):
    sht_sorted = {}

    for key in ht_dict:
        sht_sorted[key] = {k: v for k, v in sorted(dict(ht_dict[key]).items(), key=lambda item: item[1], reverse=True)}

    return sht_sorted


def calc_DF_IDF(sht_sorted):
    ht_apereance = {}

    for interval in sht_sorted:
        for ht in sht_sorted[interval]:
            if ht in ht_apereance:
                ht_apereance[ht] += sht_sorted[interval][ht]
            else:
                ht_apereance[ht] = sht_sorted[interval][ht]

    ht_apereance = {k: v for k, v in sorted(ht_apereance.items(), key=lambda item: item[1], reverse=True)}

    # filter, take only hashtags that apear minimum 10 times
    ht_to_delete = list()
    # ht_apereance
    for ht in ht_apereance:
        if ht_apereance[ht] <= 700:
            ht_to_delete.append((ht))

    for ht in ht_to_delete:
        del ht_apereance[ht]
    # sht_sorted
    for interval in sht_sorted:
        for ht in ht_to_delete:
            if ht in sht_sorted[interval]:
                del sht_sorted[interval][ht]

    # sum the total hashtags for the DF-IDF calculation
    total_ht = sum(ht_apereance.values())

    # normalize hashtags series by calculating DF-IDF
    ht_series = {}
    total_ht_interval = {}
    for ht in ht_apereance:
        ht_series[ht] = {}

        for interval in sht_sorted:
            if not interval in total_ht_interval:
                total_ht_interval[interval] = sum(sht_sorted[interval].values())

            if ht in sht_sorted[interval]:
                ht_series[ht][interval] = (sht_sorted[interval][ht] / total_ht_interval[interval]) * math.log10(
                    total_ht / ht_apereance[ht])
            else:
                ht_series[ht][interval] = 0

    return ht_series


def show_graph(ht_series):
    for ht in ht_series:
        # x axis times
        x = ht_series[ht].keys()
        # corresponding y axis amount
        y = ht_series[ht].values()

        # plotting the points
        plt.scatter(x, y, label=ht)
    # setting x and y axis range

    # x-axis label
    plt.xlabel('time')
    # frequency label
    plt.ylabel('num of tweets (normalized)')
    # plot title
    plt.title('Hashtags Spread')
    # showing legend
    plt.legend()

    # function to show the plot
    plt.show()


# get dictionary of hashtags and its amount within time interval
ht_dict = get_hashtag_discrete_graph(start_date, end_date, interval)

ht_dict_encode = {}

for t in ht_dict:
    ht_dict_encode[t.strftime("%Y-%m-%d %H:%M:%S")] = ht_dict[t]

with open('c:\\test\\data.txt', 'w') as outfile:
    json.dump(ht_dict_encode, outfile)

# Sorted hashtags that grouped by its appearance time
sht_sorted = sort_hashtags_dict(ht_dict)

# get hashtag normalized series
ht_series = calc_DF_IDF(sht_sorted)

show_graph(ht_series)

ht_smoothing = {}

for ht in ht_series:
    ht_smoothing[ht] = {}

    # x axis times
    x = list(ht_series[ht].keys())
    # corresponding y axis amount
    y = savgol_filter(list(ht_series[ht].values()), 7, 5)

    if (len(x)) != len(y):
        raise Exception("Error ")

    for i in range(len(x)):
        ht_smoothing[ht][x[i]] = y[i]

show_graph(ht_smoothing)
