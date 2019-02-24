import nltk
import re
import operator
import pandas as pd


"""
The script just 
"""

bigrams = []
df = pd.read_csv('data/shortjokes.csv', sep=',', header=0)
data = df["Joke"].to_list()
for content in data:
    words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
    for i in range(0, len(words) - 1):
        bigrams.append((words[i], words[i + 1]))
        bigrams.append((words[i], "*"))

count_all = 0
fdist = nltk.FreqDist(bigrams)
for k, v in fdist.items():
    if k[0] == 'my':
        if k[1] == "*":
            count_all = v
items = []
for k, v in fdist.items():
    if k[0] == 'my':
        items.append((k[0] + "-" + k[1], v / count_all))

sorted_bigram_frequencies = sorted(items, key=operator.itemgetter(1), reverse=True)

for i in sorted_bigram_frequencies:
    print(i)
