# -*- coding: utf-8 -*-
'''
 _______________________________________
 | MACS 30123: Large Scale Computing    |
 | Assignment 2: mrjob                 |
 | Question 2                           |
 | Andrei Bartra                        |
 | May 2021                             |
 |______________________________________|

'''
#  ________________________________________
# |                                        |
# |               1: Settings              |
# |________________________________________|

#MRJob
from mrjob.job import MRJob
from mrjob.step import MRStep

#Utilities
import re
import pandas as pd

#Visualization
import matplotlib.pyplot as plt
import seaborn as sns

#Time Duration Utilities
import time 
from functools import wraps


# Globals

WORD_RE = re.compile(r"[\w]+")


#  ________________________________________
# |                                        |
# |             2: MRJob Class             |
# |________________________________________|

class MostUsedWord(MRJob):
    def mapper_get_words(self, _, row):
        book = row.split(',')
        for word in WORD_RE.findall(book[7]):
            yield(word.lower(), 1)

    def combiner_count_words(self, word, counts):
        yield(word, sum(counts))


    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)


    def reducer_find_top_10(self, _, wc_pairs):
        w_list = [wc for wc in wc_pairs]
        for _ in range(10):
            yield(w_list.pop(w_list.index(max(w_list))))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_10)
        ]


#  ________________________________________
# |                                        |
# |              3: Reporting              |
# |________________________________________|
def mrjob_report():
    df = pd.read_csv('mrjob_out.txt', sep='\t', header=None, names=['count', 'word'])
    sns.barplot(x="word", y="count", data=df, palette='mako')
    plt.subplots_adjust(bottom=0.3, top=0.8)
    plt.ylabel("Frequency", size=14)
    plt.xlabel("Words", size=14)
    plt.title("Finding Top 10 words with MRJob", size=18)
    plt.savefig("mrjob.png")


#  ________________________________________
# |                                        |
# |               4: Parser                |
# |________________________________________|

if __name__ == '__main__':
    MostUsedWord.run()