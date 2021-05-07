# -*- coding: utf-8 -*-
'''
 _______________________________________
 | MACS 30123: Large Scale Computing    |
 | Assignment 2: Pywren                 |
 | Question 1                           |
 | Andrei Bartra                        |
 | May 2021                             |
 |______________________________________|

'''
#  ________________________________________
# |                                        |
# |               1: Settings              |
# |________________________________________|

#PyWren
import pywren

#Utilities
import pandas as pd
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

#Visualization
import matplotlib.pyplot as plt
import seaborn as sns

#Time Duration Utilities
import time 
from functools import wraps


# Globals

URL = 'http://books.toscrape.com/'
BATCH_SIZES = [1, 2, 4, 8, 16, 32, 64]

#  ________________________________________
# |                                        |
# |            2: Time Decorator           |
# |________________________________________|

#Timer Decorator
# From https://stackoverflow.com/questions/3620943/measuring-elapsed-time-with-the-time-module 
PROF_DATA = {}

def profile(fn):
    @wraps(fn)
    def with_profiling(*args, **kwargs):
        start_time = time.time()

        ret = fn(*args, **kwargs)

        elapsed_time = time.time() - start_time

        if fn.__name__ not in PROF_DATA:
            PROF_DATA[fn.__name__] = [0, []]
        PROF_DATA[fn.__name__][0] += 1
        PROF_DATA[fn.__name__][1].append(elapsed_time)

        return ret

    return with_profiling


def print_prof_data():
    for fname, data in PROF_DATA.items():
        max_time = max(data[1])
        avg_time = sum(data[1]) / len(data[1])
        print ("Function %s called %d times. " % (fname, data[0]))
        print ('Execution time max: %.3f, average: %.3f' % (max_time, avg_time))


def clear_prof_data():
    global PROF_DATA
    PROF_DATA = {}

#  ________________________________________
# |                                        |
# |          3: Database Functions         |
# |________________________________________|
BOOK_LIST = 'books/book_list.csv'
BOOK_DB = 'books/book_db.csv'

def write_book_list(data, init=False):
    if init:
        cols = ['book_id', 'last_seen']
        pd.DataFrame(columns=cols).to_csv(BOOK_LIST, encoding="utf-8")
    else:
        pd.DataFrame.from_dict(data).to_csv(BOOK_LIST, mode='a', header=False, encoding="utf-8")

def write_book_bd(data, init=False, is_list=False):
    if init:
        cols = ['book_id', 'title', 'price', 'stock', 'rating', 'img', \
                'description', 'UPC', 'Product_Type', 'Price_excl_tax_', \
                'Price_incl_tax_', 'Tax', 'Availability', 'Number_of_reviews', \
                'last_seen']
        pd.DataFrame(columns=cols).to_csv(BOOK_DB, encoding="utf-8")
        return
    
    if is_list:
        pd.DataFrame(data).to_csv(BOOK_DB, mode='a', header=False, encoding="utf-8")
        return
    
    pd.DataFrame.from_dict(data).to_csv(BOOK_DB, mode='a', header=False, encoding="utf-8")


def query_books_list():
    return pd.read_csv(BOOK_LIST)['book_id'].to_list()
#  ________________________________________
# |                                        |
# |        3: Serial Implementation        |
# |________________________________________|


def scrape_books(html_soup, url, book_dict, count, batch_size):
    for book in html_soup.select('article.product_pod'):
        # For now, we'll only store the books url
        book_url = book.find('h3').find('a').get('href')
        book_url = urljoin(url, book_url)
        path = urlparse(book_url).path
        book_id = path.split('/')[2]
        # Upsert tries to update first and then insert instead
        book_dict['book_id'] += [book_id]
        book_dict['last_seen'] += [datetime.now()]
        count += 1
        if count % batch_size == 0:
            write_book_list(book_dict)
            book_dict = {'book_id': [], 'last_seen': []}
    return book_dict, count


def all_books_list(url, batch_size=float('inf')):
    book_dict = {'book_id': [], 'last_seen': []}
    write_book_list(book_dict, init=True)
    count = 0
    while True :
        print('Now scraping page:', url)
        r = requests.get(url)
        html_soup = BeautifulSoup(r.text, 'html.parser')
        book_dict, count = scrape_books(html_soup, url, book_dict, count, batch_size)
        # Is there a next page?
        next_a = html_soup.select('li.next > a')
        if not next_a or not next_a[0].get('href'):
            break
        url = urljoin(url, next_a[0].get('href'))

    write_book_list(book_dict) 
    books = book_dict['book_id']

    if len(books) != count:
        books = query_books_list()
    return books



def scrape_book(html_soup, book_id, book):
    main = html_soup.find(class_='product_main')
    book['book_id'] = book.get('book_id', []) + \
                      [book_id]
    book['title'] = book.get('title', []) + \
                    [main.find('h1').get_text(strip=True)]
    book['price'] = book.get('price', []) + \
                    [main.find(class_='price_color').get_text(strip=True)]
    book['stock'] = book.get('stock', []) + \
                    [main.find(class_='availability').get_text(strip=True)]
    book['rating'] = book.get('rating', []) + \
                     [' '.join(main.find(class_='star-rating') \
                        .get('class')).replace('star-rating', '').strip()]
    book['img'] = book.get('img', []) + \
                  [html_soup.find(class_='thumbnail').find('img').get('src')]
    desc = html_soup.find(id='product_description')
    if desc:
        book['description'] = book.get('description', []) + \
                              [desc.find_next_sibling('p').get_text(strip=True)]
    else:
        book['description'] = book.get('description', []) + ['']

    book_product_table = html_soup.find(text='Product Information').find_next('table')
    for row in book_product_table.find_all('tr'):
        header = row.find('th').get_text(strip=True)
        # Since we'll use the header as a column, clean it a bit
        # to make sure SQLite will accept it
        header = re.sub('[^a-zA-Z]+', '_', header)
        value = row.find('td').get_text(strip=True)
        book[header] = book.get(header, []) + [value]
    book['last_seen'] = book.get('last_seen', []) +  [datetime.now()]
    return book


@profile
def crawl_books_seq(url, books, batch_size, write=True):
    write_book_bd({}, init=True) 
    data = {}
    count = 0
    for book in books:
        book_url = url + 'catalogue/{}'.format(book)
        r = requests.get(book_url)
        r.encoding = 'utf-8'
        html_soup = BeautifulSoup(r.text, 'html.parser')
        data = scrape_book(html_soup, book, data)
        count += 1
        if count % batch_size == 0:
            write_book_bd(data)
            data = {}
    if write:
        write_book_bd(data) 
    return data

     
#  ________________________________________
# |                                        |
# |        4: PyWren Implementation        |
# |________________________________________|

def scrape_book_simple(html_soup, book_id):
    book = {}
    main = html_soup.find(class_='product_main')
    book['book_id'] = book_id
    book['title'] = main.find('h1').get_text(strip=True)
    book['price'] = main.find(class_='price_color').get_text(strip=True)
    book['stock'] = main.find(class_='availability').get_text(strip=True)
    book['rating'] = ' '.join(main.find(class_='star-rating') \
                        .get('class')).replace('star-rating', '').strip()
    book['img'] = html_soup.find(class_='thumbnail').find('img').get('src')
    desc = html_soup.find(id='product_description')
    if desc:
        book['description'] = desc.find_next_sibling('p').get_text(strip=True)
    else:
        book['description'] = ''

    book_product_table = html_soup.find(text='Product Information').find_next('table')
    for row in book_product_table.find_all('tr'):
        header = row.find('th').get_text(strip=True)
        # Since we'll use the header as a column, clean it a bit
        # to make sure SQLite will accept it
        header = re.sub('[^a-zA-Z]+', '_', header)
        value = row.find('td').get_text(strip=True)
        book[header] = value
    book['last_seen'] = datetime.now()
    return book


def crawl_books_lam(url, books):
    data = []
    for book in books:
        book_url = url + 'catalogue/{}'.format(book)
        r = requests.get(book_url)
        r.encoding = 'utf-8'
        html_soup = BeautifulSoup(r.text, 'html.parser')
        data += [scrape_book_simple(html_soup, book)]
    return data


@profile
def crawl_books_par(url, books, batch_par):
    write_book_bd({}, init=True) 
    pwex = pywren.default_executor()
    batches = [books[i:i + batch_par] for i in range(0, len(books), batch_par)]
    print(batches[0:3])
    futures = pwex.map(lambda x: crawl_books_lam(url, x), batches)
    data = pywren.get_all_results(futures)
    data = [book for _list in data for book in _list]
    write_book_bd(data, is_list=True)
    return data

#  ________________________________________
# |                                        |
# |              8: Reporting              |
# |________________________________________|

books = all_books_list(URL, 1e6)


crawl_books_seq(URL, books, 1e6)

for b in BATCH_SIZES:
    crawl_books_par(URL, books, b)

report = {'Solution':
            ['Sequential'] + ['PyWrew Batch Size: ' + str(b) for b in BATCH_SIZES],
          'Times':
            PROF_DATA['crawl_books_seq'][1] + PROF_DATA['crawl_books_par'][1]}

df = pd.DataFrame.from_dict(report)

sns.barplot(x="Solution", y="Times", data=df, palette='Set3')
plt.subplots_adjust(bottom=0.3, top=0.8)
plt.ylabel("Time in Seconds", size=14)
plt.xlabel("Solutions", size=14)
plt.xticks(rotation=30)
plt.title("Serial vs PyWren Solutiion with\ndifferent number of batches", size=18)
plt.savefig("pywren.png")