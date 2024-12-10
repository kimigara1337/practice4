import sqlite3
import pandas as pd
import json
import msgpack


# Загрузка данных из файлов
def load_data():
    # Загрузка данных из item.msgpack
    with open(r"E:\66\lab4\task1-2\item.msgpack", 'rb') as f:
        books_data = pd.DataFrame(msgpack.unpack(f, raw=False))

    # Загрузка данных из subitem.pkl
    sales_data = pd.read_pickle(r"E:\66\lab4\task1-2\subitem.pkl")
    if isinstance(sales_data, list):
        sales_data = pd.DataFrame(sales_data)

    return books_data, sales_data


# Создание таблицы для данных subitems
def create_subitems_table():
    conn = sqlite3.connect('books_and_sales.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS subitems
                      (title TEXT REFERENCES books (title), price INTEGER, place TEXT, date TEXT)''')
    conn.commit()
    conn.close()


# Наполнение таблицы subitems данными из DataFrame
def populate_subitems_from_dataframe(df):
    conn = sqlite3.connect('books_and_sales.db')
    cursor = conn.cursor()
    values = df[['title', 'price', 'place', 'date']].values.tolist()
    cursor.executemany("INSERT INTO subitems VALUES (?, ?, ?, ?)", values)
    conn.commit()
    conn.close()


# Запрос 1: Вывод цен и мест продаж
def display_book_prices_and_places():
    conn = sqlite3.connect('books_and_sales.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT books.title, subitems.price, subitems.place FROM books JOIN subitems ON books.title = subitems.title")
    results = cursor.fetchall()
    data = []
    for result in results:
        book_info = {
            "Книга": result['title'],
            "Цена": result['price'],
            "Место продажи": result['place']
        }
        data.append(book_info)
    with open(r'book_prices_and_places.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


# Запрос 2: Средняя цена и количество продаж по жанрам
def average_price_and_sales_by_genre():
    conn = sqlite3.connect('books_and_sales.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT books.genre, AVG(subitems.price), COUNT(subitems.price) FROM books JOIN subitems ON books.title = subitems.title GROUP BY books.genre")
    results = cursor.fetchall()
    data = []
    for result in results:
        genre_info = {
            "Категория": result['genre'],
            "Средняя цена": result[1],
            "Количество продаж": result[2]
        }
        data.append(genre_info)
    with open(r'average_price_and_sales_by_genre.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


# Запрос 3: Самая дорогая и самая дешевая книга
def find_cheapest_and_most_expensive_books():
    conn = sqlite3.connect('books_and_sales.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT books.title, subitems.price FROM books JOIN subitems ON books.title = subitems.title ORDER BY subitems.price ASC LIMIT 1")
    cheapest_book = cursor.fetchone()
    cursor.execute(
        "SELECT books.title, subitems.price FROM books JOIN subitems ON books.title = subitems.title ORDER BY subitems.price DESC LIMIT 1")
    most_expensive_book = cursor.fetchone()

    cheapest_book_info = {
        "Самая дешевая книга": cheapest_book['title'],
        "Цена": cheapest_book['price']
    }

    most_expensive_book_info = {
        "Самая дорогая книга": most_expensive_book['title'],
        "Цена": most_expensive_book['price']
    }

    data = {
        "Самая дешевая книга": cheapest_book_info,
        "Самая дорогая книга": most_expensive_book_info
    }

    with open(r'cheapest_and_most_expensive_books.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()



books_data, sales_data = load_data()
create_subitems_table()
populate_subitems_from_dataframe(sales_data)
display_book_prices_and_places()
average_price_and_sales_by_genre()
find_cheapest_and_most_expensive_books()
