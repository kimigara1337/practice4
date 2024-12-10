import sqlite3
import pandas as pd
import msgpack
import json

# Загрузка данных из файла msgpack
file_path = r"E:\66\lab4\task1-2\item.msgpack"  # Укажите путь к вашему файлу

# Чтение данных с использованием msgpack
with open(file_path, 'rb') as file:
    unpacked_data = msgpack.unpack(file)

# Если данные представляют собой список словарей, преобразуем их в DataFrame
data_frame = pd.DataFrame(unpacked_data)

# Создание базы данных SQLite
conn = sqlite3.connect('books.db')
cursor = conn.cursor()

# Создание таблицы
cursor.execute('''
CREATE TABLE IF NOT EXISTS books (
    title TEXT,
    author TEXT,
    genre TEXT,
    pages INTEGER,
    published_year INTEGER,
    isbn TEXT,
    rating REAL,
    views INTEGER
);
''')

# Загрузка данных в SQLite
data_frame.to_sql('books', conn, if_exists='replace', index=False)

# Выполнение запросов
VAR = 66

# 1. Первые VAR+10 строк
cursor.execute('''
SELECT * FROM books
ORDER BY views DESC
LIMIT ?;
''', (VAR + 10,))
top_rows = cursor.fetchall()

# Преобразование результата запроса в формат словаря для удобства сериализации
columns = ['title', 'author', 'genre', 'pages', 'published_year', 'isbn', 'rating', 'views']
top_books = [dict(zip(columns, row)) for row in top_rows]

# Сохранение в JSON с нормализованным форматом
with open('top_books.json', 'w', encoding="utf-8") as file:
    json.dump(top_books, file, ensure_ascii=False, indent=4)

# 2. Статистика по числовому полю
cursor.execute('''
SELECT 
    SUM(rating) AS total_rating,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating,
    AVG(rating) AS avg_rating
FROM books;
''')
stats = cursor.fetchone()
stats_dict = {
    'total_rating': stats[0],
    'min_rating': stats[1],
    'max_rating': stats[2],
    'avg_rating': stats[3]
}
print("Статистика:", stats_dict)

# 3. Частота встречаемости категориального поля
cursor.execute('''
SELECT genre, COUNT(*) AS frequency
FROM books
GROUP BY genre
ORDER BY frequency DESC;
''')
genre_frequency = cursor.fetchall()
genre_frequency_dict = [{'genre': row[0], 'frequency': row[1]} for row in genre_frequency]
print("Частота жанров:", genre_frequency_dict)

# 4. Отфильтрованные строки
cursor.execute('''
SELECT * FROM books
WHERE rating > 4
ORDER BY views DESC
LIMIT ?;
''', (VAR + 10,))
filtered_rows = cursor.fetchall()

# Преобразование в формат словаря
filtered_books = [dict(zip(columns, row)) for row in filtered_rows]

# Сохранение отфильтрованных данных в JSON с нормализованным форматом
with open('filtered_books.json', 'w', encoding="utf-8") as file:
    json.dump(filtered_books, file, ensure_ascii=False, indent=4)

conn.close()
