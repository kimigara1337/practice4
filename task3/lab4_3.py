import sqlite3
import msgpack
import json


# 1. Создание таблицы для песен
def create_songs_table():
    conn = sqlite3.connect('songs_database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        instrumentalness REAL,
        explicit BOOLEAN,
        loudness REAL
    )''')
    conn.commit()
    conn.close()


# 2. Заполнение таблицы из файла MessagePack
def populate_songs_from_msgpack(filename):
    conn = sqlite3.connect('songs_database.db')
    cursor = conn.cursor()

    with open(filename, 'rb') as file:
        data = msgpack.load(file, raw=False)

        # Преобразуем данные в формат для вставки в таблицу
        values = []
        for item in data:
            artist = item.get('artist', '')
            song = item.get('song', '')
            duration_ms = int(item.get('duration_ms', 0))
            year = int(item.get('year', 0))
            tempo = float(item.get('tempo', 0.0))
            genre = item.get('genre', '')
            instrumentalness = float(item.get('instrumentalness', 0.0))
            explicit = item.get('explicit', 'False').lower() == 'true'
            loudness = float(item.get('loudness', 0.0))

            # Добавляем запись в список значений
            values.append((artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness))

        # Вставляем данные в таблицу
        cursor.executemany(
            "INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values)
        conn.commit()

    conn.close()


# 3. Заполнение таблицы из текстового файла
def populate_songs_from_txt(filename):
    conn = sqlite3.connect('songs_database.db')
    cursor = conn.cursor()

    with open(filename, 'r', encoding='utf-8') as txtfile:
        lines = txtfile.readlines()

        values = []
        for line in lines:
            parts = line.strip().split('  ')  # Допустим, данные разделены двумя пробелами
            if len(parts) == 9:  # Проверяем, что строка имеет 9 элементов
                artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness = parts
                try:
                    duration_ms = int(duration_ms)
                    year = int(year)
                    tempo = float(tempo)
                    instrumentalness = float(instrumentalness)
                    explicit = explicit.lower() == 'true'
                    loudness = float(loudness)

                    values.append((artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness))
                except ValueError:
                    print(f"Skipping invalid line: {line}")

        cursor.executemany(
            "INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values)
        conn.commit()
    conn.close()


# 4. Запрос 1: Вывод первых VAR+10 строк, отсортированных по произвольному числовому полю
def export_first_sorted_to_json(var, sort_field):
    conn = sqlite3.connect('songs_database.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = f"SELECT * FROM songs ORDER BY {sort_field} LIMIT {var + 10}"
    cursor.execute(query)
    rows = cursor.fetchall()

    headers = [description[0] for description in cursor.description]
    data = [dict(zip(headers, row)) for row in rows]

    filename = "first_sorted.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4)

    conn.close()


# 5. Запрос 2: Вывод суммы, минимума, максимума и среднего для произвольного числового поля
def export_aggregate_results(numeric_field):
    conn = sqlite3.connect('songs_database.db')
    cursor = conn.cursor()

    query = f"SELECT SUM({numeric_field}), MIN({numeric_field}), MAX({numeric_field}), AVG({numeric_field}) FROM songs"
    cursor.execute(query)
    result = cursor.fetchone()

    data = {
        "Сумма": result[0],
        "Минимум": result[1],
        "Максимум": result[2],
        "Среднее": result[3]
    }

    filename = "aggregate_results.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)

    conn.close()


# 6. Запрос 3: Вывод частоты встречаемости для категориального поля
def export_categorical_frequency(categorical_field):
    conn = sqlite3.connect('songs_database.db')
    cursor = conn.cursor()

    query = f"SELECT {categorical_field}, COUNT({categorical_field}) AS frequency FROM songs GROUP BY {categorical_field}"
    cursor.execute(query)
    rows = cursor.fetchall()

    data = [{"category": row[0], "frequency": row[1]} for row in rows]

    with open("categorical_frequency.json", "w", encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

    conn.close()


# 7. Запрос 4: Вывод первых VAR+15 строк, отфильтрованных по произвольному предикату, отсортированных по числовому полю
def export_filtered_sorted_to_json(var, filter_predicate, sort_field):
    conn = sqlite3.connect('songs_database.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = f"SELECT * FROM songs WHERE {filter_predicate} ORDER BY {sort_field} LIMIT {var + 15}"
    cursor.execute(query)
    rows = cursor.fetchall()

    headers = [description[0] for description in cursor.description]
    data = [dict(zip(headers, row)) for row in rows]

    filename = "filtered_sorted.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4)

    conn.close()


create_songs_table()
populate_songs_from_msgpack('_part_2.msgpack')
populate_songs_from_txt('_part_1.text')
export_first_sorted_to_json(66, 'duration_ms')
export_aggregate_results('tempo')
export_categorical_frequency('genre')
export_filtered_sorted_to_json(66, 'year > 2000', 'year')
