import sqlite3
import csv
import json

# Имена файлов
product_data_file = '_product_data.text'
update_data_file = '_update_data.csv'
database_file = 'fourth_task.db'

# Создание таблицы products с добавлением счётчика обновлений
def create_products_table(cursor):
    cursor.execute(''' 
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        quantity INTEGER,
        category TEXT,
        fromCity TEXT,
        isAvailable BOOLEAN,
        views INTEGER,
        update_counter INTEGER DEFAULT 0  -- Счётчик обновлений
    )''')

# Обработка данных из .text файла (товары)
def insert_products_from_text(cursor, products_data):
    products = products_data.split('=====\n')  # Разделим товары по разделителю
    all_products = []
    for product in products:
        product_info = product.strip().split('\n')
        try:
            name = next((line.split('::')[1].strip() for line in product_info if line.startswith('name::')), 'Unknown')
            price = float(next((line.split('::')[1].strip() for line in product_info if line.startswith('price::')), 0.0))
            quantity = int(next((line.split('::')[1].strip() for line in product_info if line.startswith('quantity::')), 0))
            category = next((line.split('::')[1].strip() for line in product_info if line.startswith('category::')), None)
            fromCity = next((line.split('::')[1].strip() for line in product_info if line.startswith('fromCity::')), 'Unknown')
            isAvailable = next((line.split('::')[1].strip() for line in product_info if line.startswith('isAvailable::')), 'False') == 'True'
            views = int(next((line.split('::')[1].strip() for line in product_info if line.startswith('views::')), 0))
            all_products.append((name, price, quantity, category, fromCity, isAvailable, views))
        except Exception as e:
            print(f"Ошибка обработки строки: {product_info}, ошибка: {e}")

    cursor.executemany(
        '''INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views) 
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        all_products)

# Загрузка изменений из CSV файла
def load_changes_from_csv(file_path):
    changes = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            changes.append({
                "name": row.get("name", ""),
                "method": row.get("method", ""),
                "param": row.get("param", "")
            })
    return changes

# Применение изменений
def apply_changes(cursor, changes):
    for change in changes:
        name = change['name']
        method = change['method']
        param = float(change['param']) if change['param'] else None

        if method == 'available':
            cursor.execute("UPDATE products SET isAvailable = ?, update_counter = update_counter + 1 WHERE name = ?", (param, name))
        elif method == 'price_percent':
            cursor.execute("UPDATE products SET price = price * (1 + ?), update_counter = update_counter + 1 WHERE name = ?", (param, name))
        elif method == 'price_abs':
            cursor.execute("UPDATE products SET price = price + ?, update_counter = update_counter + 1 WHERE name = ?", (param, name))
        elif method == 'quantity_add':
            cursor.execute("UPDATE products SET quantity = quantity + ?, update_counter = update_counter + 1 WHERE name = ?", (param, name))
        elif method == 'quantity_sub':
            cursor.execute("UPDATE products SET quantity = quantity - ?, update_counter = update_counter + 1 WHERE name = ?", (param, name))
        elif method == 'remove':
            cursor.execute("DELETE FROM products WHERE name = ?", (name,))

# Топ-10 самых обновляемых товаров
def query_top_updated_products(cursor):
    cursor.execute("SELECT name, update_counter FROM products ORDER BY update_counter DESC LIMIT 10")
    return cursor.fetchall()

# Анализ цен товаров по категориям
def query_price_analysis(cursor):
    cursor.execute('''SELECT category, 
                             SUM(price), 
                             MIN(price), 
                             MAX(price), 
                             AVG(price), 
                             COUNT(*)
                      FROM products 
                      GROUP BY category''')
    return cursor.fetchall()

# Анализ остатков товаров по категориям
def query_quantity_analysis(cursor):
    cursor.execute('''SELECT category, 
                             SUM(quantity), 
                             MIN(quantity), 
                             MAX(quantity), 
                             AVG(quantity), 
                             COUNT(*)
                      FROM products 
                      GROUP BY category''')
    return cursor.fetchall()

# Произвольный запрос
def query_custom(cursor):
    cursor.execute('''SELECT name, price, quantity FROM products WHERE price > 50000 AND quantity < 50''')
    return cursor.fetchall()


conn = sqlite3.connect(database_file)
cursor = conn.cursor()

# Создание таблицы и загрузка данных
create_products_table(cursor)

with open(product_data_file, 'r', encoding='utf-8') as f:
    products_data = f.read()
insert_products_from_text(cursor, products_data)

changes = load_changes_from_csv(update_data_file)
apply_changes(cursor, changes)

conn.commit()


top_updated_products = query_top_updated_products(cursor)
price_analysis = query_price_analysis(cursor)
quantity_analysis = query_quantity_analysis(cursor)
custom_query_result = query_custom(cursor)

conn.close()

# Сохранение результатов в файлы
with open('top_updated_products.json', 'w', encoding='utf-8') as f:
    json.dump(top_updated_products, f, ensure_ascii=False, indent=4)
with open('price_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(price_analysis, f, ensure_ascii=False, indent=4)
with open('quantity_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(quantity_analysis, f, ensure_ascii=False, indent=4)
with open('custom_query_result.json', 'w', encoding='utf-8') as f:
    json.dump(custom_query_result, f, ensure_ascii=False, indent=4)

