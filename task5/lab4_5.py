import sqlite3
import json
import csv
from datetime import datetime


def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS Manufacturer (
                      id INTEGER PRIMARY KEY,
                      name TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Car (
                      Manufacturer_id INTEGER,
                      Model TEXT,
                      Sales_in_thousands REAL,
                      __year_resale_value REAL,
                      FOREIGN KEY (Manufacturer_id) REFERENCES Manufacturer(id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS CarDetails (
                      Car_id INTEGER PRIMARY KEY,
                      Vehicle_type TEXT,
                      Price_in_thousands REAL,
                      Engine_size REAL,
                      Horsepower REAL,
                      FOREIGN KEY (Car_id) REFERENCES Car(Manufacturer_id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS CarDimensions (
                      Car_id INTEGER PRIMARY KEY,
                      Wheelbase REAL,
                      Width REAL,
                      Length REAL,
                      Curb_weight REAL,
                      FOREIGN KEY (Car_id) REFERENCES Car(Manufacturer_id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS CarPerformance (
                      Car_id INTEGER PRIMARY KEY,
                      Fuel_capacity REAL,
                      Fuel_efficiency REAL,
                      Latest_Launch DATE,
                      Power_perf_factor REAL,
                      FOREIGN KEY (Car_id) REFERENCES Car(Manufacturer_id))''')


def load_data_to_db(cursor, json_file, csv_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
        manufacturers = list(set([data.get('Manufacturer') for data in json_data]))
        cursor.executemany('INSERT INTO Manufacturer (name) VALUES (?)',
                           [(manufacturer,) for manufacturer in manufacturers])
        manufacturer_ids = {manufacturer: idx + 1 for idx, manufacturer in enumerate(manufacturers)}
        cursor.executemany(
            'INSERT INTO Car (Manufacturer_id, Model, Sales_in_thousands, __year_resale_value) VALUES (?, ?, ?, ?)',
            [(manufacturer_ids[data.get('Manufacturer')], data.get('Model'), data.get('Sales_in_thousands') or None,
              data.get('__year_resale_value') or None) for data in json_data])

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        csv_data = csv.reader(file)
        next(csv_data)
        for row in csv_data:
            manufacturer_id = manufacturer_ids.get(row[0])
            horsepower = int(row[3]) if row[3] else None
            wheelbase = float(row[4]) if row[4] else None
            width = float(row[5]) if row[5] else None
            length = float(row[6]) if row[6] else None
            curb_weight = float(row[7]) if row[7] else None
            fuel_capacity = float(row[8]) if row[8] else None
            fuel_efficiency = float(row[9]) if row[9] else None
            latest_launch = datetime.strptime(row[10], '%m/%d/%Y').date() if row[10] else None
            power_perf_factor = float(row[11]) if row[11] else None

            if row[1] and row[2]:
                cursor.execute(
                    'INSERT INTO CarDetails (Car_id, Vehicle_type, Price_in_thousands, Engine_size, Horsepower) VALUES (?, ?, ?, ?, ?)',
                    (manufacturer_id, row[0], float(row[1]), float(row[2]), horsepower))

            cursor.execute(
                'INSERT INTO CarDimensions (Car_id, Wheelbase, Width, Length, Curb_weight) VALUES (?, ?, ?, ?, ?)',
                (manufacturer_id, wheelbase, width, length, curb_weight))
            cursor.execute(
                'INSERT INTO CarPerformance (Car_id, Fuel_capacity, Fuel_efficiency, Latest_Launch, Power_perf_factor) VALUES (?, ?, ?, ?, ?)',
                (manufacturer_id, fuel_capacity, fuel_efficiency, latest_launch, power_perf_factor))


def execute_queries(cursor):
    cursor.execute("SELECT * FROM Car WHERE Sales_in_thousands > 200")
    result1 = cursor.fetchall()

    cursor.execute("SELECT * FROM CarDetails ORDER BY Price_in_thousands DESC LIMIT 5")
    result2 = cursor.fetchall()

    cursor.execute("SELECT COUNT(*), Vehicle_type FROM CarDetails GROUP BY Vehicle_type")
    result3 = cursor.fetchall()

    cursor.execute(
        "SELECT Manufacturer.name, AVG(Car.Sales_in_thousands) FROM Manufacturer JOIN Car ON Manufacturer.id = Car.Manufacturer_id GROUP BY Manufacturer.name")
    result4 = cursor.fetchall()

    cursor.execute("SELECT * FROM CarDetails WHERE Engine_size = (SELECT MAX(Engine_size) FROM CarDetails)")
    result5 = cursor.fetchall()

    cursor.execute("SELECT * FROM CarDimensions WHERE Length > 180")
    result6 = cursor.fetchall()

    return result1, result2, result3, result4, result5, result6


def save_results_to_json(result1, result2, result3, result4, result5, result6):
    with open('result1-выборка.json', 'w') as json_file:
        json.dump(
            [{'Manufacturer_id': row[0], 'Model': row[1], 'Sales_in_thousands': row[2], '__year_resale_value': row[3]}
             for row in result1], json_file, indent=2)

    with open('result2-сортировка.json', 'w') as json_file:
        json.dump([{'Manufacturer_id': row[0], 'Vehicle_type': row[1], 'Price_in_thousands': row[2],
                    'Engine_size': row[3], 'Horsepower': row[4]} for row in result2], json_file, indent=2)

    with open('result3-агрегация.json', 'w') as json_file:
        json.dump([{'Count': row[0], 'Vehicle_type': row[1]} for row in result3], json_file, indent=2)

    with open('result4-группировка.json', 'w') as json_file:
        json.dump([{'Manufacturer': row[0], 'Average_Sales_in_thousands': row[1]} for row in result4], json_file,
                  indent=2)

    with open('result5-выборка_макс_объем.json', 'w') as json_file:
        json.dump([{'Manufacturer_id': row[0], 'Vehicle_type': row[1], 'Price_in_thousands': row[2],
                    'Engine_size': row[3], 'Horsepower': row[4]} for row in result5], json_file, indent=2)

    with open('result6-выборка_длина_более_180.json', 'w') as json_file:
        json.dump(
            [{'Manufacturer_id': row[0], 'Wheelbase': row[1], 'Width': row[2], 'Length': row[3], 'Curb_weight': row[4]}
             for row in result6], json_file, indent=2)


def main():
    db_file = r'data_frame.db'
    json_file = r'car_sales.json'
    csv_file = r'car_sales1.csv'

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    create_tables(cursor)
    load_data_to_db(cursor, json_file, csv_file)
    results = execute_queries(cursor)
    save_results_to_json(*results)

    conn.commit()
    conn.close()


