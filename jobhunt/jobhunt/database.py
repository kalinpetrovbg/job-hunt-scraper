import sqlite3

connection = sqlite3.connect('mydatabase.db')
cursor = connection.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS job_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        date_posted TEXT
    );
''')
connection.commit()
connection.close()
