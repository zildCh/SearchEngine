import sqlite3

def create_db():
    conn = sqlite3.connect('search_engine.db')
    cursor = conn.cursor()

    # Таблица URL
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS urllist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT
    )
    ''')

    # Таблица слов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wordlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        word TEXT,
        isFiltered BOOLEAN
    )
    ''')

    # Таблица местоположения слов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wordlocation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        word_id INTEGER,
        url_id INTEGER,
        location INTEGER,
        FOREIGN KEY(word_id) REFERENCES wordlist(id),
        FOREIGN KEY(url_id) REFERENCES urllist(id)
    )
    ''')

    # Таблица связей между URL
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS link (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_url_id INTEGER,
        to_url_id INTEGER,
        FOREIGN KEY(from_url_id) REFERENCES urllist(id),
        FOREIGN KEY(to_url_id) REFERENCES urllist(id)
    )
    ''')

    # Таблица слов в ссылках
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS linkwords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        word_id INTEGER,
        link_id INTEGER,
        FOREIGN KEY(word_id) REFERENCES wordlist(id),
        FOREIGN KEY(link_id) REFERENCES link(id)
    )
    ''')

    conn.commit()
    conn.close()

create_db()