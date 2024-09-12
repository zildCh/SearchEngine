import sqlite3
from urllib.parse import urlparse
class Database:
    def __init__(self, db_path='search_engine.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

# DAO для таблицы urllist
class UrlListDAO(Database):
    def add_url(self, url):
        self.cursor.execute('INSERT INTO urllist (url) VALUES (?)', (url,))
        self.commit()

    def get_url(self, url_id):
        self.cursor.execute('SELECT * FROM urllist WHERE id = ?', (url_id,))
        return self.cursor.fetchone()

    def get_all_urls(self):
        self.cursor.execute('SELECT * FROM urllist')
        return self.cursor.fetchall()

    def get_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM urllist")
        return cursor.fetchone()[0]

    def get_top_domains(self, limit=20):
        cursor = self.conn.cursor()

        # Получаем все URL
        cursor.execute("SELECT url FROM urllist")
        urls = cursor.fetchall()

        # Извлекаем домены из URL
        domain_counts = {}
        for url_tuple in urls:
            url = url_tuple[0]
            domain = urlparse(url).netloc
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1

        # Сортируем домены по количеству и берем топ 'limit'
        sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:limit]

        return sorted_domains

    def get_url_by_value(self, url):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM urllist WHERE url = ?", (url,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем id, если URL найден
        return None  # Возвращаем None, если URL не найден
# DAO для таблицы wordlist
class WordListDAO(Database):
    def add_word(self, word, is_filtered=False):
        self.cursor.execute('INSERT INTO wordlist (word, isFiltered) VALUES (?, ?)', (word, is_filtered))
        self.commit()

    def get_word(self, word_id):
        self.cursor.execute('SELECT * FROM wordlist WHERE id = ?', (word_id,))
        return self.cursor.fetchone()

    def get_all_words(self):
        self.cursor.execute('SELECT * FROM wordlist')
        return self.cursor.fetchall()

    def get_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM wordlist")
        return cursor.fetchone()[0]


    def get_top_words(self, limit=20):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT word, COUNT(*) as frequency
            FROM wordlist
            WHERE isFiltered = FALSE
            GROUP BY word
            ORDER BY frequency DESC
            LIMIT ?
        """, (limit,))
        return cursor.fetchall()


    def get_word_by_value(self, word):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM wordlist WHERE word = ?", (word,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем id, если слово найдено
        return None  # Возвращаем None, если слово не найдено

# DAO для таблицы wordlocation
class WordLocationDAO(Database):
    def add_word_location(self, word_id, url_id, location):
        self.cursor.execute('INSERT INTO wordlocation (word_id, url_id, location) VALUES (?, ?, ?)',
                            (word_id, url_id, location))
        self.commit()

    def get_word_locations(self, word_id, url_id):
        self.cursor.execute('SELECT * FROM wordlocation WHERE word_id = ? AND url_id = ?', (word_id, url_id))
        return self.cursor.fetchall()

    def get_word_locations_by_url(self, url_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM wordlocation WHERE url_id = ?
        ''', (url_id,))
        return cursor.fetchall()
# DAO для таблицы link
class LinkDAO(Database):
    def add_link(self, from_url_id, to_url_id):
        self.cursor.execute('INSERT INTO link (from_url_id, to_url_id) VALUES (?, ?)', (from_url_id, to_url_id))
        self.commit()

    def get_links(self):
        self.cursor.execute('SELECT * FROM link')
        return self.cursor.fetchall()

    def get_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM link")
        return cursor.fetchone()[0]

    def get_link_by_urls(self, from_url_id, to_url_id):
        cursor = self.conn.cursor()
        cursor.execute('''
             SELECT id FROM link
             WHERE from_url_id = ? AND to_url_id = ?
         ''', (from_url_id, to_url_id))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем id найденной связи
        return None  # Возвращаем None, если связь не найдена
# DAO для таблицы linkwords
class LinkWordsDAO(Database):
    def add_link_word(self, word_id, link_id):
        self.cursor.execute('INSERT INTO linkwords (word_id, link_id) VALUES (?, ?)', (word_id, link_id))
        self.commit()

    def get_link_words(self):
        self.cursor.execute('SELECT * FROM linkwords')
        return self.cursor.fetchall()