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

class PageRankDAO(Database):
    def clear_page_rank(self):
        self.conn.execute('DROP TABLE IF EXISTS pagerank')
        self.conn.execute("""CREATE TABLE IF NOT EXISTS pagerank(
                             row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                             url_id INTEGER,
                             score REAL
                         );""")
        self.commit()

    def create_indexes(self):
        self.conn.execute("DROP INDEX IF EXISTS wordidx;")
        self.conn.execute("DROP INDEX IF EXISTS urlidx;")
        self.conn.execute("DROP INDEX IF EXISTS wordurlidx;")
        self.conn.execute("DROP INDEX IF EXISTS urltoidx;")
        self.conn.execute("DROP INDEX IF EXISTS urlfromidx;")
        self.conn.execute('CREATE INDEX IF NOT EXISTS wordidx ON wordlist(word)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS urlidx ON urllist(url)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS wordurlidx ON wordlocation(word_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS urltoidx ON link (to_url_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS urlfromidx ON link (from_url_id)')
        self.conn.execute("DROP INDEX IF EXISTS rankurlididx;")
        self.conn.execute('CREATE INDEX IF NOT EXISTS rankurlididx ON pagerank(url_id)')
        self.commit()

    def initialize_page_rank(self):
        self.conn.execute('INSERT INTO pagerank (url_id, score) SELECT id, 1.0 FROM urllist')
        self.commit()

    def get_all_urlids(self):
        return [row[0] for row in self.conn.execute("SELECT id FROM urllist").fetchall()]

    def get_linking_urls(self, urlid):
        return [row[0] for row in self.conn.execute("""
            SELECT DISTINCT from_url_id FROM link WHERE to_url_id = ?
        """, (urlid,)).fetchall()]

    def get_page_rank(self, fromid):
        return self.conn.execute("SELECT score FROM pagerank WHERE url_id = ?", (fromid,)).fetchone()[0]

    def get_link_count(self, fromid):
        return self.conn.execute("""
            SELECT COUNT(*) FROM link WHERE from_url_id = ?
        """, (fromid,)).fetchone()[0]

    def update_page_rank(self, urlid, pr):
        self.conn.execute('UPDATE pagerank SET score = ? WHERE url_id = ?', (pr, urlid))
        self.commit()

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
        return self.cursor.lastrowid

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

    def get_word_id(self, word):
        """ Retrieve the row ID for a specific word """
        self.cursor.execute("SELECT rowid FROM wordlist WHERE word = ? LIMIT 1;", (word,))
        result = self.cursor.fetchone()
        return result[0] if result else None


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

    def get_top_words(self, limit):
        # SQL-запрос для получения топ 20 слов на основе частоты их появления в таблице wordlocation
        query = '''
            SELECT w.word, COUNT(wl.word_id) as frequency
            FROM wordlist w
            JOIN wordlocation wl ON w.id = wl.word_id
            GROUP BY wl.word_id
            ORDER BY frequency DESC
            LIMIT ?
        '''
        self.cursor.execute(query, (limit,))
        top_words = self.cursor.fetchall()

        return top_words

    def get_match_rows(self, wordsidList):
        """
        Формирует и выполняет SQL-запрос для поиска всех совпадений слов в проиндексированных URL-адресах.
        :param wordsidList: список идентификаторов слов.
        :return: список совпадений (urlid, loc_q1, loc_q2, ...).
        """
        if not wordsidList:
            return []

        # Создание частей SQL-запроса
        sqlpart_Name = ["w0.url_id AS url_id", "w0.location AS loc_0"]
        sqlpart_Join = []
        sqlpart_Condition = ["w0.word_id = ?"]

        # Формирование частей SQL-запроса
        for wordIndex, wordID in enumerate(wordsidList[1:], start=1):
            # Добавляем местоположение для последующих слов
            sqlpart_Name.append(f"w{wordIndex}.location AS loc_{wordIndex}")
            sqlpart_Join.append(f"INNER JOIN wordlocation w{wordIndex} ON w0.url_id = w{wordIndex}.url_id")
            sqlpart_Condition.append(f"w{wordIndex}.word_id = ?")

        # Объединение частей SQL-запроса
        sqlFullQuery = f"""
            SELECT {', '.join(sqlpart_Name)}
            FROM wordlocation w0
            {' '.join(sqlpart_Join)}
            WHERE {' AND '.join(sqlpart_Condition)}
        """

        # Выполнение SQL-запроса с передачей идентификаторов слов в качестве параметров
        return self.execute_sql(sqlFullQuery, tuple(wordsidList))

    def execute_sql(self, sql_query, params=()):
        """ Выполняет SQL-запрос с параметрами и возвращает все результаты """
        cursor = self.conn.cursor()
        cursor.execute(sql_query, params)
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
