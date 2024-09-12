import sqlite3
import requests
from bs4 import BeautifulSoup
from DAO import UrlListDAO, WordListDAO, WordLocationDAO, LinkDAO, LinkWordsDAO
from DBcreate import create_db
import matplotlib.pyplot as plt
import numpy as np

class Crawler:
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.url_dao = UrlListDAO(dbFileName)
        self.word_dao = WordListDAO(dbFileName)
        self.word_location_dao = WordLocationDAO(dbFileName)
        self.link_dao = LinkDAO(dbFileName)
        self.link_words_dao = LinkWordsDAO(dbFileName)
        self.page_counts = []
        self.url_counts = []
        self.word_counts = []
        self.link_counts = []

    def __del__(self):
        print("Crawler завершает работу.")

    def addIndex(self, soup, url):
        self.monitor_db()
        if self.isIndexed(url):
            return

        # Извлекаем текст страницы
        text = self.getTextOnly(soup)
        words = self.separateWords(text)

        # Получаем идентификатор URL
        url_id = self.getEntryId("urllist", "url", url, createNew=True)

        # Индексируем каждое слово
        for i, word in enumerate(words):

            #word_id = self.getEntryId("wordlist", "word", word, createNew=True)
            word_id = self.word_dao.add_word(word)  # Добавляем слово в базу, даже если оно уже существует
            self.word_location_dao.add_word_location(word_id, url_id, i)



    def plot_graphs(self):
        x = np.array(self.page_counts)

        plt.figure(figsize=(15, 5))

        plt.subplot(1, 3, 1)
        plt.plot(x, self.url_counts, marker='')
        plt.title('URL Count')
        plt.xlabel('Pages Visited')
        plt.ylabel('URL Count')

        plt.subplot(1, 3, 2)
        plt.plot(x, self.word_counts, marker='')
        plt.title('Word Count')
        plt.xlabel('Pages Visited')
        plt.ylabel('Word Count')

        plt.subplot(1, 3, 3)
        plt.plot(x, self.link_counts, marker='')
        plt.title('Link Count')
        plt.xlabel('Pages Visited')
        plt.ylabel('Link Count')

        plt.tight_layout()
        plt.show()


    def monitor_db(self):
        url_count = self.url_dao.get_count()
        word_count = self.word_dao.get_count()
        link_count = self.link_dao.get_count()

        self.url_counts.append(url_count)
        self.word_counts.append(word_count)
        self.link_counts.append(link_count)
        self.page_counts.append(len(self.page_counts) + 1)  # или текущее количество посещённых страниц


        print(f"URL count: {url_count}, Word count: {word_count}, Link count: {link_count}")

    def analyze_indexing(self):
        # Количество записей в каждой таблице
        url_count = self.url_dao.get_count()
        word_count = self.word_dao.get_count()
        link_count = self.link_dao.get_count()

        print(f"URL count: {url_count}")
        print(f"Word count: {word_count}")
        print(f"Link count: {link_count}")

        # 20 наиболее часто встречающихся доменов
        domains = self.url_dao.get_top_domains(limit=20)
        print("Топ 20 доменов:", domains)

        # 20 наиболее часто встречающихся слов
        frequent_words = self.word_dao.get_top_words(limit=20)
        print("Топ 20 слов:", frequent_words)


    def getTextOnly(self, soup):
        return soup.get_text()

    def is_valid_url(url):
        # Исключить ссылки с расширением .apk
        return not url.endswith('.apk')

    def separateWords(self, text):
        # Разделение на слова по пробелам и знакам препинания
        russian_conjunctions = ['и', 'а', 'но', 'или', 'да', 'же', 'что', 'как', 'когда', 'если', 'то', 'ли', 'не',
                                'ни', 'либо', 'чтобы', 'хотя', 'зато']

        import re
        words = re.findall(r'\w+', text.lower())
        words = [word for word in words if word not in russian_conjunctions]
        return words

    def isIndexed(self, url):
        # Проверяем, есть ли URL в таблице urllist и связаны ли с ним слова
        url_id = self.url_dao.get_url_by_value(url)
        if not url_id:
            return False

        # Проверяем наличие слов в wordlocation
        word_locations = self.word_location_dao.get_word_locations_by_url(url_id)
        return len(word_locations) > 0

    def addLinkRef(self, urlFrom, urlTo, linkText):
        from_id = self.getEntryId("urllist", "url", urlFrom, createNew=True)
        to_id = self.getEntryId("urllist", "url", urlTo, createNew=True)
        self.link_dao.add_link(from_id, to_id)

        # Сохраняем слова из текста ссылки
        words = self.separateWords(linkText)
        for word in words:
            word_id = self.getEntryId("wordlist", "word", word, createNew=True)
            link_id = self.link_dao.get_link_by_urls(from_id, to_id)
            self.link_words_dao.add_link_word(word_id, link_id)

    def getEntryId(self, tableName, fieldName, value, createNew=True):
        # Проверка наличия записи
        result = None
        if tableName == "urllist":
            result = self.url_dao.get_url_by_value(value)

        if result:
            return result  # Возвращаем существующий ID

        if createNew:
            # Создаем новую запись
            if tableName == "urllist":
                self.url_dao.add_url(value)  # Добавляем новый URL в базу
                return self.url_dao.get_url_by_value(value)  # Получаем id вновь добавленного URL
        return None

    def crawl(self, urlList, maxDepth, maxUrls=100):
        visited_urls = set()  # Множество для отслеживания уникальных URL
        total_urls_processed = 0  # Счетчик обработанных URL

        for currDepth in range(maxDepth):
            if total_urls_processed >= maxUrls:
                print("Достигнут лимит обработанных URL.")
                break

            next_depth_urls = []
            for url in urlList:

                if total_urls_processed >= maxUrls:
                    print("Достигнут лимит обработанных URL.")
                    break

                if url in visited_urls:
                    continue

                visited_urls.add(url)
                total_urls_processed += 1

                try:
                    html_doc = requests.get(url).text
                except requests.RequestException as e:
                    print(f"Ошибка при запросе URL {url}: {e}")
                    continue

                soup = BeautifulSoup(html_doc, "html.parser")
                print(total_urls_processed)
                print(url)
                # Добавляем текущую страницу в индекс
                self.addIndex(soup, url)

                # Обрабатываем ссылки на странице
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href and not href.startswith('#') and not href.startswith('mailto:') and not href.endswith('.apk'):
                        full_url = requests.compat.urljoin(url, href)
                        if full_url not in visited_urls and full_url not in next_depth_urls:
                            next_depth_urls.append(full_url)

                            # Добавляем ссылку между страницами
                            self.addLinkRef(url, full_url, link.get_text())

            urlList = next_depth_urls  # Переход на следующий уровень глубины

            if total_urls_processed >= maxUrls:
                print("Достигнут лимит обработанных URL.")
                break

    def initDB(self):
        # Инициализация таблиц в БД
        create_db()
        # self.url_dao.init_db()
        # self.word_dao.init_db()
        # self.word_location_dao.init_db()
        # self.link_dao.init_db()
        # self.link_words_dao.init_db()