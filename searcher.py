import sqlite3
import requests
from bs4 import BeautifulSoup
from DAO import UrlListDAO, WordListDAO, WordLocationDAO, LinkDAO, LinkWordsDAO, PageRankDAO
from DBcreate import create_db
import matplotlib.pyplot as plt
import numpy as np

class Searcher:
    def __init__(self, dbFileName):
        """ Initialize the Searcher with DAOs and database connection """
        # Establish a shared connection to the database
        self.dbFileName = dbFileName
        #self.connection = sqlite3.connect(dbFileName)

        # Initialize DAO classes with the shared connection
        self.url_dao = UrlListDAO(dbFileName)
        self.word_dao = WordListDAO(dbFileName)
        self.word_location_dao = WordLocationDAO(dbFileName)
        self.page_rank_dao = PageRankDAO(dbFileName)
        # Add additional DAO initializations if necessary (e.g., WordLocationDAO)

    def getWordsIds(self, queryString):
        """
        Получение идентификаторов для каждого слова в queryString.
        :param queryString: поисковый запрос пользователя
        :return: список идентификаторов rowid искомых слов
        """
        # Привести поисковый запрос к нижнему регистру
        queryString = queryString.lower()

        # Разделить на отдельные слова
        queryWordsList = queryString.split(" ")

        # Список для хранения результата
        rowidList = []

        # Для каждого слова получить его идентификатор из БД
        for word in queryWordsList:
            word_id = self.word_dao.get_word_id(word)  # Получаем ID слова через DAO

            if word_id is not None:
                rowidList.append(word_id)
                print(f"Слово '{word}' найдено с идентификатором: {word_id}")
            else:
                raise Exception(f"Слово '{word}' не найдено в базе данных!")

        return rowidList

    def getMatchRows(self, queryString):
        """
        Поиск комбинаций из всех искомых слов в проиндексированных url-адресах.
        :param queryString: поисковый запрос пользователя
        :return: список вхождений формата (urlId, loc_q1, loc_q2, ...), где loc_qN - позиция на странице N-го слова из поискового запроса.
        """

        wordsidList = self.getWordsIds(queryString)
        # Использовать DAO для поиска строк, соответствующих искомым словам
        rows = self.word_location_dao.get_match_rows(wordsidList)
        return rows, wordsidList

    def normalizeScores(self, scores, smallIsBetter=0):

        resultDict = dict()  # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore

        return resultDict

    def locationScore(self, rowsLoc):
        """
        Расчет минимального расстояния от начала страницы у комбинации искомых слов.
        :param rowsLoc: Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
                        (на основе результата getmatchrows())
        :return: словарь {UrlId1: мин. расстояния от начала для комбинации, UrlId2: мин. расстояния от начала для комбинации}
        """

        # Создать locationsDict - словарь с расположением от начала страницы упоминаний/комбинаций искомых слов
        locationsDict = {}

        # Поместить в словарь все ключи urlid с начальным значением сумм расстояний от начала страницы "1000000"
        for row in rowsLoc:
            urlId = row[0]  # предполагаем, что первый элемент - это urlId
            # Инициализируем значение на случай, если для этого URLId еще не было записано расстояние
            locationsDict[urlId] = 1000000

            # Получаем все позиции искомых слов (все кроме первого элемента)
            positions = row[1:]  # loc_q1, loc_q2, ...

            # Вычислить сумму дистанций каждого слова от начала страницы
            sum_distance = sum(positions)

            # Проверка, является ли найденная комбинация слов ближе к началу, чем предыдущие
            if sum_distance < locationsDict[urlId]:
                locationsDict[urlId] = sum_distance

        # Передать словарь дистанций в функцию нормализации, режим "чем больше, тем лучше"
        return self.normalizeScores(locationsDict, smallIsBetter=1)

    def getSortedList(self, queryString):
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке.
        :param queryString: поисковый запрос
        :return: отсортированный список URL
        """

        # Получить rowsLoc и wordids от getMatchRows(queryString)
        rowsLoc, wordids = self.getMatchRows(queryString)

        # Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        m1Scores = self.locationScore(rowsLoc)

        m2Scores = {}
        for row in rowsLoc:
            urlId = row[0]
            if urlId is not None:  # Проверяем, что urlid не None
                m2Scores[urlId] = self.page_rank_dao.get_page_rank(urlId)[0]  # Получаем ранк для каждого URL
            else:
                m2Scores[urlId] = 0  # Если urlid None, задаем 0

        # Создать список для последующей сортировки
        rankedScoresList = []
        for urlid in m1Scores.keys():
            # Получаем M1 и M2
            m1 = m1Scores.get(urlid, 0)  # Если нет M1, то 0
            m2 = m2Scores.get(urlid, 0)  # Если нет M2, то 0

            # Вычисляем M3
            m3 = (m1 + m2) / 2  # Среднее значение M1 и M2

            # Добавляем к списку кортеж с (M3, urlid, url_text)
            url_text = self.url_dao.get_url(urlid)  # Получаем текст URL
            rankedScoresList.append((m3, urlid, url_text))

        # Сортировка по M3 по убыванию
        rankedScoresList.sort(reverse=True, key=lambda pair: pair[0])

        print("urlid, M1, M2, M3, URL_text")
        for m3, urlid, url_text in rankedScoresList[:10]:  # Первые 10 результатов
            m1 = m1Scores.get(urlid, 0)  # Получаем M1 для текущего urlid
            m2 = m2Scores.get(urlid, 0)  # Получаем M2 для текущего urlid

            # Печатаем результат в нужном формате
            print("{:<5} {:.2f} {:.2f} {:.2f}  {}".format(urlid, m1, m2, m3, url_text))

        # Передаем первые три URL и queryString в highlight_words_in_html
        search_words = queryString.split()  # Разделяем строку запроса на слова
        for i, (m3, urlid, url_text) in enumerate(rankedScoresList[:3]):
            output_file = f"highlighted_words_{i + 1}.html"  # Название файла для сохранения
            self.highlight_words_in_html(url_text, search_words, output_file)

        return rankedScoresList  # Возвращаем отсортированный список, если это необходимо


    def calculatePageRank(self, iterations=5):
        # Подготовка БД ------------------------------------------
        # стираем текущее содержимое таблицы PageRank
        self.page_rank_dao.clear_page_rank()

        # Создаем индексы
        self.page_rank_dao.create_indexes()

        # В начальный момент ранг для каждого URL равен 1
        self.page_rank_dao.initialize_page_rank()

        # Цикл Вычисление PageRank в несколько итераций
        for i in range(iterations):
            print(f"Итерация {i + 1}")

            # Получаем все URL для обновления PageRank
            urlids = self.page_rank_dao.get_all_urlids()
            for urlid in urlids:
                pr = 0.15  # коэффициент
                linking_pr = 0  # начальный ранг для ссылающихся страниц

                # Получаем все уникальные ссылки, которые ссылаются на текущий urlid
                from_ids = self.page_rank_dao.get_linking_urls(urlid)

                for fromid in from_ids:
                    linking_score = self.page_rank_dao.get_page_rank(fromid)
                    linking_count = self.page_rank_dao.get_link_count(fromid)

                    if linking_count > 0:
                        linking_pr += linking_score / linking_count

                # Обновляем PR с учетом ссылающихся страниц
                pr += linking_pr * 0.85

                # Обновляем значение score в таблице pagerank БД
                self.page_rank_dao.update_page_rank(urlid, pr)

        # Вызов функции для нормализации всех значений PageRank
        self.pagerankScore()
        #self.commit()

    def highlight_words_in_html(self, urlid, search_words, output_file='highlighted_words.html'):
        # Получаем url_id из url
        url_id = urlid[0]

        # Получаем все слова, связанные с данным url_id
        print(url_id)
        words = self.word_location_dao.get_words_by_url(url_id)
        url = self.url_dao.get_url(url_id)

        # Создаем HTML-содержимое с CSS-стилем для выделения слов
        html_content = f"""
        <html>
        <head>
            <title>Слова для URL: {url}</title>
            <style>
                .highlight {{ color: blue; font-weight: bold; }}
            </style>
        </head>
        <body>
            <h1>Слова для URL: {url}</h1>
            <ul>
        """
        # Создаем список слов с выделением искомых
        for word in words:
            if word in search_words:
                html_content += f"<li class='highlight'>{word}</li>"  # Подсвечиваем искомое слово синим цветом
            else:
                html_content += f"<li>{word}</li>"

        html_content += """
            </ul>
        </body>
        </html>
        """
        # Сохраняем HTML в файл
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(html_content)

        print(f"HTML сохранен в файл: {output_file}")


    def pagerankScore(self):
        """Нормализует все значения PageRank в таблице pagerank."""
        rows = self.page_rank_dao.get_all_page_rank()
        if not rows:
            return

        max_score = max(score for _, score in rows)
        for urlid, score in rows:
            normalized_score = score / max_score
            self.page_rank_dao.update_page_rank(urlid, normalized_score)


    def __del__(self):
        """ Destructor to close the database connection """
        print("Searcher завершает работу.")