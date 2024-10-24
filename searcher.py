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
        # Привести поисковый запрос к нижнему регистру и разбить на слова
        #queryString = queryString.lower()
       # wordsList = queryString.split(' ')

        # Получить идентификаторы искомых слов
       # wordsidList = self.getWordsIds(queryString)

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
        # Рассчитываем ранги с помощью метода locationScore
        m1Scores = self.locationScore(rowsLoc)

        # Создать список для последующей сортировки рангов и URL-адресов
        rankedScoresList = [(score, url) for url, score in m1Scores.items()]

        # Сортировка из словаря по убыванию
        rankedScoresList.sort(reverse=True, key=lambda pair: pair[0])  # Сортируем по первому элементу (score)

        # Вывод первых N результатов
        print("score, urlid, geturlname")
        for score, urlid in rankedScoresList[:10]:  # Первые 10 результатов
            print("{:.2f} {:>5}  {}".format(score, urlid, self.url_dao.get_url(urlid)))

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

        #self.commit()

    def pagerankScore(self, rows):
        # Получить значения pagerank
        pagerank_scores = {row[0]: row[1] for row in rows}

        # Нормализовать относительно максимума
        max_score = max(pagerank_scores.values())
        normalized_scores = {urlid: score / max_score for urlid, score in pagerank_scores.items()}

        return normalized_scores


    def __del__(self):
        """ Destructor to close the database connection """
        print("Searcher завершает работу.")