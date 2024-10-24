import sqlite3
import requests
from bs4 import BeautifulSoup
from DAO import UrlListDAO, WordListDAO, WordLocationDAO, LinkDAO, LinkWordsDAO
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

    def __del__(self):
        """ Destructor to close the database connection """
        print("Searcher завершает работу.")