# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from DAO import UrlListDAO
from DBcreate import create_db
from crawler import Crawler
from searcher import Searcher

def main():
    # Добавление URL
    create_db()
    db_file = 'search_engine.db'
    spider = Crawler(db_file)
    searcher = Searcher(db_file)

    # Инициализируем БД
    spider.initDB()


    #rowsLoc, wordsidList = searcher.getMatchRows("список новостей")

    # print(wordsidList)
    # for location in rowsLoc:
    #     print(location)

    #searcher.getSortedList("восток")
    # Начинаем сбор данных с заданного списка URL
    #start_urls = ['https://www.kommersant.ru/', 'https://history.eco/']
    #spider.crawl(start_urls, maxDepth=2)

    #spider.analyze_indexing()
    #spider.plot_graphs()
    #searcher.calculatePageRank()

    searcher.getMatchRows("список новостей")
    searcher.getSortedList("список новостей")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
