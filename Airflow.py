# Импортируем нужные библиотеки
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Указываем путь к файлу
TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

# Указываем аргументы
default_args = {
    'owner': 'e-astahov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 16),
    'schedule_interval': '15 10 * * *'
}

# Создаем функцию, внутри которой находятся функции - таски
@dag(default_args=default_args, catchup=False)
def e_astahov_lesson_3():
    @task()
    def get_data():
        games_df = pd.read_csv(TOP_1M_DOMAINS)
        # Определим год
        login = 'e-astahov'
        year = 1994 + hash(f'{login}') % 23
        # Оставим нужный год
        games_df = games_df[games_df['Year'] == year]

        return games_df

    @task()
    # Какая игра была самой продаваемой в этом году во всем мире?
    def get_top_sales_geme(games_df):
        top_game = games_df.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1)['Name']
        return top_game

    @task()
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_top_genre_eu(games_df):
        top_genre_eu = games_df.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending=False)
        max_EU_Sales = top_genre_eu.EU_Sales.max()
        top_genre_eu = top_genre_eu.query('EU_Sales == @max_EU_Sales')['Genre']
        return top_genre_eu

    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    def get_top_platform_na(games_df):
        top_platform_na = games_df.query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .sort_values('NA_Sales', ascending=False)
        max_NA_Sales = top_platform_na.NA_Sales.max()
        top_platform_na = top_platform_na.query('NA_Sales == @max_NA_Sales')['Platform']
        return top_platform_na

    @task()
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def get_top_mean_jp(games_df):
        top_mean_jp = games_df.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False)
        max_mean_sales_jp = top_mean_jp.JP_Sales.max()
        top_mean_jp = top_mean_jp.query('JP_Sales == @max_mean_sales_jp')['Publisher']
        return top_mean_jp

    @task()
    # Сколько игр продались лучше в Европе, чем в Японии?
    def get_count_games_sold_better_in_eu_than_in_jp(games_df):
        count_games = games_df.groupby('Name', as_index=False) \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
            .query('EU_Sales > JP_Sales')['Name'].count()
        return count_games

    @task()
    # Вывод результатов
    def print_data(top_game, top_genre_eu, top_platform_na, top_mean_jp, count_games):
        print(f'The best-selling game is {top_game}')
        print(f'Games of the genre {top_genre_eu} are the best-selling in Europe')
        print(f'The platform {top_platform_na} hast the most games that have sold more than a million copies')
        print(f'The publisher {top_mean_jp} has the highest sales in Japan')
        print(f'{count_games} games sold better in Europe than in Japan')

# Задаем порядок выполнения
    top_data = get_data()
    top_game = get_top_sales_geme(top_data)
    top_genre_eu = get_top_genre_eu(top_data)
    top_platform_na = get_top_platform_na(top_data)
    top_mean_jp = get_top_mean_jp(top_data)
    count_games = get_count_games_sold_better_in_eu_than_in_jp(top_data)

    print_data(top_game, top_genre_eu, top_platform_na, top_mean_jp, count_games)


e_astahov_lesson_3 = e_astahov_lesson_3()