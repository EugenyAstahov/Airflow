# Airflow

Используем Airflow для решения аналитических задач.

Путь до файла укажем вот так '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

Задания:

Сначала определим год, за какой будем смотреть данные.

Сделаем это вот так:
    в питоне выполним 1994 + hash(f‘{login}') % 23,  где {login} - ваш логин (или же папка с дагами)

Дальше составим DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:

- Какая игра была самой продаваемой в этом году во всем мире?
- Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
- На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
- У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
- Сколько игр продались лучше в Европе, чем в Японии?

Оформим так чтобы финальный таск писал в лог ответ на каждый вопрос. В DAG будет 7 тасков. По одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы. 
