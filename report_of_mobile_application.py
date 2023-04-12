# импортируем библиотеки
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
import io
from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# напишем функцию для подключения к CH
def get_db(query):
    con = {'host': '*****',
           'password': '*****',
           'user': '*****',
           'database': '*****'
           }
    return ph.read_clickhouse(query, connection=con)


# напишем функцию для отправки ежедневного отчета
def report_dealy(chat_id=None):
    chat_id = chat_id or 'my_chat_id'

    # сохраним токин
    my_tokin = 'bot_tokin'
    # создадим бота
    bot = telegram.Bot(token=my_tokin)

    # расчитаем DAU
    query = '''
    SELECT toDate(time) AS date,
            'news feed' AS aplication, 
           COUNT(DISTINCT user_id) as cnt_users
    FROM simulator_20230220.feed_actions 
    GROUP BY date 

    UNION ALL

    SELECT toDate(time) AS date,
            'messanger'  AS aplication,
           arrayUniq(arrayConcat(groupUniqArray(user_id), groupUniqArray(reciever_id ))) AS cnt_users
    FROM simulator_20230220.message_actions 
    GROUP BY date 
    HAVING date != today()
    ORDER BY date, aplication

    '''
    # получим наши данные
    dau = get_db(query)

    # расчитаем Retention Rate

    # 1) удержания пользователей из ленты новостей
    query = '''
    SELECT start_date AS start_date,
           date - start_date AS lifetime,
           COUNT(DISTINCT user_id) AS active_users
    FROM

    (SELECT user_id,
           MIN(toDate(time)) AS start_date
    FROM simulator_20230220.feed_actions 
    GROUP BY user_id
    HAVING (start_date > today() - 7) AND (start_date != today())) t1

    JOIN

    (SELECT DISTINCT user_id,
           toDate(time) AS date
    FROM simulator_20230220.feed_actions) t2

    USING user_id

    GROUP BY start_date, lifetime
    '''
    # получим данные
    retention_feed_actions = get_db(query)
    # оставим только дату в столбце start_date
    retention_feed_actions['start_date'] = retention_feed_actions['start_date'].dt.date

    # 2) удержание для пользователей из мессенджера
    query = '''
    SELECT start_date AS start_date,
           date - start_date AS lifetime,
           COUNT(DISTINCT user_id) AS active_users
    FROM
    (SELECT DISTINCT user_id,
           MIN(toDate(time)) OVER (PARTITION BY user_id) AS start_date,
           toDate(time) AS date
    FROM simulator_20230220.message_actions

    UNION ALL

    SELECT DISTINCT reciever_id,
           MIN(toDate(time)) OVER (PARTITION BY reciever_id) AS start_date,
           toDate(time) AS date
    FROM simulator_20230220.message_actions)
    WHERE (start_date > today() - 7) AND (start_date != today())
    GROUP BY start_date, lifetime
    '''
    # получим данные
    retention_messenger = get_db(query)
    # оставим только дату в столбце start_date
    retention_messenger['start_date'] = retention_messenger['start_date'].dt.date

    # расчитаем все события в нашем приложении (просмотры и лайки, отправка сообщений)
    query = '''
    SELECT *
    FROM

    (SELECT toDate(time) AS date,
           countIf(user_id, action='view') AS views,
           countIf(user_id, action='like') AS likes,
           ROUND((likes / views), 3) AS ctr_views_likes
    FROM simulator_20230220.feed_actions 
    GROUP BY date ) t1

    JOIN

    (SELECT toDate(time) AS date,
           COUNT(user_id) AS all_messages
    FROM simulator_20230220.message_actions 
    GROUP BY date) t2

    USING date

    WHERE date != today()

    '''
    # получим данные
    all_events = get_db(query)
    # переведем столбец в формат только даты, чтобы на графике лучше отображалось
    all_events['date'] = all_events['date'].dt.date

    # посчитаем какая доля пользователей пользуются нашими приложениями
    query = '''
    SELECT DISTINCT user_id,
           CASE WHEN user_id IN (SELECT DISTINCT user_id FROM simulator_20230220.message_actions) THEN 'news feed and messendger'
                WHEN user_id IN (SELECT DISTINCT reciever_id FROM simulator_20230220.message_actions) THEN 'news feed and messendger'
                ELSE 'only news feed'
                END AS applications_uses
    FROM simulator_20230220.feed_actions

    UNION ALL

    SELECT DISTINCT user_id,
           CASE WHEN user_id IN (SELECT DISTINCT user_id FROM simulator_20230220.feed_actions) THEN 'news feed and messendger'
                ELSE 'only messendger'
                END AS applications_uses
    FROM simulator_20230220.message_actions

    UNION ALL

    SELECT DISTINCT reciever_id,
           CASE WHEN reciever_id IN (SELECT DISTINCT user_id FROM simulator_20230220.feed_actions) THEN 'news feed and messendger'
                ELSE 'only messendger'
                END AS applications_uses
    FROM simulator_20230220.message_actions
    '''
    # получим данные
    applications_uses = get_db(query)
    # агрегируем данные для круговой диаграммы
    percent_users = (
        applications_uses
        .groupby('applications_uses', as_index=False)
        .agg({'user_id': 'nunique'})
        .rename(columns={'user_id': 'cnt_users'})
    )

    # подготовим отчет за сегодня

    # создадим фигуру
    sns.set_style('whitegrid')
    fig = plt.figure(figsize=(15, 15), frameon=False)
    fig.suptitle(f'Отчет за {date.today() - timedelta(days=1)}', size=21)

    # добавим subplots
    ax1 = fig.add_subplot(321)
    ax2 = fig.add_subplot(322)
    ax3 = fig.add_subplot(323)
    ax4 = fig.add_subplot(324)
    ax5 = fig.add_subplot(325)
    ax6 = fig.add_subplot(326)
    # напишем заголовки для графиков
    ax1.set_title('DAU')
    ax2.set_title('CTR из просмотров в лайки')
    ax3.set_title('Удержание пользователей из "Ленты новостей"')
    ax4.set_title('Удержание пользователей из "Мессенджера"')
    ax5.set_title('Количество событий в приложении')
    ax6.set_title('Доля пользователей нашего приложения')

    # построим наши графики

    # график DAU
    sns.lineplot(data=dau, x='date', y='cnt_users', hue='aplication', ax=ax1)

    # построим график CTR из просмотров в лайки
    sns.lineplot(data=all_events, x='date', y='ctr_views_likes', ax=ax2)

    # график удержания из ленты новостей
    sns.lineplot(data=retention_feed_actions, x='lifetime', y='active_users', hue='start_date', ax=ax3)

    # график удержания из месенджера
    sns.lineplot(data=retention_messenger, x='lifetime', y='active_users', hue='start_date', ax=ax4)

    # напишем цикл и построим график с количеством событий
    columns = all_events.drop(columns=['date', 'ctr_views_likes']).columns
    for column in columns:
        sns.lineplot(data=all_events, x='date', y=column, label=column, ax=ax5)

    # создадим круговую диаграмму
    labels = percent_users['applications_uses'].unique()
    values = percent_users['cnt_users'].values
    explode = [.1, .1, .1]
    ax6.pie(values, labels=labels, autopct='%1.3f%%', shadow=True, explode=explode, startangle=90,
            colors=['orange', 'cyan', 'brown'])

    # сохраним график в буфер и отправим
    report_plots = io.BytesIO()
    plt.savefig(report_plots)
    report_plots.seek(0)
    report_plots.name = 'report_plots.pdf'
    plt.close()

    # сформируем отчет и отправим

    # сформируем текстовое сообщение для телеграмма
    msg = '''
    Доброе утро! 
    Направляю Вам отчет за вчерашний день, 
    по "Ленте новостей" и "Мессенджеру"
        '''

    # добавим к отчету ссылки на оперативные данные

    msg_dash = '''
    Ссылка на дашборты:

    Оперативные данные (лента новостей и мессенджер): 
    http://superset.lab.karpov.courses/r/3297

    Лента новостей - продуктовые метрики: 
    http://superset.lab.karpov.courses/r/3298
    '''

    bot.sendMessage(chat_id=chat_id, text=msg)
    bot.sendPhoto(chat_id=chat_id, photo=report_plots)
    bot.sendMessage(chat_id=chat_id, text=msg_dash)


# Автоматизируем наш отчет

# зададим дефолтные параметры
default_args = {
    'owner': 'i-skljannyj',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25)
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'


# созаддим dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def daily_report_iskl_telegtam():
    # добавим id группы
    group_chat_id = 'group_id'

    @task
    def report_bot():
        report_dealy(chat_id=group_chat_id)

    report_bot()


daily_report_iskl_telegtam = daily_report_iskl_telegtam()