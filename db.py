import logging
import psycopg2
import datetime
import os

DB_NAME=os.getenv('DB_NAME')
DB_USERNAME=os.getenv('DB_USERNAME')
DB_PASSWORD=os.getenv('DB_PASSWORD')
DB_HOST=os.getenv('DB_HOST')

try:
    # Подключаем журналирование
    mylogs = logging.getLogger(__name__)
    file = logging.FileHandler(str(datetime.date.today()) +  ".log")
    mylogs.addHandler(file)
    logging.basicConfig(level=logging.INFO, filemode="a", format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Application is running")
    # Подключаемся к базе данных
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST)
    logging.info("Connection to database status: successful")
except Exception as ex:
    logging.error("Can`t establish connection to database")
    logging.error(str(ex))



async def insert_security(user_id: int, security_name: str):
    """ Функция вставки записи о ценной бумаге для пользователя"""
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO securities (user_id, _security) VALUES(%s, %s) ON CONFLICT DO NOTHING;', [user_id, security_name])
        conn.commit()
    except psycopg2.IntegrityError:
        print("security already exists")
        cursor.close()
        return False
    cursor.close()
    return True

async def upsert_perf_index(user_id: int, perf_index:float):
    """ Функция обновления или вставки данных об индексе эффективности портфеля пользователя"""
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO users (id, perfindex) \
                        VALUES({0}, {1}) \
                       ON CONFLICT (id) DO UPDATE SET perfindex = {1};'.format(user_id, perf_index))
    except Exception as ex:
        print("Handle any error" + str(ex))
        cursor.close()
        return
    conn.commit()
    cursor.close()

async def insert_new_user(user_id:int, username: str):
    """ Функция запроса в базу данных для вставки нового"""
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO users (id, name) VALUES(%s, %s) ON CONFLICT DO NOTHING;', [user_id, username])
    except psycopg2.IntegrityError as err:
        print("catch error: " + str(err))
        cursor.close()
        return
    conn.commit()
    print("commit db updates")
    print("Added new user")
    cursor.close()
 
async def get_all_users_ids():
    """ Функция запроса всех id пользователей"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM users;')
    ids =  cursor.fetchall()
    print("get all ids")
    cursor.close()
    return ids

async def get_perfindex_for_user(user_id:int):
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT perfindex FROM users WHERE (id=%s);', [user_id])
    except psycopg2.IntegrityError:
        cursor.close()
        return False
    perfindex=cursor.fetchone()
    cursor.close()
    return perfindex

async def get_all_securities(user_id: int):
    """ Функция запроса списка ценных бумаг пользователя"""
    cursor = conn.cursor()
    cursor.execute('SELECT _security FROM securities WHERE(user_id=%s);', [user_id])
    securities = cursor.fetchall()
    cursor.close()
    return securities

