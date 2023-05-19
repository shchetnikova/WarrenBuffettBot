from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
import requests
import json
import threading
import os
import asyncio
import aioschedule
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
import psycopg2
import db
from datetime import timedelta, date
import logging

# Создаем кнопку клавиатуры для добавления ценных бумаг в реестр.
button_add_security=KeyboardButton(text='/add_security')
# Создаем кнопку клавиатуры для запроса показателя эффективности ценных бумаг.
button_get_perf_index=KeyboardButton(text='/get_perfindex')
# Создаем встроенную клавиатуру с возможностью автоматического изменения размеров (для красивого отобрадения в телеграме), 
# затем добавляем вышесозданные кнопки на клавиатуру.
greet_kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button_add_security, button_get_perf_index)

TG_TOKEN='6028818641:AAEsoLi1s5XXg8MOgPqpNyMPPECpMND3cIU' # os.getenv('TG_TOKEN')
ALPHA_TOKEN='YXEDEJCNZUWUVZ2R'# os.getenv('ALPHA_TOKEN')

bot = Bot(token=TG_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

class UserState(StatesGroup):
    addSecurity=State()

async def is_security_exists(security: str):
    """ Функция проверки данных о ценной бумаге. Производит обращение к API alphavantage"""
    data = await request_security(security)
    return data.get('Error Message') == None and data.get('Note') == None

async def request_security(security: str):
    url ="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=" + security + "&apikey={" + ALPHA_TOKEN + "}"
    response = requests.get(url)
    data = json.loads(response.text)
    return data


@dp.message_handler(state=UserState.addSecurity)
async def process_add_security(message: types.Message):
    """ Функция обрабатывает сообщение с название ценной бумаги, которую пытается добавить пользователь"""
    # Проверяем запросом к API существование ценной бумаги.
    success = await is_security_exists(message.text)
    # Если API не предоставляет доступ для данного имени ценной бумаги.
    if not success:
        # Возвращаем сообщение пользователю и прекращаем выполнение функции.
        msg = "Такой бумаги не существует или API недоступно: " + message.text
        await message.reply(msg)
        await UserState.next()
        return
  
    if await db.insert_security(message.from_user.id, message.text):
        msg = "Бумага " + message.text + " успешно добавлена."
        await message.reply(msg)
        await UserState.next()
        perfindex = await calc_performance_index(message.from_user.id)
        logging.info("start update perf index")
        await db.upsert_perf_index(message.from_user.id, perfindex)
        logging.info("end update perf index")
    else:
        msg = "Бумага " + message.text + " не отслеживается или не существует." 
        await message.reply(msg)
        await UserState.next()

@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply("Воспользуйтесь запросами, представленными в меню!", reply_markup=greet_kb)
    await db.insert_new_user(message.from_user.id, message.from_user.username)


@dp.message_handler(commands=['help'])
async def process_help_command(message: types.Message):
    await message.reply("Вы можете добавлять ценную бумагу по названию в свой портфель, а так же получать индекс производительности портфеля.")

@dp.message_handler(commands=['add_security'])
async def add_security(message: types.Message):
    await message.reply("Введите название ценной бумаги, данные о которой вы хотели бы отслеживать: ")
    await UserState.addSecurity.set()

@dp.message_handler(commands=['get_perfindex'])
async def get_perfindex(message: types.Message):
    perfindex_tuple = await db.get_perfindex_for_user(message.from_user.id)
    await message.reply("Текущий показатель эффективности = " + str(perfindex_tuple[0]))

@dp.message_handler()
async def process_start_command(message: types.Message):
    await message.reply("Воспользуйтесь запросами, представленными в меню!", reply_markup=greet_kb)


@dp.message_handler()
async def echo_message(msg: types.Message):
    await bot.send_message(msg.from_user.id, "Воспользуйтесь запросами, представленными в меню")


async def get_date_of_last_accounting(request):
    """ Функция получения даты последнего измерения"""
    # Надеемся, что есть данные на сегодняшний день.
    last_date = date.today()
    iterations = 0
    # В цикле ищем последний день с данными в пределах 30 дней.
    if request.get('Time Series (Daily)') != None:
        while request.get('Time Series (Daily)').get(str(last_date)) == None:
            if iterations > 30:
                break
            iterations+=1
            last_date -= timedelta(days=1)
    return last_date


async def get_security_closing_price(request, date):
    logging.info("Called function get security's closing price")
    return float(request.get('Time Series (Daily)').get(str(date)).get("4. close"))

async def calc_performance_index(user_id: int):
    logging.info("Called calcing performance for user")
    """ Функция вычисления индекса производительности портфеля по id пользователя"""
    period_begin_closing_prices = []
    period_end_closing_prices = []
    # Запрос всех ценных бумаг пользователя из базы.
    user_securities = await db.get_all_securities(user_id)
    for s in user_securities:
        # Запрос для каждой бумаги в alphavantage.
        security_data = await request_security(s[0])
        # Вычисление граничных дат. Длина интервала - 30 дней.
        period_end = await get_date_of_last_accounting(security_data)
        period_begin = period_end - timedelta(days=30)
        # Парсинг цен закрытия ценных бумаг по двум датам.
        # Если пришедший ответ является информацией о ценной бумаге.
        if security_data.get("Time Series (Daily)") != None:
            current_period_begin_closing_price = await get_security_closing_price(security_data, period_begin)
            period_begin_closing_prices.append(current_period_begin_closing_price)        
            current_period_end_closing_price = await get_security_closing_price(security_data, period_end)
            period_end_closing_prices.append(current_period_end_closing_price)
        
    return await calc_performance_index_by_prices(period_begin_closing_prices, period_end_closing_prices)

async def calc_performance_index_by_prices(begin_prices: list, end_prices: list):
    logging.info("Called internal function for calcing performance")
    """ Функция вычисления индекса производительности портфеля по двум спискам данных цен закрытия ценной бумаги"""
    # Если список цен пустой или количество измерений не совпадает
    if len(begin_prices) == 0 or len(begin_prices) != len(end_prices):
        return 0.0
    _sum = 0.0
    # Вычисление индекса по формуле из методических материалов.
    for begin_price, end_price in zip(begin_prices, end_prices):
        _sum += (end_price - begin_price) / begin_price
    return _sum / len(begin_prices)

async def update():
    print("update called")
    """ Функция обновляет значения по всем пользователям раз в сутки"""
    logging.info("Called update for database")
    # Получаем список всех id пользователей.
    users_ids = await db.get_all_users_ids()
    for user_id in users_ids:
        # Для каждого id вычисляем индекс производительности портфеля.
        perf_index = await calc_performance_index(user_id)
        # Обновляем данные в базе.
        db.upsert_perf_index(user_id, perf_index)
 
async def scheduler():
    aioschedule.every(1).days.do(update)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)

async def on_startup(dp): 
    asyncio.create_task(scheduler())

def main():
    logging.info("run polling")
    executor.start_polling(dp, on_startup=on_startup)

if __name__ == '__main__':
    main()
