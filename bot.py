import os
import requests
from bs4 import BeautifulSoup
from datetime import date, time
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import logging
from peewee import SqliteDatabase, IntegerField, Model
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

# Loading Variables
load_dotenv()
token = os.environ['BOT_TOKEN']
db_path = os.environ['DB_PATH']

send_time = time(hour=10)


# Setup Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Setting up Database
db = SqliteDatabase(db_path)

# Users who recive menu
class Menu(Model):
    chat_id = IntegerField(unique=True)

    class Meta:
        database = db

# Users who recive Fry Alert
class Fries(Model):
    chat_id = IntegerField(unique=True)

    class Meta: 
        database = db


# Functions to get Menu plan from website

def parse_menu():
    url = "https://www.stwdo.de/mensa-cafes-und-catering/tu-dortmund/hauptmensa/" + str(date.today()) + "?#canteen-plan-15"
    try:
        r = requests.get(url)
    except:
        logging.critical("HTTP Request failed")

    # Parse lare part of data
    df_list = pd.read_html(r.text)
    df = df_list[0]

    # Parse Ingredients and create emoji
    soup = BeautifulSoup(r.text, features="lxml")
    ingredient_columns = soup.find('table').findAll('td', class_='meals__column-supplies')

    for idx, column in enumerate(ingredient_columns):
        ingredients = column.findAll('img')

        emoji = ""
        for ingrident in ingredients:
            new_emoji = {
                'Vegane Speise': '\N{broccoli}',
                'Mit Rindfleisch': '\N{cow face}',
                'Fleisch aus artgerechter Haltung': '\N{check mark}',
                'Mit Fisch bzw. Meeresfrüchten': '\N{fish}',
                'Ohne Fleisch': '\N{carrot}',
                'Mit Geflügel': '\N{chicken}',
                'Mit Schweinefleisch': '\N{pig face}',
                'Kinderteller': '\N{child}',
            }.get(ingrident['title'], 'Missing')

            emoji = emoji + ' ' + new_emoji
        
        df.iloc[idx,2] = emoji
                
    Beiwerkindex = df.index[df.Menü.eq('Beiwerke')][0]
    main_dishes = df.iloc[0:Beiwerkindex]
    side_dishes = df.iloc[Beiwerkindex+1: ,]

    return main_dishes, side_dishes
                
def gen_message(main_dishes):
    message = f""
    for idx in main_dishes.index:
        message = message + f"{main_dishes.iloc[idx, 1]}, {main_dishes.iloc[idx,2]} \n{main_dishes.iloc[idx, 3]} {main_dishes.iloc[idx, 4]} {main_dishes.iloc[idx, 5]} \n\n"

    return message 

def check_fries(side_dishes):
    return side_dishes.iloc[:,1].str.contains('Pommes frites').any()


# Setup functions to handle commands

# Register new User for Menu
async def menu_signup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_client, created = Menu.get_or_create(chat_id=update.effective_chat.id) 
    logging.info(f"{update.effective_chat.id} tried to register for Menu")

    if created:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst ab jetzt das Mensa Menü")
        logging.info(f"{update.effective_chat.id} is now registered for Menu")

    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst bereits das Mensa Menü")
        logging.info(f"{update.effective_chat.id} was already registered for Menu")

# Register new User for Fries 
async def fries_signup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_client, created = Fries.get_or_create(chat_id=update.effective_chat.id) 
    logging.info(f"{update.effective_chat.id} tried to register for Fries")

    if created:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst ab jetzt den Pommesalarm")
        logging.info(f"{update.effective_chat.id} is now registered for Fries")

    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst bereits den Pommesalarm")
        logging.info(f"{update.effective_chat.id} was already registered for Fries")

# Remove User from Menu
async def menu_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = Menu.get(chat_id=update.effective_chat.id)
        client.delete_instance()

        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst nun das Menü nicht mehr")
        logging.info(f"{update.effective_chat.id} was deleted from Menu")
    except:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du hast das Menü noch nie erhalten")
        logging.info(f"{update.effective_chat.id} was not signed up for Menu")

# Remove User From Fries
async def fries_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = Fries.get(chat_id=update.effective_chat.id)
        client.delete_instance()

        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du erhälst nun den Pommesalarm nicht mehr")
        logging.info(f"{update.effective_chat.id} was deleted from fries")
    except:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Du hast den Pommesalarm noch nie erhalten")
        logging.info(f"{update.effective_chat.id} was not signed up for Fries")


# Functions to send Messages

async def menu_message(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Getting update for Menu messages")

    main, _ = parse_menu()
    message = gen_message(main)

    for client in Menu.select():
        logging.info(f"Sending Menu to {client.chat_id}")
        await context.bot.send_message(chat_id=client.chat_id, text=message)

async def fries_message(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Getting update for Fries messages")

    _, side = parse_menu()

    if check_fries(side):
        for client in Fries.select():
            logging.info(f"Sending Fries message to {client.chat_id}")
            await context.bot.send_message(chat_id=client.chat_id, text="Es ist alles gut, die Welt ist in ordnung, es gibt Pommes")
    else:
        for client in Fries.select():
            logging.info(f"Sending no Fries Alarm to {client.chat_id}")
            await context.bot.send_message(chat_id=client.chat_id, text="\N{warning sign} Das ist keine Übung: Es gibt heute keine Pommes in der Mensa \N{warning sign}")
 

if __name__ == '__main__':
    db.connect()
    Menu.create_table(safe=True)
    Fries.create_table(safe=True)
 
    # Setup Telegram Bot
    application = ApplicationBuilder().token(token).build()


    # Auto generated messages
    job_queue = application.job_queue
    job_Menu = job_queue.run_daily(menu_message, time=send_time, days=(1,2,3,4,5))
    job_Fries = job_queue.run_daily(fries_message, time=send_time, days=(1,2,3,4,5) )


    # Command Handler Functions
    menu_signup_handler = CommandHandler('Menu', menu_signup)
    application.add_handler(menu_signup_handler)

    menu_rem_handler = CommandHandler('MenuStop', menu_rem)
    application.add_handler(menu_rem_handler)

    fries_signup_handler = CommandHandler('Pommes', fries_signup)
    application.add_handler(fries_signup_handler)

    fries_rem_handler = CommandHandler('PommesStop', fries_rem)
    application.add_handler(fries_rem_handler)

    application.run_polling()


