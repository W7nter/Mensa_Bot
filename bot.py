import os
import requests
import asyncio
from bs4 import BeautifulSoup
from datetime import date, time
from pytz import timezone
import pandas as pd
from dotenv import load_dotenv
import logging
from peewee import SqliteDatabase, IntegerField, Model
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    AIORateLimiter,
)

# Loading Variables
load_dotenv()
token = os.environ["BOT_TOKEN"]
db_path = os.environ["DB_PATH"]


berlin = timezone("Europe/Berlin")
send_time = time(hour=11, tzinfo=berlin)


# Setup Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
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

# Users who recive vegitarian menu
class Veggi(Model):
    chat_id = IntegerField(unique=True)

    class Meta:
        database = db


# Functions to get Menu plan from website


def parse_menu():
    url = (
        "https://www.stwdo.de/mensa-cafes-und-catering/tu-dortmund/hauptmensa/"
        + str(date.today())
        + "?#canteen-plan-15"
    )
    try:
        r = requests.get(url)
    except:
        logging.critical("HTTP Request failed")

    # Parse lare part of data
    df_list = pd.read_html(r.text)
    df = df_list[0]

    # Parse Ingredients and create emoji
    soup = BeautifulSoup(r.text, features="lxml")
    ingredient_columns = soup.find("table").findAll(
        "td", class_="meals__column-supplies"
    )

    for idx, column in enumerate(ingredient_columns):
        ingredients = column.findAll("img")

        emoji = ""
        for ingrident in ingredients:
            new_emoji = {
                "Vegane Speise": "\N{broccoli}",
                "Mit Rindfleisch": "\N{cow face}",
                "Fleisch aus artgerechter Haltung": "\N{check mark}",
                "Mit Fisch bzw. Meeresfrüchten": "\N{fish}",
                "Ohne Fleisch": "\N{carrot}",
                "Mit Geflügel": "\N{chicken}",
                "Mit Schweinefleisch": "\N{pig face}",
                "Kinderteller": "\N{child}",
            }.get(ingrident["title"], "Missing")

            emoji = emoji + " " + new_emoji

        df.iloc[idx, 2] = emoji

    Beiwerkindex = df.index[df.Menü.eq("Beiwerke")][0]
    main_dishes = df.iloc[0:Beiwerkindex]
    side_dishes = df.iloc[
        Beiwerkindex + 1 :,
    ]

    return main_dishes, side_dishes


def gen_message(dishes):
    message = f""
    for idx, row in dishes.iterrows():
        message = (
            message
            + f"{row['Gerichte']}, {row['Art']} \n{row['Studierende']} {row['Bedienstete']} {row['Gäste']} \n\n"
        )

    return message


def check_fries(side_dishes):
    return side_dishes.iloc[:, 1].str.contains("Pommes frites").any()


# Setup functions to handle commands


# Start message
async def start_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"{update.effective_chat.id} sent start message")
    greeting_msg = "Hallo vom Mensabot! \nWas kann ich für dich tun?"
    await context.bot.send_message(chat_id=update.effective_chat.id, text=greeting_msg)


# Register new User for Menu
async def menu_signup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_client, created = Menu.get_or_create(chat_id=update.effective_chat.id)
    logging.info(f"{update.effective_chat.id} tried to register for Menu")

    if created:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst ab jetzt das Mensa Menü"
        )
        logging.info(f"{update.effective_chat.id} is now registered for Menu")

    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst bereits das Mensa Menü"
        )
        logging.info(f"{update.effective_chat.id} was already registered for Menu")


# Register new User for Fries
async def fries_signup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_client, created = Fries.get_or_create(chat_id=update.effective_chat.id)
    logging.info(f"{update.effective_chat.id} tried to register for Fries")

    if created:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst ab jetzt den Pommesalarm"
        )
        logging.info(f"{update.effective_chat.id} is now registered for Fries")

    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst bereits den Pommesalarm"
        )
        logging.info(f"{update.effective_chat.id} was already registered for Fries")


# Register new User for Veggi 
async def veggi_signup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_client, created = Veggi.get_or_create(chat_id=update.effective_chat.id)
    logging.info(f"{update.effective_chat.id} tried to register for Veggi")

    if created:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst ab jetzt das vegitarische Menu"
        )
        logging.info(f"{update.effective_chat.id} is now registered for Veggi")

    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst bereits das vegitarische Menü"
        )
        logging.info(f"{update.effective_chat.id} was already registered for Veggi")



# Remove User from Menu
async def menu_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = Menu.get(chat_id=update.effective_chat.id)
        client.delete_instance()

        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du erhälst nun das Menü nicht mehr"
        )
        logging.info(f"{update.effective_chat.id} was deleted from Menu")
    except:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Du hast das Menü noch nie erhalten"
        )
        logging.info(f"{update.effective_chat.id} was not signed up for Menu")


# Remove User From Fries
async def fries_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = Fries.get(chat_id=update.effective_chat.id)
        client.delete_instance()

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Du erhälst nun den Pommesalarm nicht mehr",
        )
        logging.info(f"{update.effective_chat.id} was deleted from fries")
    except:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Du hast den Pommesalarm noch nie erhalten",
        )
        logging.info(f"{update.effective_chat.id} was not signed up for Fries")

# Remove User From Veggi 
async def veggi_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = Veggi.get(chat_id=update.effective_chat.id)
        client.delete_instance()

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Du erhälst nun das vegitarische Menü nicht mehr",
        )
        logging.info(f"{update.effective_chat.id} was deleted from Veggi")
    except:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Du hast das vegitarische Menü noch nie erhalten",
        )
        logging.info(f"{update.effective_chat.id} was not signed up for Veggi")


# Functions to send Messages


def send_msg(context, database, msg_txt):
    # Generator to create all send message jobs
    for client in database.select():
        logging.info(f"Sending message to {client.chat_id}")
        yield context.bot.send_message(chat_id=client.chat_id, text=msg_txt)


async def menu_message(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Getting update for Menu messages")

    main, _ = parse_menu()
    message = gen_message(main)

    logging.info("Start sending menu")
    msg_gen = send_msg(context, Menu, message)
    context.application.create_task(asyncio.gather(*msg_gen))
    logging.info("Finished sending menu")

async def veggi_message(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Getting update for Veggi messages")

    main, _ = parse_menu()
    veggi_options = main.loc[main['Art'].str.contains('\N{carrot}') | main['Art'].str.contains('\N{broccoli}')]
    print(veggi_options)
    message = gen_message(veggi_options)

    logging.info("Start sending veggi")
    msg_gen = send_msg(context, Veggi, message)
    context.application.create_task(asyncio.gather(*msg_gen))
    logging.info("Finished sending veggi")


async def fries_message(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Getting update for Fries messages")

    _, side = parse_menu()

    if check_fries(side):
        logging.info("Start sending fries alert")
        message = "Es ist alles gut, die Welt ist in ordnung, es gibt Pommes"
        msg_gen = send_msg(context, Fries, message)
        context.application.create_task(asyncio.gather(*msg_gen))
        logging.info("Finished sending fries alert")

    else:
        logging.info("Start sending no fries alert")
        message = "\N{warning sign} Das ist keine Übung: Es gibt heute keine Pommes in der Mensa \N{warning sign}"
        msg_gen = send_msg(context, Fries, message)
        context.application.create_task(asyncio.gather(*msg_gen))
        logging.info("Finished sending no fries alert")



# shutdown function:
async def db_shutdown(application: Application):
    logging.info("Closing Data Base connection")
    db.close()


if __name__ == "__main__":
    db.connect()
    db.create_tables([Menu, Fries, Veggi])
    Menu.create_table(safe=True)
    Fries.create_table(safe=True)

    # Setup Telegram Bot
    Rate_Lim = AIORateLimiter(overall_max_rate=20, group_max_rate=5, max_retries=5)
    application = (
        ApplicationBuilder()
        .token(token)
        .http_version("1.1")
        .get_updates_http_version("1.1")
        .rate_limiter(Rate_Lim)
        .post_shutdown(db_shutdown)
        .build()
    )

    # Auto generated messages
    scheduler_kwargs = {"misfire_grace_time": 3600}
    job_queue = application.job_queue
    job_Menu = job_queue.run_daily(
        menu_message, time=send_time, days=(1, 2, 3, 4, 5), job_kwargs=scheduler_kwargs
    )
    job_Fries = job_queue.run_daily(
        fries_message, time=send_time, days=(1, 2, 3, 4, 5), job_kwargs=scheduler_kwargs
    )
    job_Fries = job_queue.run_daily(
        veggi_message, time=send_time, days=(1, 2, 3, 4, 5), job_kwargs=scheduler_kwargs
    )


    # Command Handler Functions
    start_handler = CommandHandler("start", start_msg)
    application.add_handler(start_handler)

    menu_signup_handler = CommandHandler("Menu", menu_signup)
    application.add_handler(menu_signup_handler)

    menu_rem_handler = CommandHandler("MenuStop", menu_rem)
    application.add_handler(menu_rem_handler)

    veggi_signup_handler = CommandHandler("Veggi", veggi_signup)
    application.add_handler(veggi_signup_handler)

    veggi_rem_handler = CommandHandler("VeggiStop", veggi_rem)
    application.add_handler(veggi_rem_handler)

    fries_signup_handler = CommandHandler("Pommes", fries_signup)
    application.add_handler(fries_signup_handler)

    fries_rem_handler = CommandHandler("PommesStop", fries_rem)
    application.add_handler(fries_rem_handler)

    application.run_polling()
