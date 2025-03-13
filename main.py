import discord
from discord import app_commands, ui
from discord.ext import commands
import os
from dotenv import load_dotenv
import asyncio
import logging
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db
from pytz import timezone
import re

MSK = timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%H:%M %d:%m:%Y',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.Formatter.converter = lambda *args: datetime.now(MSK).timetuple()

load_dotenv()

if firebase_json:
    cred = credentials.Certificate(json.loads(firebase_json))
    firebase_admin.initialize_app(cred)
    print("Firebase подключен!")
else:
    print("Ошибка: FIREBASE_CREDENTIALS не найден!")

cred = credentials.Certificate("firebase-adminsdk.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://crystal-stats-default-rtdb.firebaseio.com'
})
db_ref = db.reference()

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

TOKEN = os.getenv('DISCORD_TOKEN')
if not TOKEN:
    raise ValueError("Токен бота не найден в переменных окружения!")

GUILD_ID = 1232025601666322442  # ID сервера
ADMIN_ROLES = [1232400295477248192, 1232400297347780668, 1232400304369045616]
ALLKICK_ROLES = [1232400297347780668, 1232400295477248192]
AUDIT_CHANNEL_ID = 1232400429682262096  # Канал аудита
WELCOME_CHANNEL_ID = 1232400425345486949  # Канал для приветствия (#role)
PUNISHMENTS_CHANNEL_ID = 1232400465514336416  # Канал для выговоров
EVENT_CHANNEL_ID = 1233825801003339948  # Канал для ивентов
NOTIFICATION_CHANNEL_ID = 1348702274653913152  # ID канала для уведомлений
EVENTS_REF = db_ref.child("events")  # Ссылка на события в Firebase
EVENT_COOLDOWN_MINUTES = 50  # Кулдаун между ивентами в минутах

async def get_join_date(member: discord.Member):
    logging.info(f"Получение даты присоединения для {member.id}")
    join_date = member.joined_at
    if join_date:
        join_date = join_date.astimezone(MSK)
        return join_date.strftime('%H:%M %d:%m:%Y')
    return "Неизвестно"

async def get_event_count(user_id: str):
    logging.info(f"Получение количества ивентов для {user_id}")
    user_events_ref = db_ref.child("user_events").child(str(user_id))
    user_events_count = await asyncio.to_thread(user_events_ref.child("total_events").get) or 0
    return user_events_count

async def get_active_reprimands(user_id: str):
    logging.info(f"Получение активных выговоров для {user_id}")
    user_ref = db_ref.child("reprimands").child(str(user_id))
    user_reprimands = await asyncio.to_thread(user_ref.child("reprimands").get) or {}
    if isinstance(user_reprimands, list):
        reprimands_dict = {str(i): r for i, r in enumerate(user_reprimands)}
    else:
        reprimands_dict = user_reprimands
    active_reprimands = {idx: r for idx, r in reprimands_dict.items() if r.get("active", False)}
    return active_reprimands

def parse_time_to_minutes(time_str):
    match = re.match(r'(\d+)\sч\.\s(\d+)\sм\.', time_str)
    if match:
        hours, minutes = map(int, match.groups())
        return hours * 60 + minutes
    return 0

def format_minutes_to_hours(minutes):
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} ч. {mins} м."

def parse_stat_line(line):
    parts = [part.strip() for part in line.split('|')]
    if len(parts) != 4:
        return None
    name, static_id_part, time_str, value = parts
    static_id_match = re.search(r'#(\d+)', static_id_part)
    if not static_id_match:
        return None
    static_id = static_id_match.group(1)
    reports = int(value) if value.isdigit() else 0
    minutes = parse_time_to_minutes(time_str)
    return {"name": name, "static_id": static_id, "minutes": minutes, "reports": reports}

async def get_user_stats(discord_id: str):
    admins_ref = db_ref.child("admins")
    admins_data = await asyncio.to_thread(admins_ref.get) or {}
    
    for admin_id, admin_data in admins_data.items():
        if admin_data.get("user_id") == discord_id:
            static_id = admin_data.get("static_id")
            if static_id:
                stats_ref = db_ref.child("user_stats").child(static_id)
                stats_data = await asyncio.to_thread(stats_ref.get) or {}
                return static_id, stats_data
    return None, {}

async def check_active_events():
    events = await asyncio.to_thread(EVENTS_REF.get) or {}
    now = datetime.now(MSK)
    for event_id, event_data in events.items():
        if event_data.get("active", False):
            event_time = datetime.fromisoformat(event_data["timestamp"]).astimezone(MSK)
            if now >= event_time:
                return True, event_id  # Есть активный ивент
    return False, None

async def check_scheduled_events():
    events = await asyncio.to_thread(EVENTS_REF.get) or {}
    now = datetime.now(MSK)
    for event_id, event_data in events.items():
        if event_data.get("active", False):  # Считаем только активные (не отмененные) ивенты
            event_time = datetime.fromisoformat(event_data["timestamp"]).astimezone(MSK)
            if now < event_time:  # Ивент еще не начался
                return True, event_time  # Есть запланированный ивент, возвращаем его время
    return False, None

async def get_last_event_completion_time():
    events = await asyncio.to_thread(EVENTS_REF.get) or {}
    last_completion_time = None
    for event_data in events.values():
        if "completed_at" in event_data:
            completed_at = datetime.fromisoformat(event_data["completed_at"]).astimezone(MSK)
            if last_completion_time is None or completed_at > last_completion_time:
                last_completion_time = completed_at
    return last_completion_time

class WelcomeModalJoin(ui.Modal, title="Данные нового пользователя"):
    static_id = ui.TextInput(label="Статический ID", placeholder="Введите статический ID...", required=True)
    nickname = ui.TextInput(label="Никнейм на сервере", placeholder="Введите никнейм...", required=True)
    entry_method = ui.TextInput(label="Способ вступления", placeholder="Обзвон или Восстановление", required=True)
    level = ui.TextInput(label="Уровень (1-10)", placeholder="От 1 до 10", required=True)

    def __init__(self, member_id: str, date_joined: str):
        super().__init__()
        self.member_id = member_id
        self.date_joined = date_joined

    async def on_submit(self, interaction: discord.Interaction):
        try:
            level = int(self.level.value)
            if not 1 <= level <= 10:
                raise ValueError("Уровень должен быть числом от 1 до 10!")
            entry_method = self.entry_method.value.strip()
            if entry_method not in ["Обзвон", "Восстановление"]:
                raise ValueError("Способ вступления должен быть 'Обзвон' или 'Восстановление'!")
            
            admin_data = {
                "static_id": self.static_id.value,
                "nickname": self.nickname.value,
                "entry_method": entry_method,
                "level": level,
                "date_added": datetime.now(MSK).strftime('%H:%M %d:%m:%Y') + "Z",
                "user_id": str(self.member_id),
                "date_joined": self.date_joined
            }
            admin_ref = db_ref.child("admins").child(self.static_id.value)
            await asyncio.to_thread(admin_ref.set, admin_data)
            
            channel = bot.get_channel(AUDIT_CHANNEL_ID)
            if channel:
                embed = discord.Embed(title="Новый Пользователь", color=discord.Color.green())
                embed.add_field(name="Статический ID", value=self.static_id.value, inline=False)
                embed.add_field(name="Никнейм", value=self.nickname.value, inline=False)
                embed.add_field(name="Способ вступления", value=entry_method, inline=False)
                embed.add_field(name="Уровень", value=str(level), inline=False)
                embed.add_field(name="Discord ID", value=self.member_id, inline=False)
                embed.add_field(name="Дата присоединения", value=self.date_joined, inline=False)
                embed.set_footer(text=f"Заполнено: {interaction.user} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
                await channel.send(embed=embed)
                await interaction.response.send_message("Данные успешно отправлены!", ephemeral=True)
            else:
                await interaction.response.send_message("Ошибка: канал аудита не найден.", ephemeral=True)
        except ValueError as ve:
            await interaction.response.send_message(f"Ошибка валидации: {str(ve)}", ephemeral=True)
        except Exception as e:
            logging.error(f"Ошибка при обработке данных: {e}")
            await interaction.response.send_message("Что-то пошло не так.", ephemeral=True)

class WelcomeModalKick(ui.Modal, title="Данные после кика"):
    static_id = ui.TextInput(label="Статический ID", placeholder="Введите статический ID...", required=True)
    nickname = ui.TextInput(label="Никнейм на сервере", placeholder="Введите никнейм...", required=True)
    kick_reason = ui.TextInput(label="Причина кика", placeholder="Введите причину кика...", required=True, style=discord.TextStyle.paragraph)
    admin_level = ui.TextInput(label="Уровень администратора (1-10)", placeholder="От 1 до 10", required=True)

    def __init__(self, member_id: str, date_joined: str):
        super().__init__()
        self.member_id = member_id
        self.date_joined = date_joined

    async def on_submit(self, interaction: discord.Interaction):
        try:
            level = int(self.admin_level.value)
            if not 1 <= level <= 10:
                raise ValueError("Уровень должен быть числом от 1 до 10!")
            
            admin_data = {
                "static_id": self.static_id.value,
                "nickname": self.nickname.value,
                "kick_reason": self.kick_reason.value,
                "admin_level": level,
                "date_added": datetime.now(MSK).strftime('%H:%M %d:%m:%Y') + "Z",
                "user_id": str(self.member_id),
                "date_joined": self.date_joined
            }
            admin_ref = db_ref.child("admins").child(self.static_id.value)
            await asyncio.to_thread(admin_ref.set, admin_data)
            
            channel = bot.get_channel(AUDIT_CHANNEL_ID)
            if channel:
                embed = discord.Embed(title="Данные после кика", color=discord.Color.orange())
                embed.add_field(name="Статический ID", value=self.static_id.value, inline=False)
                embed.add_field(name="Никнейм", value=self.nickname.value, inline=False)
                embed.add_field(name="Причина кика", value=self.kick_reason.value, inline=False)
                embed.add_field(name="Уровень администратора", value=str(level), inline=False)
                embed.add_field(name="Discord ID", value=self.member_id, inline=False)
                embed.add_field(name="Дата присоединения", value=self.date_joined, inline=False)
                embed.set_footer(text=f"Заполнено: {interaction.user} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
                await channel.send(embed=embed)
                await interaction.response.send_message("Данные успешно отправлены!", ephemeral=True)
            else:
                await interaction.response.send_message("Ошибка: канал аудита не найден.", ephemeral=True)
        except ValueError as ve:
            await interaction.response.send_message(f"Ошибка валидации: {str(ve)}", ephemeral=True)
        except Exception as e:
            logging.error(f"Ошибка при обработке данных: {e}")
            await interaction.response.send_message("Что-то пошло не так.", ephemeral=True)

class WelcomeButton(ui.Button):
    def __init__(self, new_member_id: int, is_kick: bool = False, date_joined: str = None):
        super().__init__(label="Заполнить данные", style=discord.ButtonStyle.primary, custom_id=f"welcome_button_{new_member_id}_{'kick' if is_kick else 'join'}_{date_joined or ''}")
        self.new_member_id = new_member_id
        self.is_kick = is_kick
        self.date_joined = date_joined

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id == self.new_member_id:
            await interaction.response.send_message("Вы не можете заполнять свои данные!", ephemeral=True)
            return
        if not any(role.id in ADMIN_ROLES for role in interaction.user.roles):
            await interaction.response.send_message("У вас нет прав для заполнения данных!", ephemeral=True)
            return
        if self.is_kick:
            modal = WelcomeModalKick(member_id=str(self.new_member_id), date_joined=self.date_joined or await get_join_date(interaction.guild.get_member(self.new_member_id)))
        else:
            modal = WelcomeModalJoin(member_id=str(self.new_member_id), date_joined=self.date_joined or await get_join_date(interaction.guild.get_member(self.new_member_id)))
        await interaction.response.send_modal(modal)

        self.disabled = True
        for item in self.view.children:
            item.disabled = True
        await interaction.message.edit(view=self.view)

class ReprimandModal(ui.Modal, title="Выдача выговора"):
    reprimand_type = ui.TextInput(label="Тип выговора", placeholder="Введите 'устный' или 'строгий'", required=True)
    reason = ui.TextInput(label="Причина", placeholder="Введите причину...", required=True, style=discord.TextStyle.paragraph)

    def __init__(self, member_id):
        super().__init__()
        self.member_id = member_id

    async def on_submit(self, interaction: discord.Interaction):
        member = await bot.fetch_user(self.member_id) or interaction.guild.get_member(self.member_id)
        if not member:
            await interaction.response.send_message("Пользователь не найден!", ephemeral=True)
            return
        reprimand_type = self.reprimand_type.value.strip().lower()
        if reprimand_type not in ["устный", "строгий"]:
            await interaction.response.send_message("Тип выговора должен быть 'устный' или 'строгий'!", ephemeral=True)
            return
        if not any(role.id in ADMIN_ROLES for role in interaction.user.roles):
            await interaction.response.send_message("У вас нет прав для выдачи выговоров!", ephemeral=True)
            return
        now = datetime.now(MSK)
        expiration_days = 7 if reprimand_type == "устный" else 14
        reprimand_type_value = "oral" if reprimand_type == "устный" else "strict"
        expiration_date = now + timedelta(days=expiration_days)
        reprimand_data = {
            "reason": self.reason.value,
            "date": datetime.now(MSK).strftime('%H:%M %d:%m:%Y') + "Z",
            "expiration_date": expiration_date.strftime('%H:%M %d:%m:%Y') + "Z",
            "active": True,
            "issuer_id": str(interaction.user.id),
            "type": reprimand_type_value
        }
        user_ref = db_ref.child("reprimands").child(str(self.member_id))
        user_reprimands = await asyncio.to_thread(user_ref.child("reprimands").get) or {}
        if isinstance(user_reprimands, list):
            user_reprimands = {str(i): r for i, r in enumerate(user_reprimands)}
        reprimand_count = len(user_reprimands)
        user_reprimands[str(reprimand_count)] = reprimand_data
        active_oral_reprimands = [r for r in user_reprimands.values() if r["type"] == "oral" and r["active"]]
        if reprimand_type == "устный" and len(active_oral_reprimands) >= 3:
            oral_count = 0
            for idx in list(user_reprimands.keys()):
                if user_reprimands[idx]["type"] == "oral" and user_reprimands[idx]["active"]:
                    del user_reprimands[idx]
                    oral_count += 1
                if oral_count == 3:
                    break
            reprimand_count = len(user_reprimands)
            strict_reprimand = {
                "reason": "Накопление 3 устных выговоров",
                "date": datetime.now(MSK).strftime('%H:%M %d:%m:%Y') + "Z",
                "expiration_date": (now + timedelta(days=14)).strftime('%H:%M %d:%m:%Y') + "Z",
                "active": True,
                "issuer_id": str(interaction.user.id),
                "type": "strict"
            }
            user_reprimands[str(reprimand_count)] = strict_reprimand
            channel = bot.get_channel(PUNISHMENTS_CHANNEL_ID)
            if channel:
                await channel.send(f"{member.mention} накопил 3 устных выговоров. Они заменены на 1 строгий выговор.")
        reindexed_reprimands = {str(i): v for i, v in enumerate(user_reprimands.values())}
        await asyncio.to_thread(user_ref.child("reprimands").set, reindexed_reprimands)
        active_oral = sum(1 for r in user_reprimands.values() if r["type"] == "oral" and r["active"])
        active_strict = sum(1 for r in user_reprimands.values() if r["type"] == "strict" and r["active"])
        channel = bot.get_channel(PUNISHMENTS_CHANNEL_ID)
        if channel:
            embed = discord.Embed(title=f"Выдан {'Устный' if reprimand_type == 'устный' else 'Строгий'} выговор", color=discord.Color.red())
            embed.add_field(name="Пользователь", value=member.mention, inline=False)
            embed.add_field(name="Причина", value=self.reason.value, inline=False)
            embed.add_field(name="Истекает", value=expiration_date.strftime('%H:%M %d:%m:%Y'), inline=False)
            embed.add_field(name="Общее количество активных выговоров", value=f"Устные: {active_oral}\nСтрогие: {active_strict}", inline=False)
            embed.set_footer(text=f"Выдал: {interaction.user} | {now.strftime('%H:%M %d:%m:%Y')}")
            await channel.send(embed=embed)
        try:
            await member.send(f"Вам выдан {'устный' if reprimand_type == 'устный' else 'строгий'} выговор за: {self.reason.value}. Истекает: {expiration_date.strftime('%H:%M %d:%m:%Y')}")
        except:
            logging.warning(f"Не удалось отправить DM {member}")
        await interaction.response.send_message(f"Выговор выдан {member.mention}!", ephemeral=True)

class ReprimandButton(ui.Button):
    def __init__(self, member_id: int):
        super().__init__(label="Открыть", style=discord.ButtonStyle.primary, custom_id=f"open_reprimand_modal_{member_id}")
        self.member_id = member_id

    async def callback(self, interaction: discord.Interaction):
        if not any(role.id in ADMIN_ROLES for role in interaction.user.roles):
            await interaction.response.send_message("У вас нет прав для выдачи выговоров!", ephemeral=True)
            return
        modal = ReprimandModal(self.member_id)
        await interaction.response.send_modal(modal)
        try:
            await interaction.message.delete()
        except:
            pass

class HourSelect(ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"{hour:02d}", value=str(hour)) for hour in range(24)]
        super().__init__(placeholder="Выберите час", min_values=1, max_values=1, options=options, custom_id="hour_select")

    async def callback(self, interaction: discord.Interaction):
        self.view.hour = int(self.values[0])
        await interaction.response.defer()

class MinuteSelect(ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"{minute:02d}", value=str(minute)) for minute in range(0, 60, 5)]
        super().__init__(placeholder="Выберите минуты", min_values=1, max_values=1, options=options, custom_id="minute_select")

    async def callback(self, interaction: discord.Interaction):
        self.view.minute = int(self.values[0])
        await interaction.response.defer()

class CancelEventButton(ui.Button):
    def __init__(self, event_id, creator_id, participants, creation_time):
        super().__init__(label="Отменить мероприятие", style=discord.ButtonStyle.red, custom_id=f"cancel_event_{event_id}")
        self.event_id = event_id
        self.creator_id = creator_id
        self.participants = participants
        self.creation_time = creation_time

    async def callback(self, interaction: discord.Interaction):
        current_time = datetime.now(MSK)
        time_difference = (current_time - self.creation_time).total_seconds()
        if time_difference > 24 * 3600:
            await interaction.response.send_message("Срок отмены мероприятия истек (24 часа)!", ephemeral=True)
            self.disabled = True
            await interaction.message.edit(view=self.view)
            return

        if not (interaction.user.id in self.participants or any(role.id in ADMIN_ROLES for role in interaction.user.roles)):
            await interaction.response.send_message("У вас нет прав для отмены этого мероприятия!", ephemeral=True)
            return

        try:
            event_ref = EVENTS_REF.child(self.event_id)
            event_data = await asyncio.to_thread(event_ref.get)
            if not event_data:
                await interaction.response.send_message("Мероприятие уже было удалено!", ephemeral=True)
                return
            if not event_data.get("active", True):
                await interaction.response.send_message("Мероприятие уже было отменено ранее!", ephemeral=True)
                return

            # Уменьшаем total_events для всех участников и создателя
            all_users = [self.creator_id] + self.participants
            for user_id in all_users:
                user_events_ref = db_ref.child("user_events").child(str(user_id))
                user_events_count = await asyncio.to_thread(user_events_ref.child("total_events").get) or 0
                if user_events_count > 0:
                    await asyncio.to_thread(user_events_ref.update, {"total_events": int(user_events_count) - 1})
                    logging.info(f"Уменьшен total_events для пользователя {user_id} до {int(user_events_count) - 1}")

            # Удаляем мероприятие из базы данных
            await asyncio.to_thread(event_ref.delete)

            channel = bot.get_channel(EVENT_CHANNEL_ID)
            if channel:
                embed = discord.Embed(title="Мероприятие отменено", color=discord.Color.red())
                embed.add_field(name="Название", value=event_data["name"], inline=False)
                embed.add_field(name="Время проведения", value=event_data["time"], inline=False)
                embed.add_field(name="Участники", value=", ".join([f"<@{user_id}>" for user_id in self.participants]), inline=False)
                embed.set_footer(text=f"Отменил: {interaction.user} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
                await channel.send(embed=embed)
                await interaction.response.send_message("Мероприятие успешно отменено!", ephemeral=True)
            else:
                await interaction.response.send_message("Ошибка: канал ивентов не найден.", ephemeral=True)

            logging.info(f"Мероприятие {self.event_id} отменено пользователем {interaction.user.id}")
            self.disabled = True
            await interaction.message.edit(view=self.view)
        except Exception as e:
            logging.error(f"Ошибка при отмене мероприятия {self.event_id}: {e}")
            await interaction.response.send_message(f"Что-то пошло не так: {str(e)}", ephemeral=True)

class TimeSelectView(ui.View):
    def __init__(self, event_name, creator_id, participants):
        super().__init__(timeout=60.0)
        self.event_name = event_name
        self.creator_id = creator_id
        self.participants = participants
        self.hour = None
        self.minute = None
        self.add_item(HourSelect())
        self.add_item(MinuteSelect())

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        await self.message.edit(content="Время выбора истекло.", view=self)

    @ui.button(label="Подтвердить", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        if self.hour is None or self.minute is None:
            await interaction.response.send_message("Пожалуйста, выберите час и минуты!", ephemeral=True)
            return

        try:
            today = datetime.now(MSK).replace(hour=0, minute=0, second=0, microsecond=0)
            event_time = today.replace(hour=self.hour, minute=self.minute)

            current_time = datetime.now(MSK)
            time_difference = (event_time - current_time).total_seconds()
            if time_difference < 0:
                await interaction.response.send_message("Нельзя создать мероприятие в прошлом!", ephemeral=True)
                return

            all_users = [self.creator_id] + self.participants
            for user_id in all_users:
                user_events_ref = db_ref.child("user_events").child(str(user_id))
                user_events_count = await asyncio.to_thread(user_events_ref.child("total_events").get) or 0
                await asyncio.to_thread(user_events_ref.update, {"total_events": int(user_events_count) + 1})

            event_data = {
                "name": self.event_name,
                "time": event_time.strftime("%H:%M"),
                "timestamp": event_time.isoformat(),
                "creator_id": self.creator_id,
                "participants": self.participants,
                "active": True
            }
            event_ref = EVENTS_REF.push()
            await asyncio.to_thread(event_ref.set, event_data)
            event_id = event_ref.key
            creation_time = datetime.now(MSK)

            channel = bot.get_channel(EVENT_CHANNEL_ID)
            if channel:
                creator_events = await asyncio.to_thread(db_ref.child("user_events").child(str(self.creator_id)).child("total_events").get) or 0
                embed = discord.Embed(title="Новое мероприятие", color=discord.Color.blue())
                embed.add_field(name="Название", value=self.event_name, inline=False)
                embed.add_field(name="Время проведения", value=event_time.strftime("%H:%M"), inline=False)
                embed.add_field(name="Участники", value=", ".join([f"<@{user_id}>" for user_id in self.participants]), inline=False)
                embed.add_field(name="Всего ивентов создателя", value=str(creator_events), inline=False)
                embed.set_footer(text=f"Создано: {interaction.user} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
                
                cancel_view = ui.View(timeout=24 * 3600)
                cancel_view.add_item(CancelEventButton(event_id=event_id, creator_id=self.creator_id, participants=self.participants, creation_time=creation_time))
                
                await channel.send(embed=embed, view=cancel_view)
                await interaction.response.send_message("Мероприятие успешно создано!", ephemeral=True)
            else:
                await interaction.response.send_message("Ошибка: канал ивентов не найден.", ephemeral=True)

            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)
        except Exception as e:
            logging.error(f"Ошибка при создании мероприятия: {e}")
            await interaction.response.send_message(f"Что-то пошло не так: {str(e)}", ephemeral=True)

class EventModal(ui.Modal, title="Создание мероприятия"):
    event_name = ui.TextInput(label="Название мероприятия", placeholder="Введите название...", required=True)

    def __init__(self, participants):
        super().__init__()
        self.participants = participants

    async def on_submit(self, interaction: discord.Interaction):
        try:
            creator_id = str(interaction.user.id)
            view = TimeSelectView(self.event_name.value, creator_id, self.participants)
            await interaction.response.send_message("Выберите время мероприятия:", view=view, ephemeral=True)
            view.message = await interaction.original_response()
        except Exception as e:
            logging.error(f"Ошибка при открытии выбора времени: {e}")
            await interaction.response.send_message(f"Что-то пошло не так: {str(e)}", ephemeral=True)

class EventButton(ui.Button):
    def __init__(self, participants):
        super().__init__(label="Заполнить данные", style=discord.ButtonStyle.primary, custom_id="open_event_modal")
        self.participants = participants

    async def callback(self, interaction: discord.Interaction):
        modal = EventModal(self.participants)
        await interaction.response.send_modal(modal)
        try:
            await interaction.message.delete()
        except:
            pass

@app_commands.command(name="menu", description="Посмотреть свои выговоры, ивенты, дату присоединения и статистику")
async def menu(interaction: discord.Interaction):
    try:
        logging.info(f"Команда /menu вызвана пользователем {interaction.user.id} в канале {interaction.channel_id}")
        user = interaction.user
        user_id = str(user.id)

        join_date = await get_join_date(user)
        event_count = await get_event_count(user_id)
        active_reprimands = await get_active_reprimands(user_id)
        static_id, stats_data = await get_user_stats(user_id)

        embed = discord.Embed(title=f"Информация о {user.display_name}", color=discord.Color.blue())
        embed.add_field(name="Дата присоединения", value=join_date, inline=False)
        embed.add_field(name="Проведено ивентов", value=str(event_count), inline=False)

        if active_reprimands:
            reprimands_text = ""
            for idx, r in active_reprimands.items():
                reprimand_type = "Устный" if r.get("type") == "oral" else "Строгий"
                issuer = bot.get_user(int(r.get("issuer_id", "0"))) or "Неизвестен"
                reprimands_text += f"**Выговор {int(idx) + 1} ({reprimand_type})**\nПричина: {r.get('reason')}\nДата: {r.get('date')}\nИстекает: {r.get('expiration_date')}\nВыдал: {issuer}\n\n"
            embed.add_field(name="Активные выговоры", value=reprimands_text, inline=False)
        else:
            embed.add_field(name="Активные выговоры", value="Нет активных выговоров", inline=False)

        if static_id and stats_data:
            total_minutes = stats_data.get("total_minutes", 0)
            total_reports = stats_data.get("total_reports", 0)
            embed.add_field(
                name="Общая статистика",
                value=f"Часы: {format_minutes_to_hours(total_minutes)}\nРепорты: {total_reports}",
                inline=False
            )

            seven_days_ago = datetime.now(MSK) - timedelta(days=7)
            recent_minutes = 0
            recent_reports = 0
            history = stats_data.get("history", [])
            for entry in history:
                entry_date = datetime.strptime(
                    entry["date"].replace("Z", ""), '%H:%M %d:%m:%Y'
                ).replace(tzinfo=MSK)
                if entry_date >= seven_days_ago:
                    recent_minutes += entry.get("added_minutes", 0)
                    recent_reports += entry.get("added_reports", 0)
            embed.add_field(
                name="За последние 7 дней",
                value=f"Часы: {format_minutes_to_hours(recent_minutes)}\nРепорты: {recent_reports}",
                inline=False
            )

            if history:
                last_entry = history[-1]
                embed.add_field(
                    name="Последнее обновление",
                    value=f"Дата: {last_entry['date']}\nЧасы: {format_minutes_to_hours(last_entry['added_minutes'])}\nРепорты: {last_entry['added_reports']}",
                    inline=False
                )
        else:
            embed.add_field(name="Статистика", value="Нет данных о статистике (привяжите static_id через /link_stats).", inline=False)

        embed.set_footer(text=f"Запросил: {user.display_name} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logging.info(f"Пользователь {user_id} успешно получил информацию через /menu")
    except Exception as e:
        logging.error(f"Ошибка в команде /menu: {e}")
        await interaction.response.send_message("Произошла ошибка при выполнении команды.", ephemeral=True)

@app_commands.command(name="import_stats", description="Импортировать статистику с другого сервера")
@app_commands.checks.has_any_role(*ADMIN_ROLES)
async def import_stats(interaction: discord.Interaction, stats_text: str):
    try:
        logging.info(f"Команда /import_stats вызвана пользователем {interaction.user.id} с текстом: {stats_text}")
        lines = re.split(r'(?=\b[A-Za-z]+\s*\|)', stats_text)
        updated_users = 0
        updated_ids = []

        for line in lines:
            line = line.strip()
            if not line:
                continue
            stat_data = parse_stat_line(line)
            if not stat_data:
                logging.warning(f"Некорректная строка статистики: {line}")
                continue

            user_id = stat_data["static_id"]
            user_ref = db_ref.child("user_stats").child(user_id)
            existing_data = await asyncio.to_thread(user_ref.get) or {}

            history_entry = {
                "date": datetime.now(MSK).strftime('%H:%M %d:%m:%Y'),
                "added_minutes": stat_data["minutes"],
                "added_reports": stat_data["reports"]
            }

            new_data = {
                "name": stat_data["name"],
                "total_minutes": existing_data.get("total_minutes", 0) + stat_data["minutes"],
                "total_reports": existing_data.get("total_reports", 0) + stat_data["reports"],
                "last_updated": datetime.now(MSK).strftime('%H:%M %d:%m:%Y'),
                "history": existing_data.get("history", []) + [history_entry]
            }
            await asyncio.to_thread(user_ref.set, new_data)
            updated_users += 1
            updated_ids.append(user_id)
            logging.info(f"Обновлена статистика для статического ID {user_id}: {new_data}")

        notification_channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)
        if notification_channel:
            embed = discord.Embed(
                title="Статистика обновлена",
                description=f"Пользователь {interaction.user.mention} импортировал статистику.\nОбновлено записей: {updated_users}\nОбновленные ID: {', '.join(updated_ids)}",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Время: {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
            await notification_channel.send(embed=embed)
            logging.info(f"Уведомление отправлено в канал {NOTIFICATION_CHANNEL_ID}")
        else:
            logging.warning(f"Канал с ID {NOTIFICATION_CHANNEL_ID} не найден")

        await interaction.response.send_message(f"Импортировано и обновлено {updated_users} записей.", ephemeral=True)
        logging.info(f"Успешно импортировано {updated_users} записей для пользователя {interaction.user.id}")
    except Exception as e:
        logging.error(f"Ошибка в команде /import_stats: {e}")
        await interaction.response.send_message("Произошла ошибка при импорте статистики.", ephemeral=True)

@app_commands.command(name="link_stats", description="Привязать статический ID к вашему Discord ID")
async def link_stats(interaction: discord.Interaction, static_id: str):
    try:
        logging.info(f"Команда /link_stats вызвана пользователем {interaction.user.id} с static_id: {static_id}")
        user_id = str(interaction.user.id)
        admins_ref = db_ref.child("admins")
        admins_data = await asyncio.to_thread(admins_ref.get) or {}
        
        found = False
        for admin_id, admin_data in admins_data.items():
            if admin_data.get("user_id") == user_id and admin_data.get("static_id") == static_id:
                found = True
                break
        
        if not found:
            await interaction.response.send_message(f"Статический ID {static_id} не соответствует вашему аккаунту.", ephemeral=True)
            logging.warning(f"Пользователь {user_id} пытался привязать неподходящий static_id: {static_id}")
            return

        stats_ref = db_ref.child("user_stats").child(static_id)
        stats_data = await asyncio.to_thread(stats_ref.get) or {}
        stats_data["discord_id"] = user_id
        await asyncio.to_thread(stats_ref.set, stats_data)
        await interaction.response.send_message(f"Статический ID {static_id} успешно привязан к вашему аккаунту.", ephemeral=True)
        logging.info(f"Пользователь {user_id} привязал статический ID {static_id}")
    except Exception as e:
        logging.error(f"Ошибка в команде /link_stats: {e}")
        await interaction.response.send_message("Произошла ошибка при привязке.", ephemeral=True)

@bot.command(name="audit")
async def audit(ctx, member: discord.Member):
    logging.info(f"Команда !audit вызвана пользователем {ctx.author.id} для пользователя {member.id}")
    if ctx.guild.id != GUILD_ID:
        logging.warning(f"Команда !audit вызвана не на основном сервере (GUILD_ID: {GUILD_ID})")
        await ctx.send("Эта команда доступна только на основном сервере!", delete_after=5)
        return
    if not any(role.id in ALLKICK_ROLES for role in ctx.author.roles):
        logging.warning(f"Пользователь {ctx.author.id} не имеет прав для команды !audit (отсутствуют роли из ALLKICK_ROLES)")
        await ctx.send("У вас нет прав для использования этой команды!", delete_after=5)
        return

    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel:
        join_date = await get_join_date(member)
        view = ui.View()
        view.add_item(WelcomeButton(new_member_id=member.id, is_kick=False, date_joined=join_date))
        try:
            await channel.send(
                f"Пользователю {member.mention} необходимо заполнить Audit.",
                view=view
            )
            logging.info(f"Сообщение об аудите для {member.id} успешно отправлено в канал {WELCOME_CHANNEL_ID}")
            await ctx.send(f"Аудит для {member.mention} инициирован в канале {channel.mention}.", ephemeral=True)
        except discord.errors.Forbidden:
            logging.error(f"Не удалось отправить сообщение в канал {WELCOME_CHANNEL_ID}: недостаточно прав.")
            await ctx.send("Ошибка: нет прав для отправки сообщения в канал аудита.", ephemeral=True)
        except discord.errors.HTTPException as e:
            logging.error(f"Не удалось отправить сообщение в канал {WELCOME_CHANNEL_ID}: {str(e)}")
            await ctx.send(f"Ошибка: {str(e)}", ephemeral=True)
    else:
        logging.error(f"Канал с ID {WELCOME_CHANNEL_ID} не найден")
        await ctx.send("Ошибка: канал аудита не найден.", ephemeral=True)

    try:
        await ctx.message.delete()
    except:
        logging.warning(f"Не удалось удалить сообщение {ctx.message.id} от {ctx.author.id}")

@bot.command(name="warn")
async def issue_reprimand(ctx):
    if ctx.guild.id != GUILD_ID or ctx.channel.id != PUNISHMENTS_CHANNEL_ID:
        return
    if not ctx.message.mentions:
        await ctx.send("Укажите пользователя с помощью @!", delete_after=5)
        return
    if not any(role.id in ADMIN_ROLES for role in ctx.author.roles):
        await ctx.send("У вас нет прав для выдачи выговоров!", delete_after=5)
        return
    view = ui.View()
    view.add_item(ReprimandButton(member_id=ctx.message.mentions[0].id))
    await ctx.send("Нажмите кнопку ниже для выдачи выговора:", view=view, ephemeral=True)
    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name="delete_warn")
async def remove_reprimand(ctx, member: discord.Member, reprimand_type: str = None):
    if ctx.guild.id != GUILD_ID or ctx.channel.id != PUNISHMENTS_CHANNEL_ID:
        return
    if not any(role.id in ADMIN_ROLES for role in ctx.author.roles):
        await ctx.send("У вас нет прав для снятия выговоров!", delete_after=5)
        return
    user_ref = db_ref.child("reprimands").child(str(member.id))
    user_reprimands = await asyncio.to_thread(user_ref.child("reprimands").get) or {}
    if isinstance(user_reprimands, list):
        user_reprimands = {str(i): r for i, r in enumerate(user_reprimands)}
    if not user_reprimands or not any(r["active"] for r in user_reprimands.values()):
        await ctx.send("У пользователя нет активных выговоров!", ephemeral=True)
        return
    
    reprimand_to_remove = None
    if reprimand_type and reprimand_type.lower() in ["устный", "строгий"]:
        for idx in reversed(list(user_reprimands.keys())):
            if user_reprimands[idx]["active"] and user_reprimands[idx]["type"] == ("oral" if reprimand_type.lower() == "устный" else "strict"):
                reprimand_to_remove = idx
                break
    else:
        for idx in list(user_reprimands.keys()):
            if user_reprimands[idx]["active"] and user_reprimands[idx]["type"] == "oral":
                reprimand_to_remove = idx
                break
        if not reprimand_to_remove:
            for idx in reversed(list(user_reprimands.keys())):
                if user_reprimands[idx]["active"]:
                    reprimand_to_remove = idx
                    break

    if reprimand_to_remove is not None:
        removed_type = "устный" if user_reprimands[reprimand_to_remove]["type"] == "oral" else "строгий"
        del user_reprimands[reprimand_to_remove]
        reindexed_reprimands = {str(i): v for i, v in enumerate(user_reprimands.values())}
        await asyncio.to_thread(user_ref.child("reprimands").set, reindexed_reprimands)
        channel = bot.get_channel(PUNISHMENTS_CHANNEL_ID)
        if channel:
            embed = discord.Embed(title=f"Снят {removed_type} выговор", color=discord.Color.green())
            embed.add_field(name="Пользователь", value=member.mention, inline=False)
            embed.set_footer(text=f"Снял: {ctx.author} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
            await channel.send(embed=embed)
        try:
            await member.send(f"С вас снят {removed_type} выговор.")
        except:
            pass
        await ctx.send(f"Выговор ({removed_type}) снят с {member.mention}.", ephemeral=True)
        await ctx.message.delete()
    else:
        await ctx.send("У пользователя нет активных выговоров указанного типа!", ephemeral=True)

@bot.command(name="warnings")
async def reprimand_list(ctx, member: discord.Member):
    if ctx.guild.id != GUILD_ID:
        return
    user_ref = db_ref.child("reprimands").child(str(member.id))
    user_reprimands = await asyncio.to_thread(user_ref.child("reprimands").get) or {}
    if isinstance(user_reprimands, list):
        reprimands_dict = {str(i): r for i, r in enumerate(user_reprimands)}
    else:
        reprimands_dict = user_reprimands
    active_reprimands = {idx: r for idx, r in reprimands_dict.items() if r.get("active", False)}
    if not active_reprimands:
        await ctx.send(f"У {member.mention} нет активных выговоров.", ephemeral=True)
        return
    embed = discord.Embed(title=f"Выговоры {member}", color=discord.Color.blue())
    for idx, r in active_reprimands.items():
        reprimand_type = "Устный" if r.get("type") == "oral" else "Строгий"
        issuer = bot.get_user(int(r.get("issuer_id", "0"))) or "Неизвестен"
        embed.add_field(
            name=f"Выговор {int(idx) + 1} ({reprimand_type})",
            value=f"Причина: {r.get('reason')}\nДата: {r.get('date')}\nИстекает: {r.get('expiration_date')}\nВыдал: {issuer}",
            inline=False
        )
    embed.set_footer(text=f"Всего активных: {len(active_reprimands)} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
    await ctx.send(embed=embed, ephemeral=True)
    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name="allkick")
async def all_kick(ctx, member: discord.Member):
    if ctx.guild.id != GUILD_ID:
        return
    if not any(role.id in ALLKICK_ROLES for role in ctx.author.roles):
        await ctx.send("У вас нет прав для использования этой команды!", delete_after=5)
        return

    kick_count = 0
    failed_guilds = []
    for guild in bot.guilds:
        if not (member_in_guild := guild.get_member(member.id)):
            failed_guilds.append(f"{guild.name} (пользователь не найден)")
            continue
        bot_member = guild.get_member(bot.user.id)
        if not bot_member or not bot_member.guild_permissions.kick_members:
            failed_guilds.append(f"{guild.name} (нет прав на кик)")
            continue
        try:
            await member_in_guild.kick(reason=f"Кик инициирован {ctx.author} через !allkick")
            kick_count += 1
            logging.info(f"Пользователь {member.name} кикнут с сервера {guild.name} (ID: {guild.id})")
        except Exception as e:
            failed_guilds.append(f"{guild.name} ({str(e)})")

    response = f"Успешно кикнуто с {kick_count} серверов."
    if failed_guilds:
        response += f"\n\nПроблемы на серверах:\n" + "\n".join(f"- {guild_name}" for guild_name in failed_guilds)
    await ctx.send(response, ephemeral=True)

    admin_ref = db_ref.child("admins")
    admins = await asyncio.to_thread(admin_ref.get) or {}
    user_id = str(member.id)
    keys_to_delete = []
    for key, admin in admins.items():
        if admin.get("user_id") == user_id or admin.get("nickname") == member.name or admin.get("static_id") == member.name:
            keys_to_delete.append(key)
    for key in keys_to_delete:
        await asyncio.to_thread(admin_ref.child(key).delete)
        logging.info(f"Удалены данные пользователя {member.id} с ключом {key} из базы admins")

    channel = bot.get_channel(AUDIT_CHANNEL_ID)
    if channel:
        join_date = await get_join_date(member)
        embed = discord.Embed(title="Пользователь кикнут", color=discord.Color.red())
        embed.add_field(name="Пользователь", value=member.mention, inline=False)
        embed.add_field(name="Кикнут с серверов", value=str(kick_count), inline=False)
        embed.add_field(name="Дата присоединения", value=join_date, inline=False)
        embed.set_footer(text=f"Инициировал: {ctx.author} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
        await channel.send(embed=embed)

        view = ui.View()
        view.add_item(WelcomeButton(new_member_id=member.id, is_kick=True, date_joined=join_date))
        try:
            await channel.send(
                f"Пользователь {member.mention} был кикнут. Старшая администрация, заполните данные ниже:",
                view=view
            )
            logging.info(f"Сообщение о кике {member.id} с кнопкой заполнения данных отправлено в канал {AUDIT_CHANNEL_ID}")
        except discord.errors.Forbidden:
            logging.error(f"Не удалось отправить сообщение в канал {AUDIT_CHANNEL_ID}: недостаточно прав.")
        except discord.errors.HTTPException as e:
            logging.error(f"Не удалось отправить сообщение в канал {AUDIT_CHANNEL_ID}: {str(e)}")

    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name="kick")
async def kick(ctx, member: discord.Member):
    if ctx.guild.id != GUILD_ID:
        return 
    if not any(role.id in ALLKICK_ROLES for role in ctx.author.roles):
        await ctx.send("У вас нет прав для использования этой команды!", delete_after=5)
        return

    member_in_guild = ctx.guild.get_member(member.id)
    if not member_in_guild:
        await ctx.send("Пользователь не найден на этом сервере!", ephemeral=True)
        return

    bot_member = ctx.guild.get_member(bot.user.id)
    if not bot_member or not bot_member.guild_permissions.kick_members:
        await ctx.send("У меня нет прав на кик пользователей на этом сервере!", ephemeral=True)
        return

    try:
        await member_in_guild.kick(reason=f"Кик инициирован {ctx.author} через !kick")
        logging.info(f"Пользователь {member.name} кикнут с сервера {ctx.guild.name} (ID: {ctx.guild.id})")

        admin_ref = db_ref.child("admins")
        admins = await asyncio.to_thread(admin_ref.get) or {}
        user_id = str(member.id)
        keys_to_delete = []
        for key, admin in admins.items():
            if admin.get("user_id") == user_id or admin.get("nickname") == member.name or admin.get("static_id") == member.name:
                keys_to_delete.append(key)
        for key in keys_to_delete:
            await asyncio.to_thread(admin_ref.child(key).delete)
            logging.info(f"Удалены данные пользователя {member.id} с ключом {key} из базы admins")

        channel = bot.get_channel(AUDIT_CHANNEL_ID)
        if channel:
            join_date = await get_join_date(member)
            embed = discord.Embed(title="Пользователь кикнут", color=discord.Color.red())
            embed.add_field(name="Пользователь", value=member.mention, inline=False)
            embed.add_field(name="Кикнут с сервера", value=ctx.guild.name, inline=False)
            embed.add_field(name="Дата присоединения", value=join_date, inline=False)
            embed.set_footer(text=f"Инициировал: {ctx.author} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
            await channel.send(embed=embed)

            view = ui.View()
            view.add_item(WelcomeButton(new_member_id=member.id, is_kick=True, date_joined=join_date))
            try:
                await channel.send(
                    f"Пользователь {member.mention} был кикнут. Старшая администрация, заполните данные ниже:",
                    view=view
                )
                logging.info(f"Сообщение о кике {member.id} с кнопкой заполнения данных отправлено в канал {AUDIT_CHANNEL_ID}")
            except discord.errors.Forbidden:
                logging.error(f"Не удалось отправить сообщение в канал {AUDIT_CHANNEL_ID}: недостаточно прав.")
            except discord.errors.HTTPException as e:
                logging.error(f"Не удалось отправить сообщение в канал {AUDIT_CHANNEL_ID}: {str(e)}")

        await ctx.send(f"Пользователь {member.mention} успешно кикнут с сервера {ctx.guild.name}.", ephemeral=True)
    except Exception as e:
        logging.error(f"Ошибка при кике пользователя {member.id}: {e}")
        await ctx.send(f"Не удалось кикнуть пользователя: {str(e)}", ephemeral=True)

    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name="event")
async def create_event(ctx):
    if ctx.guild.id != GUILD_ID or ctx.channel.id != EVENT_CHANNEL_ID:
        return 
    if not ctx.message.mentions:
        await ctx.send("Укажите хотя бы одного участника с помощью @!", delete_after=5)
        return
    if len(ctx.message.mentions) > 3:
        await ctx.send("Можно упомянуть не более 3 пользователей!", delete_after=5)
        return

    # Проверка активных ивентов (уже начавшихся)
    has_active_event, active_event_id = await check_active_events()
    if has_active_event:
        await ctx.send("Сейчас идет активное мероприятие! Новое мероприятие нельзя создать, пока идет текущее.", delete_after=10)
        return

    # Проверка запланированных ивентов (еще не начавшихся)
    has_scheduled_event, event_time = await check_scheduled_events()
    current_time = datetime.now(MSK)
    if has_scheduled_event:
        time_until_end = (event_time - current_time).total_seconds() / 60  # Время до начала ивента
        cooldown_end = event_time + timedelta(minutes=EVENT_COOLDOWN_MINUTES)  # Время окончания кулдауна
        time_until_available = (cooldown_end - current_time).total_seconds() / 60  # Время до возможности создать новый ивент
        minutes = int(time_until_available)
        seconds = int((time_until_available % 1) * 60)
        await ctx.send(
            f"Уже запланировано мероприятие на {event_time.strftime('%H:%M')}! "
            f"Новое мероприятие можно создать только после его завершения и кулдауна, через {minutes} мин {seconds} сек.",
            delete_after=10
        )
        return

    # Проверка кулдауна после последнего завершенного ивента
    last_completion_time = await get_last_event_completion_time()
    if last_completion_time:
        time_since_last_event = (current_time - last_completion_time).total_seconds() / 60
        if time_since_last_event < EVENT_COOLDOWN_MINUTES:
            remaining_minutes = EVENT_COOLDOWN_MINUTES - time_since_last_event
            minutes = int(remaining_minutes)
            seconds = int((remaining_minutes % 1) * 60)
            await ctx.send(
                f"Между мероприятиями должно пройти {EVENT_COOLDOWN_MINUTES} минут. "
                f"Новое мероприятие можно создать через {minutes} мин {seconds} сек.",
                delete_after=10
            )
            return

    participants = [user.id for user in ctx.message.mentions]
    view = ui.View()
    view.add_item(EventButton(participants))
    await ctx.send("Нажмите кнопку для заполнения данных мероприятия:", view=view, ephemeral=True)
    try:
        await ctx.message.delete()
    except:
        pass

async def check_expired_reprimands():
    while True:
        try:
            now = datetime.now(MSK)
            reprimands_ref = db_ref.child("reprimands")
            snapshot = await asyncio.to_thread(reprimands_ref.get)
            if not snapshot:
                await asyncio.sleep(3 * 3600)  # Если данных нет, ждем 3 часа
                continue

            for user_id, user_data in snapshot.items():
                user_reprimands = user_data.get("reprimands", {})
                if isinstance(user_reprimands, list):
                    user_reprimands = {str(i): r for i, r in enumerate(user_reprimands)}
                
                updated = False
                for idx in list(user_reprimands.keys()):
                    reprimand = user_reprimands[idx]
                    is_active = reprimand.get("active", False)
                    
                    if not is_active:
                        del user_reprimands[idx]
                        updated = True
                    elif is_active:
                        expiration_date_str = reprimand.get("expiration_date", "").replace("Z", "")
                        try:
                            expiration_date = datetime.strptime(expiration_date_str, '%H:%M %d:%m:%Y').replace(tzinfo=MSK)
                        except ValueError:
                            try:
                                expiration_date = datetime.fromisoformat(expiration_date_str).astimezone(MSK)
                            except ValueError:
                                logging.warning(f"Некорректный формат expiration_date для {user_id}, idx {idx}: {expiration_date_str}")
                                continue
                        
                        if now >= expiration_date:
                            del user_reprimands[idx]
                            updated = True
                
                if updated:
                    reindexed_reprimands = {str(i): v for i, v in enumerate(user_reprimands.values())}
                    await asyncio.to_thread(reprimands_ref.child(user_id).child("reprimands").set, reindexed_reprimands)
                    logging.info(f"Обновлены выговоры для пользователя {user_id}: удалено истекших или неактивных записей")

            await asyncio.sleep(3 * 3600)  # Проверка каждые 3 часа
        except Exception as e:
            logging.error(f"Ошибка при проверке выговоров: {e}")
            await asyncio.sleep(3 * 3600)  # В случае ошибки ждем 3 часа перед повторной попыткой

async def check_event_completion():
    while True:
        try:
            now = datetime.now(MSK)
            events = await asyncio.to_thread(EVENTS_REF.get) or {}
            for event_id, event_data in events.items():
                if event_data.get("active", False):
                    event_time = datetime.fromisoformat(event_data["timestamp"]).astimezone(MSK)
                    if now >= event_time:
                        await asyncio.to_thread(EVENTS_REF.child(event_id).update, {
                            "active": False,
                            "completed_at": now.isoformat()
                        })
                        logging.info(f"Мероприятие {event_id} завершено в {now.strftime('%H:%M %d:%m:%Y')}")
            await asyncio.sleep(60)  # Проверка каждую минуту
        except Exception as e:
            logging.error(f"Ошибка при проверке завершения ивентов: {e}")
            await asyncio.sleep(60)

@app_commands.command(name="view_stats", description="Посмотреть статистику другого пользователя")
@app_commands.checks.has_any_role(*ADMIN_ROLES)
async def view_stats(interaction: discord.Interaction, user: discord.User):
    try:
        logging.info(f"Команда /view_stats вызвана пользователем {interaction.user.id} для пользователя {user.id}")
        user_id = str(user.id)

        static_id, stats_data = await get_user_stats(user_id)

        embed = discord.Embed(title=f"Статистика пользователя {user.display_name}", color=discord.Color.blue())

        if static_id and stats_data:
            total_minutes = stats_data.get("total_minutes", 0)
            total_reports = stats_data.get("total_reports", 0)
            embed.add_field(
                name="Общая статистика",
                value=f"Часы: {format_minutes_to_hours(total_minutes)}\nРепорты: {total_reports}",
                inline=False
            )

            seven_days_ago = datetime.now(MSK) - timedelta(days=7)
            recent_minutes = 0
            recent_reports = 0
            history = stats_data.get("history", [])
            for entry in history:
                entry_date = datetime.strptime(
                    entry["date"].replace("Z", ""), '%H:%M %d:%m:%Y'
                ).replace(tzinfo=MSK)
                if entry_date >= seven_days_ago:
                    recent_minutes += entry.get("added_minutes", 0)
                    recent_reports += entry.get("added_reports", 0)
            embed.add_field(
                name="За последние 7 дней",
                value=f"Часы: {format_minutes_to_hours(recent_minutes)}\nРепорты: {recent_reports}",
                inline=False
            )

            if history:
                last_entry = history[-1]
                embed.add_field(
                    name="Последнее обновление",
                    value=f"Дата: {last_entry['date']}\nЧасы: {format_minutes_to_hours(last_entry['added_minutes'])}\nРепорты: {last_entry['added_reports']}",
                    inline=False
                )
        else:
            embed.add_field(
                name="Статистика",
                value="Нет данных о статистике. Пользователь должен привязать static_id через /link_stats.",
                inline=False
            )

        embed.set_footer(text=f"Запросил: {interaction.user.display_name} | {datetime.now(MSK).strftime('%H:%M %d:%m:%Y')}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logging.info(f"Пользователь {interaction.user.id} успешно просмотрел статистику пользователя {user_id}")
    except Exception as e:
        logging.error(f"Ошибка в команде /view_stats: {e}")
        await interaction.response.send_message("Произошла ошибка при выполнении команды.", ephemeral=True)

@bot.event
async def on_member_join(member):
    if member.guild.id != GUILD_ID:
        return 

    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel:
        join_date = await get_join_date(member)
        view = ui.View()
        view.add_item(WelcomeButton(new_member_id=member.id, is_kick=False, date_joined=join_date))
        try:
            await channel.send(
                f"Присоединился новый пользователь: {member.mention}. Старшая администрация, заполните данные ниже:",
                view=view
            )
            logging.info(f"Сообщение о присоединении {member.id} успешно отправлено в канал {WELCOME_CHANNEL_ID}")
        except discord.errors.Forbidden:
            logging.error(f"Не удалось отправить сообщение в канал {WELCOME_CHANNEL_ID}: недостаточно прав.")
        except discord.errors.HTTPException as e:
            logging.error(f"Не удалось отправить сообщение в канал {WELCOME_CHANNEL_ID}: {str(e)}")

@bot.command(name="sync")
async def sync_commands(ctx):
    if ctx.author.id == 310707269547458570:  # МойID
        try:
            synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
            logging.info(f"Синхронизировано {len(synced)} команд: {[cmd.name for cmd in synced]}")
            await ctx.send(f"Синхронизировано {len(synced)} команд: {[cmd.name for cmd in synced]}")
        except Exception as e:
            logging.error(f"Ошибка синхронизации команд: {e}")
            await ctx.send(f"Ошибка синхронизации: {e}")
    else:
        await ctx.send("У вас нет прав для выполнения этой команды!")

@bot.command(name="clear_commands")
async def clear_commands(ctx):
    if ctx.author.id == 310707269547458570:  # Мой ID
        bot.tree.clear_commands(guild=discord.Object(id=GUILD_ID))
        await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        await ctx.send("Команды очищены и пересинхронизированы!")
    else:
        await ctx.send("У вас нет прав для выполнения этой команды!")

@bot.event
async def on_ready():
    await bot.change_presence(status=discord.Status.dnd)
    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        logging.info(f"Синхронизировано {len(synced)} команд при запуске: {[cmd.name for cmd in synced]}")
    except Exception as e:
        logging.error(f"Ошибка синхронизации команд при запуске: {e}")
    logging.info(f'Бот {bot.user} готов к работе!')
    asyncio.create_task(check_expired_reprimands())
    asyncio.create_task(check_event_completion())

async def main():
    bot.tree.add_command(menu, guild=discord.Object(id=GUILD_ID))
    bot.tree.add_command(import_stats, guild=discord.Object(id=GUILD_ID))
    bot.tree.add_command(link_stats, guild=discord.Object(id=GUILD_ID))
    bot.tree.add_command(view_stats, guild=discord.Object(id=GUILD_ID))
    
    while True:
        try:
            await bot.start(TOKEN)
        except Exception as e:
            logging.error(f"Ошибка: {e}. Повторная попытка через 5 секунд...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
