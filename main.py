"""MVP Telegram-бота для вакансий монтажёров.

Бот принимает новые сообщения из добавленных групп/супергрупп/каналов,
отбирает похожие на вакансии, сохраняет их в SQLite и отправляет подходящие
карточки подписанным пользователям.

Платежи в MVP не подключены: для проверки сценария есть демо-доступ на 7 дней.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"])
DB_PATH = Path(os.getenv("DB_PATH", Path(__file__).parent / "vacancies.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("vacancy_bot")


KEYWORDS = [
    r"монтаж[её]р",
    r"видеомонтаж",
    r"editor",
    r"video editor",
    r"video editing",
    r"motion designer",
    r"моушн дизайнер",
    r"colorist",
    r"колорист",
    r"reels?-монтаж",
]
HIRING_HINTS = (
    r"вакан|ищ[ую]|нужен|нужна|нужны|требуется|hiring|hire|looking\s+for|"
    r"набира|в\s+команду|оплата|ставка|salary|budget|бюджет|оклад|зарплата"
)
_KEYWORD_RE = re.compile("|".join(KEYWORDS), re.IGNORECASE)
_HIRING_RE = re.compile(HIRING_HINTS, re.IGNORECASE)
_PAY_RE = re.compile(
    r"оплат|ставк|salary|budget|бюджет|зарплат|руб|₽|\$|€|usd|byn|"
    r"за ролик|за проект|в месяц|в час",
    re.IGNORECASE,
)
_TASK_RE = re.compile(r"задач|требован|обязан|монтаж|reels|shorts|youtube", re.IGNORECASE)
_LONG_TERM_RE = re.compile(r"долгосроч|постоянн|стабильн|ежедневн|в месяц|в команд", re.IGNORECASE)
_CONTACT_RE = re.compile(
    r"@[A-Za-z0-9_]{4,}|t\.me/[A-Za-z0-9_/-]+|https?://\S+|[\w.+-]+@[\w.-]+\.\w+",
    re.IGNORECASE,
)


class UserStates(StatesGroup):
    waiting_stop_words = State()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def init_db() -> None:
    with db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                full_name TEXT NOT NULL DEFAULT '',
                subscription_until TEXT,
                format_filter TEXT NOT NULL DEFAULT 'all',
                quality_filter TEXT NOT NULL DEFAULT 'all',
                stop_words TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_hash TEXT NOT NULL UNIQUE,
                text TEXT NOT NULL,
                source_title TEXT NOT NULL DEFAULT '',
                source_username TEXT NOT NULL DEFAULT '',
                message_id INTEGER,
                author TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                quality TEXT NOT NULL DEFAULT 'Приемлемая',
                score INTEGER NOT NULL DEFAULT 50,
                format_type TEXT NOT NULL DEFAULT 'short',
                tags TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )


def ensure_user(user_id: int, username: str = "", full_name: str = "") -> sqlite3.Row:
    now = iso_now()
    with db() as connection:
        connection.execute(
            """
            INSERT INTO users (user_id, username, full_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                full_name = excluded.full_name,
                updated_at = excluded.updated_at
            """,
            (user_id, username, full_name, now, now),
        )
        row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row is None:
        raise RuntimeError("Не удалось создать пользователя")
    return row


def get_user(user_id: int) -> sqlite3.Row:
    return ensure_user(user_id)


def update_user(user_id: int, **fields: Any) -> sqlite3.Row:
    allowed = {
        "subscription_until",
        "format_filter",
        "quality_filter",
        "stop_words",
        "username",
        "full_name",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"Недопустимые поля пользователя: {unknown}")
    if not fields:
        return get_user(user_id)
    fields["updated_at"] = iso_now()
    assignments = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [user_id]
    with db() as connection:
        connection.execute(f"UPDATE users SET {assignments} WHERE user_id = ?", values)
        row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row is None:
        raise RuntimeError("Пользователь не найден")
    return row


def subscription_active(row: sqlite3.Row) -> bool:
    until = parse_dt(row["subscription_until"])
    return until is not None and until > utc_now()


def get_active_users() -> list[sqlite3.Row]:
    now = iso_now()
    with db() as connection:
        return list(
            connection.execute(
                "SELECT * FROM users WHERE subscription_until IS NOT NULL AND subscription_until > ?",
                (now,),
            ).fetchall()
        )


def vacancy_count(period_days: int | None = None) -> int:
    with db() as connection:
        if period_days is None:
            row = connection.execute("SELECT COUNT(*) AS count FROM vacancies").fetchone()
        else:
            since = (utc_now() - timedelta(days=period_days)).isoformat()
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM vacancies WHERE created_at >= ?", (since,)
            ).fetchone()
    return int(row["count"]) if row else 0


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def looks_like_vacancy(text: str) -> bool:
    normalized = normalize(text)
    if len(normalized) < 15:
        return False
    return bool(_KEYWORD_RE.search(normalized)) and bool(_HIRING_RE.search(normalized))


def classify_vacancy(text: str) -> dict[str, Any]:
    normalized = normalize(text)
    score = 40
    tags: list[str] = []
    if _PAY_RE.search(normalized):
        score += 25
    if _TASK_RE.search(normalized):
        score += 10
    if _LONG_TERM_RE.search(normalized):
        score += 10
    if len(normalized) >= 500:
        score += 5
    if "capcut" in normalized and "не подходит" in normalized:
        score -= 10

    if re.search(r"reels?|shorts?|tiktok|коротк", normalized, re.IGNORECASE):
        format_type = "short"
        tags.append("#короткийформат")
    elif re.search(r"youtube|документаль|подкаст|длинн|кинематограф", normalized, re.IGNORECASE):
        format_type = "long"
        tags.append("#длинныйформат")
    else:
        format_type = "short" if len(normalized) < 500 else "long"

    tags.insert(0, "#видеомонтаж")
    if score >= 75:
        quality = "Отличная"
    elif score >= 55:
        quality = "Приемлемая"
    else:
        quality = "Сомнительная"

    return {
        "score": max(0, min(100, score)),
        "quality": quality,
        "format_type": format_type,
        "tags": " ".join(tags),
    }


def vacancy_matches(row: sqlite3.Row, user: sqlite3.Row) -> bool:
    format_filter = user["format_filter"]
    quality_filter = user["quality_filter"]
    if format_filter != "all" and row["format_type"] != format_filter:
        return False
    if quality_filter == "good" and row["score"] < 70:
        return False
    if quality_filter == "excellent" and row["score"] < 85:
        return False
    stop_words = [word.strip().lower() for word in user["stop_words"].split(",") if word.strip()]
    content = row["text"].lower()
    return not any(word in content for word in stop_words)


def add_vacancy(
    text: str,
    source_title: str,
    source_username: str = "",
    message_id: int | None = None,
    author: str = "",
) -> sqlite3.Row | None:
    analysis = classify_vacancy(text)
    content_hash = hashlib.sha256(normalize(text).encode("utf-8")).hexdigest()
    source_url = ""
    if source_username and message_id:
        source_url = f"https://t.me/{source_username.lstrip('@')}/{message_id}"
    with db() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO vacancies
            (content_hash, text, source_title, source_username, message_id, author,
             source_url, quality, score, format_type, tags, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                content_hash,
                text.strip(),
                source_title,
                source_username,
                message_id,
                author,
                source_url,
                analysis["quality"],
                analysis["score"],
                analysis["format_type"],
                analysis["tags"],
                iso_now(),
            ),
        )
        if cursor.rowcount == 0:
            return None
        row = connection.execute(
            "SELECT * FROM vacancies WHERE content_hash = ?", (content_hash,)
        ).fetchone()
    return row


def get_vacancy(vacancy_id: int) -> sqlite3.Row | None:
    with db() as connection:
        return connection.execute("SELECT * FROM vacancies WHERE id = ?", (vacancy_id,)).fetchone()


def recent_vacancies(limit: int = 20) -> list[sqlite3.Row]:
    with db() as connection:
        return list(
            connection.execute(
                "SELECT * FROM vacancies ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        )


def quality_icon(quality: str) -> str:
    return {"Отличная": "🟢", "Приемлемая": "🟡", "Сомнительная": "🔴"}.get(quality, "🟡")


def pretty_date(value: str | None) -> str:
    parsed = parse_dt(value)
    if parsed is None:
        return "не указано"
    return parsed.astimezone().strftime("%d.%m.%Y %H:%M")


def format_filter(value: str) -> str:
    return {"all": "любой формат", "short": "короткие ролики", "long": "длинные проекты"}.get(
        value, value
    )


def quality_filter(value: str) -> str:
    return {"all": "любой уровень", "good": "от 70 баллов", "excellent": "только топовые"}.get(
        value, value
    )


def profile_text(user: sqlite3.Row) -> str:
    if subscription_active(user):
        status = f"🟩 Доступ открыт до {pretty_date(user['subscription_until'])}"
        delivery = "Новые находки будут приходить в эту ленту"
    else:
        status = "⚪ Доступ пока закрыт"
        delivery = "Лента ждёт активации"
    stop_words = user["stop_words"] or "список пуст"
    return (
        "🎬 <b>Монтажный радар</b>\n\n"
        f"{status}\n{delivery}\n\n"
        "🧭 <b>Настройки радара</b>\n"
        f"<b>Профиль:</b> базовый режим\n"
        f"<b>Тип задач:</b> {format_filter(user['format_filter'])}\n"
        f"<b>Порог:</b> {quality_filter(user['quality_filter'])}\n"
        f"<b>Исключения:</b> {stop_words}\n\n"
        f"📡 Найдено за сегодня: {vacancy_count(1)}\n"
        f"🗓 За последние 30 дней: {vacancy_count(30)}"
    )


def profile_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Открыть доступ", callback_data="subscription")],
            [
                InlineKeyboardButton(text="🧭 Настроить ленту", callback_data="filters"),
                InlineKeyboardButton(text="🧹 Исключения", callback_data="stop_words"),
            ],
            [InlineKeyboardButton(text="✨ Что умеет радар", callback_data="more")],
        ]
    )


def filters_keyboard(user: sqlite3.Row) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"Тип задач: {format_filter(user['format_filter'])}",
                    callback_data="cycle_format",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"Порог: {quality_filter(user['quality_filter'])}",
                    callback_data="cycle_quality",
                )
            ],
            [
                InlineKeyboardButton(text="🧹 Исключения", callback_data="stop_words"),
                InlineKeyboardButton(text="↩️ В меню", callback_data="profile"),
            ],
        ]
    )


def subscription_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧪 Запустить тест на 7 дней", callback_data="demo_access"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💠 Карта — следующий этап", callback_data="payment_soon"
                )
            ],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="profile")],
        ]
    )


def stop_words_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Обновить список", callback_data="edit_stop_words")],
            [InlineKeyboardButton(text="🧽 Убрать всё", callback_data="clear_stop_words")],
            [InlineKeyboardButton(text="↩️ К настройкам", callback_data="filters")],
        ]
    )


def vacancy_keyboard(vacancy_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔎 Открыть контакты", callback_data=f"contacts:{vacancy_id}"
                )
            ]
        ]
    )


def vacancy_text(row: sqlite3.Row) -> str:
    source = row["source_title"] or "неизвестный источник"
    text = row["text"].strip()
    if len(text) > 3000:
        text = f"{text[:2990].rstrip()}…"
    return (
        f"🎞 <b>Новый сигнал #{row['id']:04d}</b>\n"
        f"{row['tags']}\n\n"
        "<b>Рейтинг радара:</b>\n"
        f"{quality_icon(row['quality'])} {row['quality']}\n\n"
        f"{text}\n\n"
        f"🗺 <b>Пришло из:</b> {source}"
    )


def contacts_text(row: sqlite3.Row) -> str:
    found = _CONTACT_RE.findall(row["text"])
    contacts = "\n".join(f"• {contact}" for contact in dict.fromkeys(found))
    if not contacts:
        contacts = "Прямой контакт не найден. Откройте исходную публикацию."
    source_link = f"\n🔗 {row['source_url']}" if row["source_url"] else ""
    return (
        f"🧷 <b>Координаты по сигналу #{row['id']:04d}</b>\n\n"
        f"{contacts}{source_link}\n\n"
        "Перед откликом проверьте условия и портфолио заказчика."
    )


async def send_profile(message: Message) -> None:
    user = ensure_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name,
    )
    await message.answer(profile_text(user), parse_mode="HTML", reply_markup=profile_keyboard())


async def send_vacancy(target: Message | CallbackQuery, row: sqlite3.Row) -> None:
    kwargs = {
        "text": vacancy_text(row),
        "parse_mode": "HTML",
        "reply_markup": vacancy_keyboard(row["id"]),
    }
    if isinstance(target, CallbackQuery):
        await target.message.answer(**kwargs)
    else:
        await target.answer(**kwargs)


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await send_profile(message)


@dp.message(Command("profile"))
async def cmd_profile(message: Message) -> None:
    await send_profile(message)


@dp.message(Command("feed"))
async def cmd_feed(message: Message) -> None:
    user = ensure_user(message.from_user.id)
    if not subscription_active(user) and message.from_user.id != ADMIN_CHAT_ID:
        await message.answer(
            "🔐 Радар пока не активирован. Откройте доступ, чтобы получать новые сигналы.",
            reply_markup=subscription_keyboard(),
        )
        return
    sent = 0
    for row in recent_vacancies():
        if vacancy_matches(row, user) or message.from_user.id == ADMIN_CHAT_ID:
            await send_vacancy(message, row)
            sent += 1
    if sent == 0:
        await message.answer(
            "Пока тихо: подходящих сигналов нет. Подключите источник или ослабьте "
            "настройки радара."
        )


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "Добавьте радар в группы или каналы, где появляются задачи для монтажёров. "
        "Подходящие публикации он отберёт сам.\n\n"
        "/start — открыть меню\n"
        "/feed — запросить свежие сигналы\n"
        "/cancel — выйти из ввода исключений"
    )


@dp.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await message.answer(f"Ваш chat_id: <code>{message.chat.id}</code>", parse_mode="HTML")


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Изменения отменены.", reply_markup=profile_keyboard())


@dp.callback_query(F.data == "profile")
async def callback_profile(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    await query.answer()
    await query.message.edit_text(
        profile_text(user), parse_mode="HTML", reply_markup=profile_keyboard()
    )


@dp.callback_query(F.data == "subscription")
async def callback_subscription(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    current = pretty_date(user["subscription_until"]) if subscription_active(user) else "неактивна"
    await query.answer()
    await query.message.edit_text(
        "🚀 <b>Доступ к радару</b>\n\n"
        f"Сейчас: {current}\n\n"
        "Для проверки можно включить тестовый маршрут на 7 дней. "
        "Настоящие платежи добавим после проверки ленты.",
        parse_mode="HTML",
        reply_markup=subscription_keyboard(),
    )


@dp.callback_query(F.data == "demo_access")
async def callback_demo_access(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    current = parse_dt(user["subscription_until"])
    start = current if current and current > utc_now() else utc_now()
    updated = update_user(query.from_user.id, subscription_until=(start + timedelta(days=7)).isoformat())
    await query.answer("Тестовый маршрут запущен")
    await query.message.edit_text(
        profile_text(updated), parse_mode="HTML", reply_markup=profile_keyboard()
    )


@dp.callback_query(F.data == "payment_soon")
async def callback_payment_soon(query: CallbackQuery) -> None:
    await query.answer("Платежи будут подключены после проверки MVP", show_alert=True)


@dp.callback_query(F.data == "filters")
async def callback_filters(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    await query.answer()
    await query.message.edit_text(
        "🧭 <b>Настройка радара</b>\n\n"
        "Нажмите на строку, чтобы переключить режим отбора.",
        parse_mode="HTML",
        reply_markup=filters_keyboard(user),
    )


@dp.callback_query(F.data == "cycle_format")
async def callback_cycle_format(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    values = ["all", "short", "long"]
    next_value = values[(values.index(user["format_filter"]) + 1) % len(values)]
    user = update_user(query.from_user.id, format_filter=next_value)
    await query.answer(f"Формат: {format_filter(next_value)}")
    await query.message.edit_reply_markup(reply_markup=filters_keyboard(user))


@dp.callback_query(F.data == "cycle_quality")
async def callback_cycle_quality(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    values = ["all", "good", "excellent"]
    next_value = values[(values.index(user["quality_filter"]) + 1) % len(values)]
    user = update_user(query.from_user.id, quality_filter=next_value)
    await query.answer(f"Качество: {quality_filter(next_value)}")
    await query.message.edit_reply_markup(reply_markup=filters_keyboard(user))


@dp.callback_query(F.data == "stop_words")
async def callback_stop_words(query: CallbackQuery) -> None:
    user = ensure_user(query.from_user.id)
    words = user["stop_words"] or "не заданы"
    await query.answer()
    await query.message.edit_text(
        "🧹 <b>Исключения из ленты</b>\n\n"
        f"Сейчас: {words}\n\n"
        "Публикации с этими словами будут отсеиваться до отправки.",
        parse_mode="HTML",
        reply_markup=stop_words_keyboard(),
    )


@dp.callback_query(F.data == "edit_stop_words")
async def callback_edit_stop_words(query: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(UserStates.waiting_stop_words)
    await query.answer()
    await query.message.answer(
        "Введите исключения через запятую.\n"
        "Например: <code>бесплатно, capcut, стажировка</code>\n"
        "Чтобы вернуть чистую ленту, отправьте <code>-</code>.",
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "clear_stop_words")
async def callback_clear_stop_words(query: CallbackQuery) -> None:
    user = update_user(query.from_user.id, stop_words="")
    await query.answer("Список исключений очищен")
    await query.message.edit_text(
        "🧹 <b>Исключения из ленты</b>\n\nСейчас: список пуст",
        parse_mode="HTML",
        reply_markup=stop_words_keyboard(),
    )


@dp.callback_query(F.data == "more")
async def callback_more(query: CallbackQuery) -> None:
    await query.answer()
    await query.message.edit_text(
        "✨ <b>Радар умеет</b>\n\n"
        "Отбирать задачи по формату, отсекать нежелательные условия, "
        "сохранять дубли и открывать контакты только после активации доступа.\n\n"
        "Дальше добавим оплату, расширенные источники и оценку по портфолио.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="↩️ Вернуться в меню", callback_data="profile")]]
        ),
    )


@dp.callback_query(F.data.startswith("contacts:"))
async def callback_contacts(query: CallbackQuery) -> None:
    vacancy_id = int(query.data.split(":", 1)[1])
    row = get_vacancy(vacancy_id)
    user = ensure_user(query.from_user.id)
    if row is None:
        await query.answer("Этот сигнал больше недоступен", show_alert=True)
        return
    if not subscription_active(user) and query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Откройте доступ, чтобы увидеть контакты", show_alert=True)
        return
    await query.answer()
    await query.message.answer(contacts_text(row), parse_mode="HTML")


@dp.message(UserStates.waiting_stop_words)
async def save_stop_words(message: Message, state: FSMContext) -> None:
    raw = message.text.strip()
    value = "" if raw == "-" else ", ".join(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))
    user = update_user(message.from_user.id, stop_words=value)
    await state.clear()
    await message.answer(
        f"Исключения сохранены: {value or 'список пуст'}",
        reply_markup=profile_keyboard(),
    )
    log.info("Updated stop words for user %s", user["user_id"])


async def health_server() -> None:
    port_value = os.getenv("PORT")
    if not port_value:
        return

    port = int(port_value)

    async def handle_health_request(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await reader.read(1024)
            body = b"OK\n"
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: 3\r\n"
                b"Connection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle_health_request, "0.0.0.0", port)
    log.info("Health endpoint слушает порт %s", port)
    async with server:
        await server.serve_forever()


async def handle_source_message(message: Message) -> None:
    text = message.text or message.caption or ""
    if not looks_like_vacancy(text):
        return
    author = ""
    if message.from_user:
        author = message.from_user.full_name
        if message.from_user.username:
            author += f" (@{message.from_user.username})"
    row = add_vacancy(
        text=text,
        source_title=message.chat.title or "без названия",
        source_username=message.chat.username or "",
        message_id=message.message_id,
        author=author,
    )
    if row is None:
        return

    active_users = get_active_users()
    for user in active_users:
        if vacancy_matches(row, user):
            try:
                await bot.send_message(
                    user["user_id"],
                    vacancy_text(row),
                    parse_mode="HTML",
                    reply_markup=vacancy_keyboard(row["id"]),
                )
            except Exception as error:  # noqa: BLE001
                log.warning("Не удалось отправить вакансию пользователю %s: %s", user["user_id"], error)

    if ADMIN_CHAT_ID not in {user["user_id"] for user in active_users}:
        try:
            await bot.send_message(
                ADMIN_CHAT_ID,
                "🛠 <b>Новая вакансия в мониторинге</b>\n\n"
                + vacancy_text(row),
                parse_mode="HTML",
                reply_markup=vacancy_keyboard(row["id"]),
            )
        except Exception as error:  # noqa: BLE001
            log.warning("Не удалось отправить уведомление админу: %s", error)


@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL}))
async def handle_group_or_channel_message(message: Message) -> None:
    await handle_source_message(message)


@dp.message(Command("demo"))
async def cmd_demo(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    samples = [
        (
            "Ищем видеомонтажёра для Reels и Shorts. Долгосрочная работа, "
            "оплата 1 500 ₽ за ролик. Пишите @demo_contact.",
            "Демо-источник",
        ),
        (
            "Нужен монтажёр для YouTube и подкастов. Задачи: монтаж выпусков, "
            "субтитры и звук. Зарплата 80 000 ₽ в месяц, отклик @demo_long.",
            "Демо-источник",
        ),
    ]
    added = 0
    for text, source in samples:
        if add_vacancy(text, source):
            added += 1
    await message.answer(f"Добавлено демо-вакансий: {added}. Откройте /feed.")


async def main() -> None:
    init_db()
    retry_delay = 5
    health_task = asyncio.create_task(health_server()) if os.getenv("PORT") else None
    try:
        while True:
            try:
                log.info("Бот запущен, слушаю группы, супергруппы и каналы...")
                await dp.start_polling(bot, handle_signals=False)
                log.warning("Polling остановился без ошибки, перезапускаю...")
                retry_delay = 5
            except asyncio.CancelledError:
                raise
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                log.exception(
                    "Polling завершился с ошибкой. Повторная попытка через %s секунд.",
                    retry_delay,
                )

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)
    finally:
        if health_task is not None:
            health_task.cancel()
            with suppress(asyncio.CancelledError):
                await health_task
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())