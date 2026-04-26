"""
MAX ↔ Telegram Bridge Bot

Зеркалит выбранный MAX-чат в TG-группу и обратно.
Поддерживает: текст, фото, документы, голосовые, видео, стикеры.
"""

import asyncio
import json
import os
import re
import random
from io import BytesIO
from pathlib import Path

# Подключаем Pillow для обработки фото
try:
    from PIL import Image, ImageOps
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False

import aiohttp
from aiogram import Bot, Dispatcher, F, types

from pymax.core import SocketMaxClient as MaxClient
from pymax import Message
from pymax.payloads import UserAgentPayload
from pymax.static.enum import MessageType
from pymax.types import (
    AudioAttach,
    ContactAttach,
    ControlAttach,
    FileAttach,
    PhotoAttach,
    StickerAttach,
    VideoAttach,
)
import pymax.files  # Динамически достаем классы отсюда

# ── НАСТРОЙКИ ─────────────────────────────────────────────

MAX_PHONE    = os.getenv("MAX_PHONE", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_GROUP_ID  = int(os.getenv("TG_GROUP_ID", "0"))
WORK_DIR     = os.getenv("WORK_DIR", "cache")
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "200"))

CONFIG_FILE = "bridge_config.json"
DB_FILE = "bridge_db.json"

IS_STARTED = False       
TG_HELLO_SENT = False    

# ─── Надежное хранилище в памяти ──────────────────────────

DB_CACHE = None
CONFIG_CACHE = None

def load_db() -> dict:
    global DB_CACHE
    if DB_CACHE is not None:
        return DB_CACHE
    
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            DB_CACHE = json.loads(content) if content else {}
    except (FileNotFoundError, json.JSONDecodeError):
        DB_CACHE = {}
        
    DB_CACHE.setdefault("sent_ids",[])
    DB_CACHE.setdefault("users", {})
    DB_CACHE.setdefault("tg_to_max", {})
    DB_CACHE.setdefault("max_to_tg", {})
    return DB_CACHE

def save_db() -> None:
    global DB_CACHE
    if DB_CACHE is not None:
        temp_file = DB_FILE + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(DB_CACHE, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, DB_FILE)

def load_config() -> dict:
    global CONFIG_CACHE
    if CONFIG_CACHE is not None:
        return CONFIG_CACHE
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            CONFIG_CACHE = json.loads(content) if content else {}
    except (FileNotFoundError, json.JSONDecodeError):
        CONFIG_CACHE = {}
    return CONFIG_CACHE

def save_config(data: dict) -> None:
    global CONFIG_CACHE
    CONFIG_CACHE = data
    temp_file = CONFIG_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, CONFIG_FILE)

# ─── Функции базы ─────────────────────────────────────────

def is_sent(msg_id: int) -> bool:
    db = load_db()
    return str(msg_id) in db["sent_ids"]

def mark_sent(msg_id: int) -> None:
    db = load_db()
    ids = db["sent_ids"]
    if str(msg_id) not in ids:
        ids.append(str(msg_id))
        if len(ids) > 5000:
            db["sent_ids"] = ids[-5000:]
        save_db()

def get_user_name(tg_user_id: int) -> str | None:
    return load_db()["users"].get(str(tg_user_id))

def register_user(tg_user_id: int, name: str) -> None:
    db = load_db()
    db["users"][str(tg_user_id)] = name
    save_db()

def save_msg_mapping(tg_msg_id: int, max_msg_id: int | str) -> None:
    db = load_db()
    db["tg_to_max"][str(tg_msg_id)] = str(max_msg_id)
    db["max_to_tg"][str(max_msg_id)] = str(tg_msg_id)
    save_db()

# ─── Клиенты ──────────────────────────────────────────────

ua = UserAgentPayload(device_type="DESKTOP", app_version="25.12.13")
max_client = MaxClient(phone=MAX_PHONE, work_dir=WORK_DIR, headers=ua, reconnect=True)
tg_bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher()

pending_messages: dict[int, list[str]] = {}

# ─── Утилиты ──────────────────────────────────────────────

async def download_bytes(url: str, filename: str = "file") -> BytesIO:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            data = await resp.read()
    buf = BytesIO(data)
    buf.name = resp.headers.get("X-File-Name", filename)
    return buf

async def get_max_display_name(sender_id: int | None) -> str:
    if sender_id is None:
        return "Система"
    try:
        user = await asyncio.wait_for(max_client.get_user(sender_id), timeout=5)
        if user and user.names:
            n = user.names[0]
            return f"{n.first_name or ''} {n.last_name or ''}".strip() or f"User#{sender_id}"
    except Exception:
        pass
    return f"User#{sender_id}"

async def safe_tg_send(method, *args, **kwargs):
    while True:
        try:
            return await method(*args, **kwargs)
        except Exception as e:
            err_msg = str(e).lower()
            if "retry after" in err_msg or "too many requests" in err_msg:
                match = re.search(r"retry after (\d+)", err_msg)
                delay = int(match.group(1)) if match else 30
                print(f"⏳ Telegram просит подождать. Пауза {delay} сек...")
                await asyncio.sleep(delay + 1)
                continue
            raise e

async def safe_max_send(text, chat_id, attachment=None):
    kwargs = {"text": text, "chat_id": chat_id}
    if attachment is not None:
        kwargs["attachment"] = attachment
    
    return await max_client.send_message(**kwargs)

async def delayed_remove(path: str, delay: int = 20):
    """Удаляет временный файл с задержкой."""
    await asyncio.sleep(delay)
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

def clean_max_text(raw_text: str) -> str:
    """Очищает текст от маркеров Брайля и[медиа] перед отправкой в TG"""
    if not raw_text:
        return ""
    # Удаляем наш специальный блок [⠃медиа⠇] вместе с возможным пробелом
    cleaned = re.sub(r'\[[\u2800-\u28FF]+медиа[\u2800-\u28FF]+\]\s*', '', raw_text)
    # На всякий случай удаляем остатки шрифта Брайля
    cleaned = re.sub(r'[\u2800-\u28FF]+', '', cleaned)
    return cleaned.strip()

# ─── Отправка MAX→TG ──────────────────────────────────────

async def forward_to_tg(msg: Message, chat_id: int, max_chat_id: int) -> None:
    if is_sent(msg.id):
        return

    me_id = max_client.me.id if max_client.me else None
    if me_id and msg.sender == me_id:
        if msg.text and any(marker in msg.text for marker in ("🔔[TG]", "🗣️ [TG]", "🗣️[TG]", "[фото", "[от ", "[медиа", "не перенесён")):
            mark_sent(msg.id)
            return

    actual_chat_id = msg.chat_id if msg.chat_id is not None else max_chat_id

    if msg.type in (MessageType.SYSTEM, MessageType.SERVICE, "SYSTEM", "SERVICE"):
        name = "⚙️ Система"
    else:
        name = await get_max_display_name(msg.sender)

    try:
        sent_tg_msg = None
        
        if msg.type in (MessageType.SYSTEM, MessageType.SERVICE, "SYSTEM", "SERVICE"):
            text = msg.text or "системное уведомление"
            sent_tg_msg = await safe_tg_send(tg_bot.send_message, chat_id, f"ℹ️ {text}")

        elif msg.attaches:
            # Очищаем маркеры Брайля из текста
            clean_text = clean_max_text(msg.text)
            base_text = f"🗣️ {name}:\n{clean_text}" if clean_text else f"🗣️ {name}:"
            
            for i, attach in enumerate(msg.attaches):
                current_caption = base_text if i == 0 else None
                m = None

                if isinstance(attach, PhotoAttach):
                    try:
                        buf = await download_bytes(attach.base_url, "photo.jpg")
                        m = await safe_tg_send(
                            tg_bot.send_photo, chat_id,
                            photo=types.BufferedInputFile(buf.getvalue(), filename=buf.name),
                            caption=current_caption
                        )
                    except Exception as e:
                        m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[фото — ошибка: {e}]")

                elif isinstance(attach, VideoAttach):
                    try:
                        video = await asyncio.wait_for(max_client.get_video_by_id(chat_id=actual_chat_id, message_id=msg.id, video_id=attach.video_id), timeout=15)
                        buf = await download_bytes(video.url, "video.mp4")
                        m = await safe_tg_send(
                            tg_bot.send_video, chat_id,
                            video=types.BufferedInputFile(buf.getvalue(), filename=buf.name),
                            caption=current_caption
                        )
                    except Exception as e:
                        m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[видео — ошибка: {e}]")

                elif isinstance(attach, FileAttach):
                    try:
                        file = await asyncio.wait_for(max_client.get_file_by_id(chat_id=actual_chat_id, message_id=msg.id, file_id=attach.file_id), timeout=15)
                        buf = await download_bytes(file.url, getattr(attach, "filename", "file"))
                        m = await safe_tg_send(
                            tg_bot.send_document, chat_id,
                            document=types.BufferedInputFile(buf.getvalue(), filename=buf.name),
                            caption=current_caption
                        )
                    except Exception as e:
                        m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[файл — ошибка: {e}]")

                elif isinstance(attach, AudioAttach):
                    try:
                        buf = await download_bytes(attach.url, "voice.ogg")
                        m = await safe_tg_send(
                            tg_bot.send_voice, chat_id,
                            voice=types.BufferedInputFile(buf.getvalue(), filename=buf.name),
                            caption=current_caption
                        )
                    except Exception as e:
                        m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[голосовое — ошибка: {e}]")

                elif isinstance(attach, StickerAttach):
                    try:
                        url = attach.lottie_url or attach.url
                        buf = await download_bytes(url, "sticker.webp")
                        m = await safe_tg_send(
                            tg_bot.send_sticker, chat_id,
                            sticker=types.BufferedInputFile(buf.getvalue(), filename=buf.name)
                        )
                        if current_caption:
                            await safe_tg_send(tg_bot.send_message, chat_id, current_caption)
                    except Exception as e:
                        m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[стикер — ошибка: {e}]")

                else:
                    m = await safe_tg_send(tg_bot.send_message, chat_id, f"{current_caption or ''}\n[неизвестное вложение]")

                if m and not sent_tg_msg:
                    sent_tg_msg = m

        elif msg.text:
            clean_text = clean_max_text(msg.text)
            full_text = f"🗣️ {name}:\n{clean_text}"
            sent_tg_msg = await safe_tg_send(tg_bot.send_message, chat_id, full_text)

        if sent_tg_msg:
            save_msg_mapping(sent_tg_msg.message_id, msg.id)

    except Exception as e:
        print(f"[ОШИБКА пересылки msg.id={msg.id}]: {e}")
    finally:
        mark_sent(msg.id)

# ─── Отправка TG→MAX (Медиа) ──────────────────────────────

async def forward_media_to_max(message, name, max_chat_id):
    file_id = None
    file_name = f"file_{message.message_id}"
    AttachClass = None
    
    def get_class(*names):
        for n in names:
            if hasattr(pymax.files, n):
                return getattr(pymax.files, n)
        return None

    if message.photo:
        file_id = message.photo[-1].file_id
        file_name += ".jpg"
        AttachClass = get_class("Photo", "File")
    elif message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name or file_name + ".dat"
        AttachClass = get_class("File")
    elif message.animation or message.video or message.voice or message.audio or message.sticker:
        text = message.caption or message.text or ""
        media_type = "Медиафайл"
        if message.animation: media_type = "GIF"
        elif message.video: media_type = "Видео"
        elif message.voice or message.audio: media_type = "Голосовое/Аудио"
        elif message.sticker: media_type = "Стикер"
            
        warning = f"[{media_type} не перенесён — поддерживаются только фото и документы]"
        full_text = f"🗣️[TG] {name}:\n{warning}"
        if text:
            full_text += f"\n{text}"
            
        try:
            sent_max = await asyncio.wait_for(safe_max_send(full_text, max_chat_id), timeout=15)
            if sent_max and hasattr(sent_max, "id"):
                save_msg_mapping(message.message_id, sent_max.id)
        except Exception as e:
            print(f"[!] Не удалось отправить предупреждение: {e}")
            
        return True

    if not file_id:
        return False

    if not AttachClass:
        await safe_tg_send(message.answer, "❌ Библиотека pymax пока не поддерживает этот тип файлов.")
        return True

    base_text = message.text or message.caption or ""

    status_msg = await safe_tg_send(message.answer, "⏳ Скачиваю файл...")
    file_path = ""
    
    try:
        # 1. СКАЧИВАНИЕ
        file_info = await tg_bot.get_file(file_id)
        url = f"https://api.telegram.org/file/bot{TG_BOT_TOKEN}/{file_info.file_path}"
        buf = await download_bytes(url, file_name)
        
        os.makedirs(WORK_DIR, exist_ok=True)
        file_path = os.path.abspath(os.path.join(WORK_DIR, file_name))
        
        with open(file_path, "wb") as f:
            f.write(buf.getvalue())
            
        # 2. НОРМАЛИЗАЦИЯ
        if message.photo and HAS_PILLOW:
            await safe_tg_send(tg_bot.edit_message_text, text="⏳ Очистка метаданных фото...", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
            try:
                temp_path = file_path + ".tmp"
                with Image.open(file_path) as img:
                    img = ImageOps.exif_transpose(img) 
                    img.load()
                    clean_img = Image.new("RGB", img.size, (255, 255, 255))
                    if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                        clean_img.paste(img, mask=img.convert('RGBA').split()[3])
                    else:
                        clean_img.paste(img)
                    clean_img.thumbnail((2048, 2048), getattr(Image, "Resampling", Image).LANCZOS)
                    clean_img.save(temp_path, "JPEG", quality=90, optimize=False, progressive=False)
                os.replace(temp_path, file_path)
            except Exception as e:
                print(f"[!] Ошибка обработки фото Pillow: {e}")

        # 3. ОТПРАВКА С МАРКЕРОМ В СЛОВЕ [МЕДИА]
        await safe_tg_send(tg_bot.edit_message_text, text="⏳ Отправка в MAX...", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
        
        # Генерируем 2 случайных символа Брайля (до и после "медиа")
        m1 = chr(random.randint(0x2801, 0x28FF))
        m2 = chr(random.randint(0x2801, 0x28FF))
        marker_str = f"[{m1}медиа{m2}]"

        if base_text:
            attempt_caption = f"🗣️ [TG] {name}:\n{marker_str} {base_text}"
        else:
            attempt_caption = f"🗣️ [TG] {name}:\n{marker_str}"

        sent_max_id = None
        try:
            sent_max = await asyncio.wait_for(
                safe_max_send(attempt_caption, max_chat_id, attachment=AttachClass(path=file_path)),
                timeout=25.0
            )
            if sent_max and hasattr(sent_max, "id"):
                sent_max_id = sent_max.id
        except Exception as e:
            print(f"🔴[ОШИБКА ОТПРАВКИ]: {type(e).__name__} - {e}")

        # 4. ФОЛЛБЕК НА ДОКУМЕНТ (При ошибке API)
        if not sent_max_id and AttachClass.__name__ == "Photo":
            FallbackClass = get_class("File")
            if FallbackClass and AttachClass != FallbackClass:
                await safe_tg_send(tg_bot.edit_message_text, text="🔄 Ошибка отправки фото. Пробую отправить как файл...", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
                try:
                    sent_max = await asyncio.wait_for(
                        safe_max_send(attempt_caption, max_chat_id, attachment=FallbackClass(path=file_path)),
                        timeout=25.0
                    )
                    if sent_max and hasattr(sent_max, "id"):
                        sent_max_id = sent_max.id
                except Exception as e:
                    print(f"🔴[ОШИБКА ОТПРАВКИ ФАЙЛА]: {e}")

        # 5. ИТОГ
        if sent_max_id:
            save_msg_mapping(message.message_id, sent_max_id)
            try:
                await tg_bot.delete_message(chat_id=status_msg.chat.id, message_id=status_msg.message_id)
            except Exception:
                pass
        else:
            try:
                await safe_tg_send(tg_bot.edit_message_text, text="❌ Окончательная ошибка: Не удалось отправить файл в MAX.", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
            except Exception:
                pass
                
    except Exception as e:
        await safe_tg_send(message.answer, f"❌ Критическая ошибка медиа:\n`{e}`", parse_mode="Markdown")
        print(f"Media Error: {e}")
        
    finally:
        if file_path and os.path.exists(file_path):
            asyncio.create_task(delayed_remove(file_path, delay=15))
                
    return True

# ─── MAX события ──────────────────────────────────────────

@max_client.on_message()
async def on_max_message(msg: Message) -> None:
    cfg = load_config()
    max_chat_id = cfg.get("max_chat_id")
    if max_chat_id is None or msg.chat_id != max_chat_id:
        return
    await forward_to_tg(msg, TG_GROUP_ID, max_chat_id)

# ─── TG события ───────────────────────────────────────────

@dp.message()
async def on_tg_message(message: types.Message) -> None:
    if message.chat.id != TG_GROUP_ID:
        return

    cfg = load_config()
    max_chat_id = cfg.get("max_chat_id")
    if max_chat_id is None:
        return

    tg_user_id = message.from_user.id if message.from_user else None
    if tg_user_id is None:
        return

    text = message.text or message.caption or ""

    if text.lower().startswith("+имя "):
        new_name = text[5:].strip()
        if new_name:
            old_name = get_user_name(tg_user_id)
            
            if old_name is None:
                register_user(tg_user_id, new_name)
                await safe_max_send(f"🔔[TG] {new_name} вошёл в чат.", max_chat_id)
                await safe_tg_send(message.answer, f"✅ Зарегистрирован как: {new_name}")
            elif old_name != new_name:
                register_user(tg_user_id, new_name)
                await safe_max_send(f"🔔[TG] {old_name} сменил имя на {new_name}", max_chat_id)
                await safe_tg_send(message.answer, f"✅ Имя изменено на: {new_name}")
            else:
                await safe_tg_send(message.answer, f"✅ Твоё имя уже {new_name}.")

            queued = pending_messages.pop(tg_user_id,[])
            for queued_text in queued:
                try:
                    await safe_max_send(f"🗣️[TG] {new_name}:\n{queued_text}", max_chat_id)
                except Exception as e:
                    await safe_tg_send(message.answer, f"❌ Не удалось отправить в MAX: {e}")
        return

    name = get_user_name(tg_user_id)
    if name is None:
        tg_first = message.from_user.first_name if message.from_user else "?"
        pending_messages.setdefault(tg_user_id,[]).append(text)
        await safe_tg_send(
            message.answer,
            f"👋 Привет, {tg_first}! напиши свои РЕАЛЬНЫЕ Имя и Фамилию,\n"
            f"чтобы зарегистрироваться, именно в таком формате:\n"
            f"`+имя Иван Иванов`\n\n"
            f"Твоё сообщение будет отправлено после регистрации.",
            parse_mode="Markdown",
        )
        return

    try:
        handled_media = await forward_media_to_max(message, name, max_chat_id)
        if handled_media:
            return

        if text:
            full_text = f"🗣️ [TG] {name}:\n{text}"
            sent_max = await safe_max_send(full_text, max_chat_id)
            if sent_max and hasattr(sent_max, "id"):
                save_msg_mapping(message.message_id, sent_max.id)

    except Exception as e:
        await safe_tg_send(message.answer, f"❌ Ошибка отправки в MAX: {e}")

# ─── Выбор чата при первом запуске ────────────────────────

async def first_run_select_chat() -> int:
    print("\n╔══════════════════════════════════════════╗")
    print("║   Первый запуск — выбери MAX-чат         ║")
    print("╚══════════════════════════════════════════╝\n")

    entries =[]
    for d in max_client.dialogs:
        other_id = next((uid for uid in d.participants if uid != max_client.me.id), d.owner)
        try:
            user = await max_client.get_user(other_id)
            if user and user.names:
                n = user.names[0]
                name = f"{n.first_name or ''} {n.last_name or ''}".strip()
            else:
                name = f"User#{other_id}"
        except Exception:
            name = f"User#{other_id}"
        entries.append({"id": d.id, "name": name, "icon": "💬"})

    for c in max_client.chats:
        entries.append({"id": c.id, "name": c.title or f"Chat#{c.id}", "icon": "👥"})

    for ch in max_client.channels:
        entries.append({"id": ch.id, "name": getattr(ch, "title", None) or f"Channel#{ch.id}", "icon": "📢"})

    for i, e in enumerate(entries, 1):
        print(f"  {i:>2}. {e['icon']} {e['name']}")

    print()
    while True:
        try:
            raw = input("Введи номер чата: ").strip()
            idx = int(raw) - 1
            if 0 <= idx < len(entries):
                chosen = entries[idx]
                print(f"\n✅ Выбран чат: {chosen['icon']} {chosen['name']} (ID: {chosen['id']})\n")
                return chosen["id"]
        except (ValueError, KeyboardInterrupt):
            pass
        print("Неверный номер, попробуй ещё раз.")

# ─── Старт ────────────────────────────────────────────────

@max_client.on_start
async def on_start() -> None:
    global IS_STARTED, TG_HELLO_SENT
    cfg = load_config()

    if "max_chat_id" not in cfg:
        max_chat_id = await first_run_select_chat()
        cfg["max_chat_id"] = max_chat_id
        save_config(cfg)
    else:
        max_chat_id = cfg["max_chat_id"]

    if IS_STARTED:
        print(f"🔄 MAX переподключен (Сокет восстановлен).")
        return
        
    IS_STARTED = True
    me_name = max_client.me.names[0].first_name if max_client.me.names else str(max_client.me.id)
    print(f"✅ MAX авторизован как: {me_name} (ID: {max_client.me.id})")
    print(f"📌 Привязан к MAX-чату ID: {max_chat_id}")

    await asyncio.sleep(2)

    if not TG_HELLO_SENT:
        try:
            await safe_tg_send(tg_bot.send_message, TG_GROUP_ID, "🤖 Мост MAX↔TG запущен. Загружаю историю...")
            TG_HELLO_SENT = True
        except Exception:
            pass

    print(f"📥 Загружаю историю (до {HISTORY_LIMIT} сообщений)...")
    try:
        history = await max_client.fetch_history(max_chat_id, backward=HISTORY_LIMIT)
        if history:
            sent_count = 0
            for msg in reversed(history):  
                if not is_sent(msg.id):
                    await forward_to_tg(msg, TG_GROUP_ID, max_chat_id)
                    sent_count += 1
                    await asyncio.sleep(0.7)
            print(f"✅ Отправлено {sent_count} новых сообщений из истории.")
        else:
            print("История пуста.")
    except Exception as e:
        print(f"[!] Ошибка при загрузке истории: {e}")

    try:
        await safe_tg_send(tg_bot.send_message, TG_GROUP_ID, "✅ История загружена. Слушаю новые сообщения.")
    except Exception:
        pass

# ─── Точка входа ──────────────────────────────────────────

async def main() -> None:
    print("🚀 Запускаю бридж MAX↔Telegram...")
    tg_task = asyncio.create_task(dp.start_polling(tg_bot))

    try:
        await max_client.start()
    finally:
        await max_client.close()
        tg_task.cancel()
        await tg_bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ Остановлено пользователем.")
        os._exit(0)
