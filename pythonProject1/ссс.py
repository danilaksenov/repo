import aiohttp, ssl, json
import asyncio
import logging
import re
import tempfile
import redis.asyncio as redis
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import subprocess, pathlib, threading, random

import yt_dlp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    FSInputFile,
    BufferedInputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

# если используете стример
# from pythonProject2.streamer import enqueue_stream

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
TOKEN = "7459959678:AAEpyKF35x2ivY-e1UtFcybIGyO1H6fD4sE"
LOCAL_API = TelegramAPIServer.from_base("http://127.0.0.1:8081")
session = AiohttpSession(api=LOCAL_API)

YOUTUBE_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([\w-]{11})"
)

# Simple in-memory storage: (chat_id, message_id) -> {url, best}
CONTEXT: Dict[Tuple[int, int], Dict] = {}

REDIS_URL = "redis://localhost:6379/0"
QUEUE_KEY = "dl:queue"
MAX_WORKERS = 8
redis_pool = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
sem = asyncio.Semaphore(MAX_WORKERS)

TIMEOUT = aiohttp.ClientTimeout(total=1800, sock_read=1800)  # 30 мин на upload
UA_HEADER = {"User-Agent": "curl/7.87.0"}  # Cloudflare friendly
FILE_IO_LIMIT_MB = 1950  # лимит размера для прямой отправки в ТГ

bot = Bot(token=TOKEN, session=session, timeout=TIMEOUT)
router = Dispatcher()

ssl_ctx = ssl.create_default_context()


# ----------------------------------------------------------------------
# Разное
# ----------------------------------------------------------------------
FALLBACK_SERVERS: List[str] = [
    "store1",
    "store2",
    "store3",
    "store4",
    "store5",
    "store6",
    "store7",
    "store9",
    "srv-store8",
    "srv-store10",
]

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🔍"), KeyboardButton(text="❓")]],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="🔗 Вставьте ссылку сюда",
)

@router.message(lambda m: m.text and "🔍" in m.text.lower())
async def cmd_search(msg: Message):
    await msg.answer("Пришлите ссылку на видео *youtube* или *rutube*", reply_markup=REPLY_KB, parse_mode="MarkdownV2")


@router.message(lambda m: m.text and "❓" in m.text.lower())
async def cmd_help(msg: Message):
    await msg.answer("Это бот, который поможет загрузить видео с *youtube* или *rutube* \n\nПросто пришлите ссылку на видео 🎥",
                     reply_markup=REPLY_KB, parse_mode="MarkdownV2")

async def _safe_json(resp: aiohttp.ClientResponse) -> Optional[dict]:
    try:
        return json.loads(await resp.text())
    except json.JSONDecodeError:
        return None

# ----------------------------------------------------------------------
# yt-dlp helpers
# ----------------------------------------------------------------------
def _calc_size(fmt: dict, duration: int) -> int:
    """Approximate size in bytes (∞ если нельзя оценить)."""
    if fmt.get("filesize"):
        return fmt["filesize"]
    if fmt.get("filesize_approx"):
        return fmt["filesize_approx"]
    bitrate = fmt.get("tbr") or fmt.get("abr") or 0  # kbit/s
    if bitrate and duration:
        return int(bitrate * 125 * duration)
    return float("inf")


# 1. Один-единственный экземпляр YoutubeDL на всё время работы бота
_YDL_LOCK = threading.Lock()
_YDL: yt_dlp.YoutubeDL | None = None


def _get_ytdl() -> yt_dlp.YoutubeDL:
    global _YDL
    with _YDL_LOCK:
        if _YDL is None:
            _YDL = yt_dlp.YoutubeDL(
                {
                    "skip_download": True,
                    "quiet": True,
                    "no_warnings": True,
                    # понимайте только MP4-видео + лучшую M4A-дорожку
                    "format": (
                        "bv*[vcodec^=avc1][ext=mp4]+ba[ext=m4a]/best[ext=mp4][vcodec^=avc1]"
                    ),
                    "extractor_args": {
                        "youtube": ["skip=dash,hls", "player_client=android"]
                    },
                    "forcejson": True,
                    "simulate": True,
                }
            )
        return _YDL


def _best_formats(url: str):
    """Собирает лучшие видео-варианты + минимальное аудио."""
    ydl = _get_ytdl()
    info = ydl.extract_info(url, download=False)

    duration = info.get("duration", 0) or 0
    best: Dict = {}

    # ① минимальный m4a-поток
    audio_fmt = None
    audio_size = float("inf")
    for fmt in info["formats"]:
        if fmt.get("vcodec") == "none" and fmt.get("ext") == "m4a":
            size = _calc_size(fmt, duration)
            if size < audio_size:
                audio_fmt, audio_size = fmt, size

    if audio_fmt:
        best["aud"] = {"selector": audio_fmt["format_id"], "size": audio_size}

    # ② перебираем только MP4/H.264-видео-потоки
    for fmt in info["formats"]:
        if fmt.get("ext") != "mp4":
            continue
        vcodec = fmt.get("vcodec") or ""
        if not vcodec.startswith("avc") or vcodec == "none":
            continue

        h = fmt.get("height") or 0
        if not h:
            continue

        size = _calc_size(fmt, duration)
        selector = fmt["format_id"]

        # если DASH-видео без звука, прибавляем вес аудио
        if fmt.get("acodec") == "none" and audio_fmt:
            size += audio_size
            selector += f"+{audio_fmt['format_id']}"

        if h not in best or size < best[h]["size"]:
            best[h] = {"selector": selector, "size": size}

    return best, duration


async def get_best_formats(url: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _best_formats, url)

async def _download_video(url: str, selector: str, tmp_dir: Path) -> Path:
    """
    Скачивает DASH-видео и аудио через aria2c, затем remux-ит в MP4.
    Возвращает Path к готовому файлу.
    """
    def _dl() -> Path:
        ydl_opts = {
            # 1. какой поток вытягивать
            "format": selector,                      # например "137+140"
            "outtmpl": str(tmp_dir / "%(_id)s_%(height)sp.%(ext)s"),

            # 2. Внешний загрузчик
            "external_downloader": "aria2c",
            "external_downloader_args": [
                # 16 соединений, кусок 2 МБ
                "-x16", "-k2M",
                # докачка, таймауты
                "--continue=true", "--retry-wait=3",
            ],

            # 3. Пост-процесс только REMUX (быстро!)
            "postprocessors": [
                {"key": "FFmpegVideoRemuxer", "preferedformat": "mp4"},
            ],

            # 4. Тихий режим
            "quiet": True,
            "no_warnings": True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info)).with_suffix(".mp4")

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _dl)


async def _download_video_convert(url: str, selector: str, tmp_dir: Path) -> Path:
    """Скачивает одно видео (mp4) в отдельном потоке."""
    def _dl():
        ydl_opts = {
            "format": selector,
            "outtmpl": str(tmp_dir / "%(_id)s_%(height)sp.mp4"),
            "merge_output_format": "mp4",
            "postprocessors": [
                {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
            ],
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info))

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _dl)


async def _download_audio(url: str, selector: str, tmp_dir: Path) -> Path:
    """Скачивает одно аудио (m4a) в отдельном потоке."""
    def _dl():
        ydl_opts = {
            "format": selector,  # зачастую «140»
            "outtmpl": str(tmp_dir / "%(_id)s.m4a"),
            "merge_output_format": "m4a",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "m4a",
                    "preferredquality": "0",
                }
            ],
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info))

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _dl)


def get_wh(path: pathlib.Path) -> tuple[int, int]:
    """Возвращает (width, height) первой видеодорожки."""
    meta = subprocess.check_output(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            str(path),
        ]
    )
    w, h = json.loads(meta)["streams"][0].values()
    return int(w), int(h)


# ----------------------------------------------------------------------
# Bot handlers
# ----------------------------------------------------------------------
@router.message(CommandStart())
async def cmd_start(msg: Message):
    await msg.answer("Привет! 🙃 Пришли ссылку на YouTube-видео", reply_markup=REPLY_KB)


async def _oembed_thumb(video_id: str, ses: aiohttp.ClientSession) -> bytes | None:
    """Берём thumbnail_url из oEmbed, если классические jpg отсутствуют."""
    api = f"https://www.youtube.com/oembed?url=https://youtu.be/{video_id}&format=json"
    async with ses.get(api) as r:
        if r.status != 200:
            return None
        thumb_url = (await r.json(content_type=None)).get("thumbnail_url")
    async with ses.get(thumb_url) as r:
        if r.status == 200 and r.headers.get("Content-Type", "").startswith("image/"):
            return await r.read()
    return None


_VARIANTS = ["maxresdefault.jpg", "hqdefault.jpg", "mqdefault.jpg", "sddefault.jpg"]


async def best_youtube_thumb(video_id: str):
    """Возвращает (объект/URL, is_url:boolean)."""
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_ctx), timeout=TIMEOUT
    ) as ses:
        # 1️⃣ классические jpg
        for suffix in _VARIANTS:
            url = f"https://img.youtube.com/vi/{video_id}/{suffix}"
            async with ses.head(url, allow_redirects=True) as r:
                if 200 <= r.status < 300 and r.headers.get("Content-Type", "").startswith(
                    "image/"
                ):
                    return url, True

        # 2️⃣ oEmbed
        if (img := await _oembed_thumb(video_id, ses)):
            buf = BufferedInputFile(img, filename=f"{video_id}.jpg")
            return buf, False

        # 3️⃣ крайний случай
        url = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
        async with ses.get(url) as r:
            buf = BufferedInputFile(await r.read(), filename=f"{video_id}.jpg")
        return buf, False


@router.message(F.text.regexp(YOUTUBE_URL_RE))
async def handle_youtube(msg: Message):
    find_vid = await msg.answer("⚙️ Поиск видео…")
    url = YOUTUBE_URL_RE.search(msg.text).group(0)
    video_id = YOUTUBE_URL_RE.search(msg.text).group(1)

    # 1. title
    title = None
    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_ctx)
        ) as ses:
            async with ses.get(
                "https://www.youtube.com/oembed",
                params={"url": url, "format": "json"},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as r:
                oembed = await r.json(content_type=None)
        title = oembed["title"]
    except Exception:
        pass

    if not title:
        title = (
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: _get_ytdl().extract_info(url, download=False)
            )
        ).get("title", "Видео")

    # 2. thumb
    thumb_obj, _ = await best_youtube_thumb(video_id)

    # 3. best formats
    best_dict, _ = await get_best_formats(url)
    if not best_dict:
        await msg.answer("😔 Не удалось найти видео.")
        return

    kb = InlineKeyboardBuilder()

    # аудио-кнопка
    aud_mb = round(best_dict["aud"]["size"] / 1_048_576, 1)
    kb.row(
        InlineKeyboardButton(text=f"🎧 Аудио • {aud_mb} MB", callback_data="dl|aud")
        )

    # 4. keyboard
    for h in sorted(k for k in best_dict if k != "aud"):
        mb = round(best_dict[h]["size"] / 1_048_576, 1)
        if mb < FILE_IO_LIMIT_MB:
            kb.row(
                InlineKeyboardButton(text=f"⚡️ {h}p • {mb} MB", callback_data=f"dl|{h}")
            )

    # 5. send
    await find_vid.delete()
    sent = await msg.answer_photo(
        thumb_obj,
        caption=f"<b>{title}</b>\n\nВыберите вариант ↓",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
        disable_web_page_preview=True,
    )

    # 6. context
    CONTEXT[(sent.chat.id, sent.message_id)] = {
        "url": url,
        "best": best_dict,
        "orig_id": msg.message_id,
        "title": title,
    }


# ----------------------------------------------------------------------
# Очередь и worker
# ----------------------------------------------------------------------
BUSY_KEY = lambda cid: f"busy:{cid}"
BUSY_TTL = 7200


@router.callback_query(F.data.startswith("dl|"))
async def callback_download(call: CallbackQuery):
    key = (call.message.chat.id, call.message.message_id)
    context = CONTEXT.get(key)
    if not context:
        await call.answer("Пришлите ссылку заново.", show_alert=True)
        return

    token = call.data.split("|", 1)[1]  # 'aud' или '720'
    if token == "aud":
        kind = "audio"
        fmt_info = context["best"]["aud"]
    else:
        kind = "video"
        try:
            height = int(token)
        except ValueError:
            await call.answer("Неверный формат.", show_alert=True)
            return
        if height not in context["best"]:
            await call.answer("Это качество недоступно.", show_alert=True)
            return
        fmt_info = context["best"][height]

    selector = fmt_info["selector"]
    url = context["url"]
    size_bytes = fmt_info["size"]

    chat_id = call.message.chat.id

    # блокируем пользователя
    locked = await redis_pool.set(BUSY_KEY(chat_id), 1, nx=True, ex=BUSY_TTL)
    if not locked:
        await call.answer("⏳ Подождите, загрузка…", show_alert=True)
        return

    # формируем job
    job = {
        "chat_id": chat_id,
        "reply_id": context["orig_id"],
        "url": url,
        "selector": selector,
        "kind": kind,
        "height": int(token) if kind == "video" else 0,
        "title": context["title"],
        "size_bytes": size_bytes,
    }

    await redis_pool.rpush(QUEUE_KEY, json.dumps(job))
    await call.answer("✅ Добавлено в очередь")
    await call.message.edit_reply_markup()
    await call.message.delete()
    CONTEXT.pop(key, None)


async def process_job(job: dict):
    chat_id = job["chat_id"]
    reply_id = job["reply_id"]
    url = job["url"]
    selector = job["selector"]
    kind = job["kind"]
    height = job.get("height", 0)
    title = job["title"]
    size_mb = job["size_bytes"] / 1_048_576

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)

        # Отправляем сразу, если файл не огромный
        if size_mb <= FILE_IO_LIMIT_MB:
            status = await bot.send_message(
                chat_id,
                f"⬇️ Скачиваю {'аудио' if kind=='audio' else f'{height}p видео'}…",
                reply_to_message_id=reply_id,
            )
            try:
                if kind == "audio":
                    file_path = await _download_audio(url, selector, tmp)
                else:
                    try:
                        file_path = await _download_video(url, selector, tmp)
                    except yt_dlp.utils.DownloadError:
                        return
                        # fallback на перекодирование (медленнее, но 100 % совместимо)
                        # file_path = await _download_video_convert(url, selector, tmp)
            except Exception as e:
                print(e)
                await status.edit_text("Ошибка загрузки")
                return

            await status.edit_text("⬆️ Отправляю…")
            if kind == "audio":
                await bot.send_audio(
                    chat_id,
                    FSInputFile(file_path),
                    title=title,
                    reply_to_message_id=reply_id,
                    request_timeout=7200,
                )
            else:
                w, h = get_wh(file_path)
                await bot.send_video(
                    chat_id,
                    FSInputFile(file_path),
                    width=w,
                    height=h,
                    supports_streaming=True,
                    reply_to_message_id=reply_id,
                    request_timeout=7200,
                )
            await status.delete()
        else:
            # большой файл → ссылка
            # jid = await enqueue_stream(url, selector, title)
            # link = f"http://45.128.99.176/dl/{jid}"
            # await bot.send_message(
            #     chat_id,
            #     f"Файл большой, скачайте по ссылке:\n{link}",
            #     disable_web_page_preview=True,
            # )
            # return
            pass

        await bot.send_message(
            chat_id, "Пришлите ссылку, чтобы скачать новое видео 🎥"
        )


async def handle_job(raw: str):
    job = json.loads(raw)
    chat_id = job["chat_id"]
    try:
        await process_job(job)
    finally:
        await redis_pool.delete(BUSY_KEY(chat_id))
        sem.release()


async def worker():
    while True:
        _, raw = await redis_pool.brpop(QUEUE_KEY, timeout=0)
        await sem.acquire()
        asyncio.create_task(handle_job(raw))


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
async def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.create_task(worker())
    await router.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
