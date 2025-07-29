import aiohttp, ssl, json
import asyncio
import logging
import re
import tempfile
import redis.asyncio as redis
from pathlib import Path
from typing import Dict, Tuple
import subprocess, pathlib
import random, threading
from typing import Optional, List
import yt_dlp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    FSInputFile, BufferedInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from pythonProject2.streamer import enqueue_stream

TIMEOUT   = aiohttp.ClientTimeout(total=1800, sock_read=1800)   # 30 мин на upload
UA_HEADER = {"User-Agent": "curl/7.87.0"}                       # Cloudflare friendly
MAX_TRIES = 3                                                   # повторов при сбое
BACKOFF   = 2

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
TOKEN = '7459959678:AAEpyKF35x2ivY-e1UtFcybIGyO1H6fD4sE'
LOCAL_API = TelegramAPIServer.from_base("http://127.0.0.1:8081")
session = AiohttpSession(api=LOCAL_API)

YOUTUBE_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([\w-]{11})"
)

# Simple in‑memory storage: (chat_id, message_id) -> {url, best}
CONTEXT: Dict[Tuple[int, int], Dict] = {}


REDIS_URL   = "redis://localhost:6379/0"
QUEUE_KEY   = "dl:queue"
MAX_WORKERS = 5
redis_pool  = redis.from_url(
    REDIS_URL, encoding="utf-8", decode_responses=True
)
sem = asyncio.Semaphore(MAX_WORKERS)



# ----------------------------------------------------------------------
# Helpers (blocking parts run in ThreadPool via run_in_thread)
# ----------------------------------------------------------------------
async def run_in_thread(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args))

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE
FILE_IO_LIMIT_MB = 1

bot = Bot(token=TOKEN, session=session, timeout=TIMEOUT)
router = Dispatcher()

FALLBACK_SERVERS: List[str] = [
    "store1", "store2", "store3", "store4", "store5",
    "store6", "store7", "store9", "srv-store8", "srv-store10"
]


async def _safe_json(resp: aiohttp.ClientResponse) -> Optional[dict]:
    try:
        return json.loads(await resp.text())
    except json.JSONDecodeError:
        return None


async def _get_server(session: aiohttp.ClientSession) -> str:
    """
    Возвращает id свободного сервера (store1 …), даже если основной вызов зафейлился.
    """
    # 1) новый энд‑поинт /servers (возвращает список всех)
    try:
        async with session.get("https://api.gofile.io/servers") as r:
            js = await _safe_json(r)
            if js and js.get("status") == "ok":
                servers = list(js["data"]["servers"].keys())
                return random.choice(servers)
    except Exception:
        pass

    # 2) старый /getServer
    try:
        async with session.get("https://api.gofile.io/getServer") as r:
            js = await _safe_json(r)
            if js and js.get("status") == "ok":
                return js["data"]["server"]
    except Exception:
        pass

    # 3) запасной жёсткий список
    return random.choice(FALLBACK_SERVERS)


async def upload_to_gofile(path: Path, tries: int = 3) -> str:
    """
    Загружает файл (до 10 ГБ) на gofile.io, возвращая ссылку на страницу скачивания.
    """
    async with aiohttp.ClientSession(
        timeout=TIMEOUT,
        connector=aiohttp.TCPConnector(ssl=ssl_ctx, limit=4),
        headers=UA_HEADER
    ) as ses:
        backoff = 2
        for attempt in range(1, tries + 1):
            try:
                server = await _get_server(ses)

                with path.open("rb") as f:
                    form = aiohttp.FormData()
                    form.add_field("file", f, filename=path.name, content_type="video/mp4")

                    async with ses.post(f"https://{server}.gofile.io/uploadFile", data=form) as r:
                        js = await _safe_json(r)
                        if js and js.get("status") == "ok":
                            return js["data"]["downloadPage"]
                        raise RuntimeError(js.get("message", "upload failed") if js else "upload: not JSON")

            except Exception as e:
                if attempt == tries:
                    raise RuntimeError(f"Gofile upload failed: {e}") from e
                await asyncio.sleep(backoff + random.random())
                backoff *= 2


def _calc_size(fmt: dict, duration: int) -> int:
    """Approximate size in bytes or ∞ if cannot guess."""
    if fmt.get("filesize"):
        return fmt["filesize"]
    if fmt.get("filesize_approx"):
        return fmt["filesize_approx"]
    bitrate = fmt.get("tbr") or fmt.get("abr") or 0  # kbit/s
    if bitrate and duration:
        return int(bitrate * 125 * duration)
    return float("inf")


# 1. Один‑единственный экземпляр YoutubeDL на всё время работы бота
_YDL_LOCK = threading.Lock()
_YDL: yt_dlp.YoutubeDL | None = None

def _get_ytdl() -> yt_dlp.YoutubeDL:
    global _YDL
    with _YDL_LOCK:
        if _YDL is None:
            _YDL = yt_dlp.YoutubeDL({
                "skip_download": True,
                "quiet": True,
                "no_warnings": True,

                # берем только нужные потоки прямо на сервере:
                #  • DASH‑видео MP4/H.264 + лучшую M4A‑аудио
                #  • если DASH нет, то лучший прогрессив MP4/H.264
                "format": ("bv*[vcodec^=avc1][ext=mp4]+ba[ext=m4a]"
                           "/best[ext=mp4][vcodec^=avc1]"),

                # пропускаем DASH‑ и HLS‑манифест, сразу Android‑клиент
                "extractor_args": {
                    "youtube": ["skip=dash,hls", "player_client=android"]
                },

                "forcejson": True,   # только JSON, никакой попытки качать
                "simulate": True,
            })
        return _YDL


def _best_formats(url: str):
    ydl = _get_ytdl()
    info = ydl.extract_info(url, download=False)

    duration = info.get("duration", 0) or 0
    best = {}

    # — найдём минимальный размер AAC/M4A аудио (для DASH‑видео) —
    audio_min = min(
        (
            fmt.get("filesize")
            or fmt.get("filesize_approx")
            or int((fmt.get("abr") or 0) * 125 * duration)        # abr kbit/s ➜ bytes
            for fmt in info["formats"]
            if fmt.get("vcodec") == "none" and fmt.get("ext") == "m4a"
        ),
        default=0,
    )

    # — перебираем только MP4/H.264 видео‑потоки —
    for fmt in info["formats"]:
        if fmt.get("ext") != "mp4":
            continue
        vcodec = fmt.get("vcodec") or ""
        if not vcodec.startswith("avc") or vcodec == "none":
            continue

        h = fmt.get("height") or 0
        if not h:
            continue

        size = (
            fmt.get("filesize")
            or fmt.get("filesize_approx")
            or int((fmt.get("tbr") or 0) * 125 * duration)        # tbr kbit/s ➜ bytes
        )

        selector = fmt["format_id"]
        if fmt.get("acodec") == "none":          # DASH‑видео без звука
            size += audio_min                    # ← прибавляем вес аудио
            selector += "+bestaudio"

        if h not in best or size < best[h]["size"]:
            best[h] = {"selector": selector, "size": size}

    return best, duration


async def get_best_formats(url: str):
    return await run_in_thread(_best_formats, url)


async def _download_video(url: str, selector: str, tmp_dir: Path) -> Path:
    """Download via yt‑dlp in thread; return file path."""

    def _dl():
        ydl_opts = {
            "format": selector,
            "outtmpl": str(tmp_dir / "%(_id)s_%(height)sp.mp4"),
            # ensure container mp4 after merge/recode if needed
            "merge_output_format": "mp4",
            "postprocessors": [
                {
                    "key": "FFmpegVideoConvertor",
                    "preferedformat": "mp4",
                }
            ],
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info))

    return await run_in_thread(_dl)

def get_wh(path: pathlib.Path) -> tuple[int, int]:
    """Вернёт (width, height) для первого видеопотока."""
    meta = subprocess.check_output([
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-select_streams", "v:0", "-show_entries", "stream=width,height",
        str(path)
    ])
    w, h = json.loads(meta)["streams"][0].values()
    return int(w), int(h)

# ----------------------------------------------------------------------
# Bot handlers
# ----------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(msg: Message):
    await msg.answer(
        "Привет! 🙃 Пришли ссылку на YouTube‑видео"
    )

async def _oembed_thumb(video_id: str, ses: aiohttp.ClientSession) -> bytes | None:
    """Берём thumbnail_url из oEmbed, если классические jpg отсутствуют."""
    api = (
        "https://www.youtube.com/oembed"
        f"?url=https://youtu.be/{video_id}&format=json"
    )
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
    """Возвращает (объект_или_URL, is_url: bool)."""
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_ctx), timeout=TIMEOUT
    ) as ses:

        # 1️⃣ пытаемся классические jpg (принимаем 200‑299)
        for suffix in _VARIANTS:
            url = f"https://img.youtube.com/vi/{video_id}/{suffix}"
            async with ses.head(url, allow_redirects=True) as r:
                if 200 <= r.status < 300 and r.headers.get("Content-Type", "").startswith("image/"):
                    return url, True          # Telegram умеет URL

        # 2️⃣ fallback: oEmbed‑thumbnail (webp/jpg)
        if (img := await _oembed_thumb(video_id, ses)):
            buf = BufferedInputFile(img, filename=f"{video_id}.jpg")
            return buf, False                # Telegram получит файл

        # 3️⃣ крайний случай — пробуем всё‑таки hqdefault.jpg GET‑ом
        url = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
        async with ses.get(url) as r:
            buf = BufferedInputFile(await r.read(), filename=f"{video_id}.jpg")
        return buf, False

@router.message(F.text.regexp(YOUTUBE_URL_RE))
async def handle_youtube(msg: Message):
    find_vid = await msg.answer("⚙️ Поиск видео…")
    url = YOUTUBE_URL_RE.search(msg.text).group(0)
    video_id = YOUTUBE_URL_RE.search(msg.text).group(1)

    # ── 1. title + preview (oEmbed → fallback yt‑dlp) ────────────────
    title, thumb_obj = None, None
    try:
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=ssl_ctx)) as ses:
            async with ses.get(
                    "https://www.youtube.com/oembed",
                    params={"url": url, "format": "json"},
                    timeout=aiohttp.ClientTimeout(total=4)
            ) as r:
                oembed = await r.json(content_type=None)   # не проверяем MIME
        title = oembed["title"]
    except Exception:
        pass                                            # oEmbed не сработал

    if not title:
        info = await run_in_thread(lambda: _get_ytdl().extract_info(url, download=False))
        title = info.get("title", "Видео")

    # — картинка (проверяем maxres→hq→mq) —
    thumb_obj, _ = await best_youtube_thumb(video_id)

    # ── 2. MP4‑форматы параллельно —────────────────────────────────────
    best_dict, _ = await get_best_formats(url)
    if not best_dict:
        await msg.answer("😔 Не удалось найти видео.")
        return

    # ── 3. клавиатура —────────────────────────────────────────────────
    kb = InlineKeyboardBuilder()
    mb = 0
    for h in sorted(best_dict):
        mb = round(best_dict[h]["size"] / 1_048_576, 1)
        kb.row(InlineKeyboardButton(text=f"⚡️ {h}p • {mb} MB", callback_data=f"dl|{h}"))

    # ── 4. единое сообщение —──────────────────────────────────────────
    await find_vid.delete()
    sent = await msg.answer_photo(
        thumb_obj,
        caption=f"<b>{title}</b>\n\nВыберите качество ↓",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
        disable_web_page_preview=True,
    )

    # ── 5. сохраняем контекст —────────────────────────────────────────
    CONTEXT[(sent.chat.id, sent.message_id)] = {"url": url, "best": best_dict, "orig_id": msg.message_id, "title": title,
                                                "mb": mb}


async def process_job(job: dict):
    chat_id  = job["chat_id"]
    reply_id = job["reply_id"]
    url      = job["url"]
    selector = job["selector"]
    height   = job["height"]
    title = job["title"]
    sizefile = job["size_bytes"] / 1_048_576

    with tempfile.TemporaryDirectory() as tmp:
        if sizefile <= FILE_IO_LIMIT_MB:
            status = await bot.send_message(chat_id,
                                            f"⬇️ Скачиваю {height}p видео...", reply_to_message_id=reply_id)
            try:
                file_path = await _download_video(url, selector, Path(tmp))
            except Exception as e:
                print(e)
                await status.edit_text(f"Ошибка загрузки")
                return

            await status.edit_text(f"⬇️ Загрузка файла в Telegram")
            w, h = get_wh(file_path)
            await bot.send_video(
                chat_id,
                FSInputFile(file_path),
                width=w, height=h,
                supports_streaming=True,
                reply_to_message_id=reply_id,
                request_timeout=7200
            )
            await status.delete()
        else:
            jid = await enqueue_stream(url, selector, title)  # 👈
            link = f"http://45.128.99.176/dl/{jid}"
            await bot.send_message(chat_id,
                f"Файл большой, скачайте по ссылке:\n{link}",
                disable_web_page_preview=True
            )
            return
        await bot.send_message(chat_id, 'Пришлите новую ссылку, чтобы скачать видео 🎥'
                               )


async def handle_job(raw: str):
    job = json.loads(raw)
    chat_id = job["chat_id"]
    try:
        await process_job(job)      # ваша логика "скачать → отправить"
    finally:
        await redis_pool.delete(BUSY_KEY(chat_id))
        sem.release()

async def worker():
    while True:
        _, raw = await redis_pool.brpop(QUEUE_KEY, timeout=0)   # ждём job
        await sem.acquire()                                    # ≤ 5 одновременно
        asyncio.create_task(handle_job(raw))


BUSY_KEY = lambda cid: f"busy:{cid}"   # busy:123456789
BUSY_TTL = 7200

@router.callback_query(F.data.startswith("dl|"))
async def callback_download(call: CallbackQuery):
    # --- достаём из локального CONTEXT ---
    key = (call.message.chat.id, call.message.message_id)
    context = CONTEXT.get(key)
    if not context:
        await call.answer("Пришлите ссылку заново.", show_alert=True)
        return

    height = int(call.data.split("|", 1)[1])
    if height not in context["best"]:
        await call.answer("Это качество недоступно.", show_alert=True)
        return

    fmt_info = context["best"][height]
    selector = context["best"][height]["selector"]
    url      = context["url"]
    size_bytes = fmt_info["size"]

    chat_id = call.message.chat.id

    # ── 1. пытаемся «заблокировать» пользователя ──────────────
    locked = await redis_pool.set(BUSY_KEY(chat_id), 1, nx=True, ex=BUSY_TTL)
    if not locked:
        await call.answer("⏳ Подождите, загрузка...", show_alert=True)
        return

    # --- формируем задачу ---
    job = {
        "chat_id":  call.message.chat.id,
        "reply_id": context["orig_id"],
        "url":      url,
        "selector": selector,
        "height":   height,
        "title": context["title"],
        "size_bytes": size_bytes
    }

    # --- кладём в очередь (хвост) ---
    await redis_pool.rpush(QUEUE_KEY, json.dumps(job))

    # --- ответ пользователю ---
    await call.answer("✅ Добавлено в очередь")
    await call.message.edit_reply_markup()   # убираем кнопки
    await call.message.delete()
    CONTEXT.pop(key, None)                   # локальный контекст больше не нужен


async def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.create_task(worker())  # фон‑очередь
    await router.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())