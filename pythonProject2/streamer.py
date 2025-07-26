import asyncio
import tempfile
import aiofiles
import shutil
import yt_dlp
import uuid
from pathlib import Path
from pythonProject2.redis_client import r, JOB

# Папка для временных и окончательных файлов
DOWNLOAD_ROOT = Path(tempfile.gettempdir()) / "yt_stream"
DOWNLOAD_ROOT.mkdir(exist_ok=True)

async def stream_job(job_id: str, url: str, selector: str, title: str):
    loop = asyncio.get_event_loop()

    # 1) Получаем метаданные
    info = await loop.run_in_executor(
        None,
        lambda: yt_dlp.YoutubeDL({"quiet": True}).extract_info(url, download=False)
    )

    # 2) Разбираем selector на видео + аудио (если есть "+")
    parts = selector.split("+")
    dur = info.get("duration", 0) or 0

    if len(parts) > 1:
        video_sel, audio_sel = parts[0], parts[1]

        # находим видеопоток
        video_fmt = next((f for f in info["formats"] if f["format_id"] == video_sel), None)
        if not video_fmt:
            await r.hset(JOB(job_id), mapping={
                "status": "error",
                "msg":    f"Видео‑поток {video_sel} не найден"
            })
            return
        # находим аудиопоток
        if audio_sel == "bestaudio":
            audio_fmt = max(
                (f for f in info["formats"] if f.get("vcodec") == "none"),
                key=lambda f: (f.get("abr") or 0),
                default=None
            )
        else:
            audio_fmt = next((f for f in info["formats"] if f["format_id"] == audio_sel), None)

        if not audio_fmt:
            await r.hset(JOB(job_id), mapping={
                "status": "error",
                "msg":    f"Аудио‑поток {audio_sel} не найден"
            })
            return

        # размер видео + аудио
        v_size = video_fmt.get("filesize") or video_fmt.get("filesize_approx") \
                 or int((video_fmt.get("tbr") or 0) * 125 * dur)
        a_size = audio_fmt.get("filesize") or audio_fmt.get("filesize_approx") \
                 or int((audio_fmt.get("abr") or 0) * 125 * dur)
        total_bytes = v_size + a_size

    else:
        # простой селектор без "+"
        fmt = next((f for f in info["formats"] if f["format_id"] == selector), None)
        if not fmt:
            await r.hset(JOB(job_id), mapping={
                "status": "error",
                "msg":    f"Format {selector} not found"
            })
            return
        total_bytes = fmt.get("filesize") or fmt.get("filesize_approx") \
                      or int((fmt.get("tbr") or 0) * 125 * dur)

    # 3) Запускаем yt-dlp для стриминга в stdout
    tmp = DOWNLOAD_ROOT / f"{job_id}.partial"
    cmd = [
        "yt-dlp",
        "-f", selector,
        "--merge-output-format", "mp4",
        "-o", "-", url
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL
    )

    first_chunk = True
    async with aiofiles.open(tmp, "wb") as f:
        async for chunk in proc.stdout:
            await f.write(chunk)
            downloaded = tmp.stat().st_size

            # переключаем статус на "downloading" после первого мегабайта
            if first_chunk and downloaded > 1_000_000:
                first_chunk = False
                await r.hset(JOB(job_id), mapping={
                    "status":  "downloading",
                    "percent": "0%",
                    "file":    str(tmp),
                    "name":    f"{title.strip().replace(' ', '_')}.mp4"
                })

            # обновляем процент
            if not first_chunk and total_bytes and total_bytes != float("inf"):
                pct = int(downloaded / total_bytes * 100)
                if pct > 100:
                    pct = 100
                await r.hset(JOB(job_id), "percent", f"{pct}%")

        await proc.wait()

    # 4) Переименовываем в .mp4 и финальный статус
    final = tmp.with_suffix(".mp4")
    shutil.move(tmp, final)
    await r.hset(JOB(job_id), mapping={
        "status": "ready",
        "file":   str(final),
        "name":   f"{title.strip().replace(' ', '_')}.mp4"
    })

    await r.expire(JOB(job_id), 3600)


async def enqueue_stream(url: str, selector: str, title: str) -> str:
    """
    Ставит задачу на фон и возвращает её job_id.
    """
    jid = uuid.uuid4().hex
    await r.hset(JOB(jid), mapping={"status": "queued"})
    asyncio.create_task(stream_job(jid, url, selector, title))
    return jid
