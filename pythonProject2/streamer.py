import asyncio
import tempfile
import shutil
import uuid
from pathlib import Path

import yt_dlp
from pythonProject2.redis_client import r, JOB

# Папка для временных и окончательных файлов
DOWNLOAD_ROOT = Path(tempfile.gettempdir()) / "yt_stream"
DOWNLOAD_ROOT.mkdir(exist_ok=True)

# Базовые опции для yt_dlp с внешним загрузчиком aria2c
YDL_OPTS_BASE = {
    "format": "bestaudio[ext=m4a]+bestvideo[ext=mp4]/best[ext=mp4]",
    "merge_output_format": "mp4",
    "buffer_size": "64M",
    "external_downloader": "aria2c",
    "external_downloader_args": ["-x", "16", "-s", "16", "-k", "4M"],
    "quiet": True,
    "no_warnings": True,
}


def progress_hook_factory(job_id: str, total_bytes: int):
    last = {"pct": 0}
    def hook(d):
        if d.get("status") == "downloading":
            downloaded = d.get("downloaded_bytes", 0)
            if total_bytes:
                pct = int(downloaded / total_bytes * 100)
                if pct > 100:
                    pct = 100
                # Обновляем не чаще, чем на 2%
                if pct - last["pct"] >= 2:
                    last["pct"] = pct
                    # Асинхронно пушим в Redis
                    asyncio.get_event_loop().create_task(
                        r.hset(JOB(job_id), "percent", f"{pct}%")
                    )
    return hook


async def stream_job(job_id: str, url: str, selector: str, title: str):
    loop = asyncio.get_event_loop()

    # 1) Получаем метаданные без загрузки
    info = await loop.run_in_executor(
        None,
        lambda: yt_dlp.YoutubeDL({"quiet": True}).extract_info(url, download=False)
    )

    # 2) Считаем общий размер по селектору
    parts = selector.split("+")
    dur = info.get("duration", 0) or 0
    total_bytes = 0
    if len(parts) > 1:
        video_sel, audio_sel = parts
        video_fmt = next((f for f in info["formats"] if f["format_id"] == video_sel), None)
        audio_fmt = None
        if audio_sel == "bestaudio":
            audio_fmt = max(
                (f for f in info["formats"] if f.get("vcodec") == "none"),
                key=lambda f: f.get("abr", 0), default=None
            )
        else:
            audio_fmt = next((f for f in info["formats"] if f["format_id"] == audio_sel), None)
        if not video_fmt or not audio_fmt:
            msg = []
            if not video_fmt:
                msg.append(f"Видео-поток {video_sel} не найден")
            if not audio_fmt:
                msg.append(f"Аудио-поток {audio_sel} не найден")
            await r.hset(JOB(job_id), mapping={"status": "error", "msg": ", ".join(msg)})
            return
        v_size = video_fmt.get("filesize") or video_fmt.get("filesize_approx") or int((video_fmt.get("tbr", 0) * 125) * dur)
        a_size = audio_fmt.get("filesize") or audio_fmt.get("filesize_approx") or int((audio_fmt.get("abr", 0) * 125) * dur)
        total_bytes = v_size + a_size
    else:
        fmt = next((f for f in info["formats"] if f["format_id"] == selector), None)
        if not fmt:
            await r.hset(JOB(job_id), mapping={"status": "error", "msg": f"Format {selector} not found"})
            return
        total_bytes = fmt.get("filesize") or fmt.get("filesize_approx") or int((fmt.get("tbr", 0) * 125) * dur)

    # 3) Готовим временный файл и инициализируем статус
    tmp = DOWNLOAD_ROOT / f"{job_id}.partial"
    final_fn = DOWNLOAD_ROOT / f"{job_id}.mp4"
    await r.hset(JOB(job_id), mapping={
        "status": "downloading",
        "percent": "0%",
        "file": str(tmp),
        "name": f"{title.strip().replace(' ', '_')}.mp4"
    })

    # Опции для загрузки
    ydl_opts = {
        **YDL_OPTS_BASE,
        "outtmpl": str(tmp),
        "progress_hooks": [progress_hook_factory(job_id, total_bytes)]
    }

    # 4) Запускаем yt-dlp в пуле потоков (aria2c)
    await loop.run_in_executor(
        None,
        lambda: yt_dlp.YoutubeDL(ydl_opts).download([url])
    )

    # 5) Переименовываем .partial → .mp4 и отмечаем готовность
    shutil.move(str(tmp), str(final_fn))
    await r.hset(JOB(job_id), mapping={
        "status": "ready",
        "file": str(final_fn),
        "name": f"{title.strip().replace(' ', '_')}.mp4"
    })
    await r.expire(JOB(job_id), 3600)


async def enqueue_stream(url: str, selector: str, title: str) -> str:
    jid = uuid.uuid4().hex
    await r.hset(JOB(jid), mapping={"status": "queued"})
    asyncio.create_task(stream_job(jid, url, selector, title))
    return jid
