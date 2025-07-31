import asyncio
import tempfile
import uuid
import re
import time
import threading
from pathlib import Path

import yt_dlp
from pythonProject2.redis_client import r, JOB

# Папка для временных и окончательных файлов
DOWNLOAD_ROOT = Path(tempfile.gettempdir()) / "yt_stream"
DOWNLOAD_ROOT.mkdir(exist_ok=True)

# Опции внешнего загрузчика (aria2c)
YDL_OPTS = {
    "buffer_size": "32M",
    "external_downloader": "aria2c",
    "external_downloader_args": ["-x", "16", "-s", "16", "-k", "2M"],
    "quiet": True,
    "no_warnings": True,
}

# Путь к лог-файлу nginx для X-Accel-Redirect и внешних /file/ запросов
PROTECTED_LOG = "/var/log/nginx/protected-access.log"

async def _delayed_delete(path: Path, delay: float = 3600.0):
    """Удаляет файл через заданную задержку, если он ещё существует."""
    await asyncio.sleep(delay)
    try:
        if path.exists():
            path.unlink()
            print(f"[delayed_delete] removed {path}")
    except Exception as e:
        print(f"[delayed_delete] error deleting {path}: {e}")

# Функция watcher для удаления сразу после скачивания клиентом
def watch_and_delete(job_id: str, out_path: Path):
    patterns = [f"/protected/{job_id}.mp4", f"GET /file/{job_id}"]
    # Ждём лог-файл
    while not Path(PROTECTED_LOG).exists():
        time.sleep(0.5)
    # Ждём создания файла
    while not out_path.exists():
        time.sleep(0.2)
    print(f"[watcher] started for {job_id}")
    with open(PROTECTED_LOG, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            # Отладка
            print(f"[watcher] read: {line.strip()}")
            # Проверяем оба паттерна
            if any(pat in line for pat in patterns):
                print(f"[watcher] matched, deleting {out_path}")
                try:
                    out_path.unlink()
                    print(f"[watcher] deleted {out_path}")
                except Exception as e:
                    print(f"[watcher] delete error: {e}")
                break

# Хук для прогресса загрузки
def progress_hook_factory(job_id: str, total_bytes: int, loop: asyncio.AbstractEventLoop):
    last_pct = 0
    def hook(d):
        nonlocal last_pct
        if d.get("status") == "downloading":
            downloaded = d.get("downloaded_bytes", 0)
            pct = int(downloaded / total_bytes * 100) if total_bytes else 0
            pct = min(pct, 100)
            if pct - last_pct >= 2:
                last_pct = pct
                loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(
                        r.hset(JOB(job_id), "percent", f"{pct}%")
                    )
                )
    return hook

async def stream_job(job_id: str, url: str, selector: str, title: str):
    loop = asyncio.get_running_loop()

    # 1) Метаданные без загрузки
    info = await loop.run_in_executor(
        None,
        lambda: yt_dlp.YoutubeDL({"quiet": True}).extract_info(url, download=False)
    )

    # 2) Расчёт ожидаемого объёма
    duration = info.get("duration") or 0
    parts = selector.split("+")
    if len(parts) == 2:
        v_fmt = next((f for f in info["formats"] if f["format_id"] == parts[0]), None)
        a_fmt = (max((f for f in info["formats"] if f.get("vcodec") == "none"), key=lambda f: f.get("abr", 0))
                 if parts[1] == "bestaudio" else
                 next((f for f in info["formats"] if f["format_id"] == parts[1]), None))
        total_bytes = ((v_fmt.get("filesize") or v_fmt.get("filesize_approx") or int((v_fmt.get("tbr", 0) * 125) * duration)) +
                       (a_fmt.get("filesize") or a_fmt.get("filesize_approx") or int((a_fmt.get("abr", 0) * 125) * duration)))
    else:
        fmt = next((f for f in info["formats"] if f["format_id"] == selector), None)
        total_bytes = fmt.get("filesize") or fmt.get("filesize_approx") or int((fmt.get("tbr", 0) * 125) * duration)

    # 3) Подготовка пути и статус
    out_file = DOWNLOAD_ROOT / f"{job_id}.mp4"
    await r.hset(JOB(job_id), mapping={
        "status": "downloading",
        "percent": "0%",
        "file": str(out_file),
        "name": f"{title.strip().replace(' ', '_')}.mp4"
    })

    # 4) Запуск загрузки
    opts = {
        **YDL_OPTS,
        "format": selector,
        "merge_output_format": "mp4",
        "outtmpl": str(out_file),
        "progress_hooks": [progress_hook_factory(job_id, total_bytes or 1, loop)]
    }
    await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(opts).download([url]))

    # 5) Финал и удаление
    await r.hset(JOB(job_id), mapping={"status": "ready", "percent": "100%"})
    await r.expire(JOB(job_id), 3600)
    # Запустить watcher и отложенное удаление
    threading.Thread(target=watch_and_delete, args=(job_id, out_file), daemon=True).start()
    asyncio.create_task(_delayed_delete(out_file, delay=3600.0))

async def enqueue_stream(url: str, selector: str, title: str) -> str:
    jid = uuid.uuid4().hex
    await r.hset(JOB(jid), mapping={"status": "queued"})
    asyncio.create_task(stream_job(jid, url, selector, title))
    return jid