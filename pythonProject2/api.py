import asyncio

from fastapi import FastAPI, HTTPException, Form, Response, WebSocket
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse

from pythonProject2.redis_client import r, JOB
from pythonProject2.streamer    import enqueue_stream
import yt_dlp

app = FastAPI()

@app.websocket("/ws/{jid}")
async def ws_status(ws: WebSocket, jid: str):
    await ws.accept()
    while True:
        data = await r.hgetall(JOB(jid))
        await ws.send_json({"percent": data.get("percent"), "status": data.get("status")})
        if data.get("status") == "ready":
            break
        await asyncio.sleep(1)
    await ws.close()

@app.get("/dl/{jid}", response_class=HTMLResponse)
async def dl(jid: str):
    return HTMLResponse(f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Загрузка...</title>
      </head>
      <body>
        <h1>Загрузка…</h1>
        <div>
          <progress id="bar" value="0" max="100"></progress>
          <span id="pct">0%</span>
        </div>
        <script>
          const jid = "{jid}";
          const ws = new WebSocket("ws://" + location.host + "/ws/" + jid);
          ws.onmessage = e => {{
            const j = JSON.parse(e.data);
            document.getElementById("bar").value = parseInt(j.percent) || 0;
            document.getElementById("pct").textContent = j.percent;
            if (j.status === "ready") {{
              ws.close();
              window.location = "/file/" + jid;
            }}
          }};
        </script>
      </body>
    </html>
    """)

@app.get("/status/{jid}")
async def status(jid: str):
    data = await r.hgetall(JOB(jid))
    # вернём хотя бы статус + percent/msg
    return {
        "status":  data.get("status", "none"),
        "percent": data.get("percent", "0%"),
        "msg":     data.get("msg", "")
    }

@app.get("/file/{jid}")
async def file(jid: str):
    job = await r.hgetall(JOB(jid))
    if job.get("status") != "ready":
        raise HTTPException(404, "not ready")
    filepath = job["file"]      # полное path, напр. /tmp/yt_stream/abcd.mp4
    filename = job["name"]      # имя для скачивания

    # Вернём «перенаправление» на внутренний путь /protected/…
    return Response(
        status_code=200,
        headers={
            "X-Accel-Redirect": f"/protected/{jid}.mp4",
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

# тестовая форма: вводите URL и формат (например 137 для 1080p)
@app.get("/", response_class=HTMLResponse)
async def form():
    return """
      <form method="post">
        YouTube URL: <input name="url" size=50><br>
        FormatID:    <input name="fmt" size=5><br>
        <button>Скачать</button>
      </form>
    """

@app.post("/", response_class=HTMLResponse)
async def start(url: str = Form(...), fmt: str = Form(...)):
    info = yt_dlp.YoutubeDL({"skip_download": True, "quiet": True})\
             .extract_info(url, download=False)
    jid  = await enqueue_stream(url, fmt, info["title"])
    return HTMLResponse(f"<a href='/dl/{jid}'>⇨ Скачать {info['title']}</a>")
