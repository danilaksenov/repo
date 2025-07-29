from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse

from pythonProject2.redis_client import r, JOB
from pythonProject2.streamer    import enqueue_stream
import yt_dlp

app = FastAPI()

@app.get("/dl/{jid}", response_class=HTMLResponse)
async def dl(jid: str):
    # просто рисуем прогресс-блок и JS‑polling
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
      function poll() {{
        fetch("/status/" + jid)
          .then(r => r.json())
          .then(j => {{
            if (j.status === "downloading") {{
              document.getElementById("bar").value = parseInt(j.percent) || 0;
              document.getElementById("pct").textContent = j.percent;
              setTimeout(poll, 500);
            }} else if (j.status === "ready") {{
              // когда готово — переходим на скачивание
              window.location = "/file/" + jid;
            }} else if (j.status === "error") {{
              document.body.innerHTML = "❌ Ошибка: " + j.msg;
            }} else {{
              setTimeout(poll, 500);
            }}
          }});
      }}
      poll();
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
    return FileResponse(job["file"], filename=job["name"])

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
