// scripts/cron_poster.js — Zero-deps автопостер для GitHub Actions
// Фичи: CSV без зависимостей (кавычки, ,/;), normalizeTime(), антидубли,
// Google Drive → прямые ссылки, внешние URL-кнопки всегда,
// + опционально добавляем «🤖 Открыть чат», если бот на Replit жив.

import fs from "fs";
import https from "https";

// ===== ENV =====
const BOT_TOKEN       = process.env.BOT_TOKEN;
const CHANNEL_ID      = process.env.CHANNEL_ID;
const OWNER_ID        = process.env.OWNER_ID || "";
const TZ              = process.env.TZ || "Europe/Kaliningrad";
const WINDOW_MINUTES  = parseInt(process.env.WINDOW_MINUTES || "20", 10);
const CSV_PATH        = "avtopost.csv";

// Пинги и внешние ссылки для фолбэка
const KEEPALIVE_URL   = process.env.KEEPALIVE_URL || "";   // https://...replit.dev/ron?token=...
const LINK_SKILLS     = process.env.LINK_SKILLS   || "";   // публичная страница
const LINK_PRICES     = process.env.LINK_PRICES   || "";   // публичная страница
const LINK_FEEDBACK   = process.env.LINK_FEEDBACK || "";   // публичная страница
const LINK_ORDER      = process.env.LINK_ORDER    || "https://t.me/Ka_terina8"; // fallback-CTA

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID");
  process.exit(1);
}

// ====== Telegram minimal API (без зависимостей) ======
function tgRequest(path, payload) {
  const data = payload ? JSON.stringify(payload) : null;
  const opts = {
    hostname: "api.telegram.org",
    path,
    method: data ? "POST" : "GET",
    headers: data
      ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) }
      : {},
  };
  return new Promise((resolve) => {
    const req = https.request(opts, (res) => {
      let buf = "";
      res.on("data", (c) => (buf += c));
      res.on("end", () => {
        try { resolve(JSON.parse(buf || "{}")); } catch { resolve({ ok:false, description:"Bad JSON" }); }
      });
    });
    req.on("error", () => resolve({ ok:false, description:"Network error" }));
    if (data) req.write(data);
    req.end();
  });
}
async function tgSendMessage(chat_id, text, extra = {}) { await tgRequest(`/bot${BOT_TOKEN}/sendMessage`, { chat_id, text, ...extra }); }
async function tgSendPhoto(chat_id, photo, caption, extra = {}) { await tgRequest(`/bot${BOT_TOKEN}/sendPhoto`, { chat_id, photo, caption, ...extra }); }
async function tgSendVideo(chat_id, video, caption, extra = {}) { await tgRequest(`/bot${BOT_TOKEN}/sendVideo`, { chat_id, video, caption, ...extra }); }
async function tgGetMe() { const r = await tgRequest(`/bot${BOT_TOKEN}/getMe`); return (r?.ok && r?.result?.username) ? r.result.username : ""; }

// ====== Utils ======
function short(s, n=140){ return String(s||"").replace(/\s+/g," ").slice(0,n); }
function normalizeTime(t){
  if (!t) return "00:00";
  let [h="0", m="0"] = String(t).split(":");
  h = /^\d+$/.test(h) ? h.padStart(2,"0") : "00";
  m = /^\d+$/.test(m) ? m.padStart(2,"0") : "00";
  return `${h}:${m}`;
}

// Быстрая проверка, жив ли Replit-бот (таймаут 3с)
function checkBotLive(url, timeoutMs=3000){
  if (!url) return Promise.resolve(false);
  return new Promise((resolve)=>{
    const req = https.get(url, (res)=>{
      let buf=""; res.on("data",(c)=>buf+=c);
      res.on("end",()=>resolve(buf.trim().toLowerCase()==="ok"));
    });
    req.on("error",()=>resolve(false));
    req.setTimeout(timeoutMs, ()=>{ req.destroy(); resolve(false); });
  });
}

// Google Drive → прямой URL
function extractDriveId(url=""){
  try{
    const u = new URL(url);
    if (!u.hostname.includes("drive.google.com")) return null;
    const m1 = u.pathname.match(/\/file\/d\/([A-Za-z0-9_-]{10,})/); if (m1) return m1[1];
    const id2 = u.searchParams.get("id"); if (id2) return id2;
    if (u.pathname.startsWith("/uc")) return u.searchParams.get("id");
    return null;
  }catch{ return null; }
}
function convertDriveUrl(url=""){ const id=extractDriveId(url); return id ? `https://drive.google.com/uc?export=download&id=${id}` : url; }

// ====== CSV (кавычки + autodetect ,/;) ======
function detectSep(line){ const c=(line.match(/,/g)||[]).length, s=(line.match(/;/g)||[]).length; return s>c?";":","; }
function splitWithQuotes(line, sep){
  const out=[]; let cur=""; let inQ=false;
  for (let i=0;i<line.length;i++){
    const ch=line[i];
    if (ch === '"'){ if (inQ && line[i+1] === '"'){ cur+='"'; i++; } else inQ=!inQ; }
    else if (ch === sep && !inQ){ out.push(cur); cur=""; }
    else cur+=ch;
  }
  out.push(cur); return out;
}
function parseCSV(filePath){
  const raw = fs.readFileSync(filePath,"utf8").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  const lines = raw.split("\n").filter(l=>l.length>0);
  if (lines.length===0) return { rows:[], sep:"," };
  const sep = detectSep(lines[0]);
  const headers = splitWithQuotes(lines[0], sep).map(h=>h.trim());
  const rows=[];
  for (let i=1;i<lines.length;i++){
    const arr = splitWithQuotes(lines[i], sep);
    if (arr.every(c=>c.trim()==="")) continue;
    const r={}; headers.forEach((h,idx)=>r[h]=(arr[idx]??"").trim());
    if (!r.photo_url && r.photo) r.photo_url=r.photo;
    if (!r.video_url && r.video) r.video_url=r.video;
    if (r.photo_url) r.photo_url=convertDriveUrl(r.photo_url);
    if (r.video_url) r.video_url=convertDriveUrl(r.video_url);
    rows.push(r);
  }
  return { rows, sep };
}

// ====== Кнопки ======
function customButtonsFromRow(r){
  const res=[]; for(let i=1;i<=8;i++){
    const t=(r[`btn${i}_text`]||"").trim(), u=(r[`btn${i}_url`]||"").trim();
    if(!t||!u) continue;
    try{ new URL(u); res.push({text:t, url:u}); }catch{}
  }
  return res;
}
function packRows(btns, perRow=2){ const rows=[]; for(let i=0;i<btns.length;i+=perRow) rows.push(btns.slice(i,i+perRow)); return rows; }

// Всегда строим внешние URL-кнопки (стабильно работает без бота)
function buildFallbackKeyboardAlways(){
  const ext=[];
  if (LINK_SKILLS)   ext.push({text:"🧠 Что умеет?", url: LINK_SKILLS});
  if (LINK_PRICES)   ext.push({text:"💰 Цены",       url: LINK_PRICES});
  if (LINK_FEEDBACK) ext.push({text:"💬 Отзывы",     url: LINK_FEEDBACK});
  const orderBtn = {text:"📝 Заказать", url: LINK_ORDER};
  const rows=[], base=[...ext, orderBtn];
  for (let i=0;i<base.length;i+=2) rows.push(base.slice(i,i+2));
  return rows;
}

// Итоговая клавиатура: приоритет — кнопки из CSV; иначе внешние + (если бот жив) отдельной строкой deeplink
async function buildKeyboard(r, botUsername, botLive){
  const custom = customButtonsFromRow(r);
  if (custom.length) return { reply_markup:{ inline_keyboard: packRows(custom,2) } };

  const rows = buildFallbackKeyboardAlways();
  if (botLive && botUsername){
    const base = `https://t.me/${botUsername}`;
    rows.push([{ text:"🤖 Открыть чат", url:`${base}?start=hello` }]);
  }
  return { reply_markup:{ inline_keyboard: rows } };
}

// ====== Anti-dup ======
const SENT_FILE = "sent.json";
let sentSet = new Set();
try { if (fs.existsSync(SENT_FILE)) sentSet = new Set(JSON.parse(fs.readFileSync(SENT_FILE,"utf8"))); } catch {}
function saveSent(){ fs.writeFileSync(SENT_FILE, JSON.stringify([...sentSet], null, 2)); }
function sentKey({date,time,channel,text,photo_url,video_url}){
  const payload = `${date}|${time}|${channel}|${text||""}|${photo_url||""}|${video_url||""}`;
  return Buffer.from(payload).toString("base64").slice(0,32);
}

// ====== MAIN ======
(async () => {
  try{
    const { rows, sep } = parseCSV(CSV_PATH);
    console.log(`CSV: ${CSV_PATH}, sep="${sep}", rows=${rows.length}`);
    if (rows.length===0){ if (OWNER_ID) await tgSendMessage(OWNER_ID,"⚠️ CSV пуст — нет строк."); return; }

    const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
    const windowStart = new Date(now.getTime() - WINDOW_MINUTES*60000);
    const todayStr = now.toISOString().slice(0,10);

    // проверяем Replit-бота
    const botLive = await checkBotLive(KEEPALIVE_URL);
    const botUsername = botLive ? (await tgGetMe()) : "";

    let dueToday=0, sentCount=0;

    for (const r of rows){
      const date=(r.date||"").trim();
      const time=normalizeTime(r.time||"");
      const text=r.text||"";
      const channel=(r.channel_id||"").trim() || CHANNEL_ID;
      const photo_url=(r.photo_url||"").trim();
      const video_url=(r.video_url||"").trim();
      if (!date || !time || !text) continue;

      const dt = new Date(`${date}T${time}:00`);
      if (isNaN(dt)) continue;

      if (dt.toISOString().slice(0,10) === todayStr){
        dueToday++;
        if (dt>=windowStart && dt<=now){
          const key = sentKey({date,time,channel,text,photo_url,video_url});
          if (sentSet.has(key)) continue;

          const keyboard = await buildKeyboard(r, botUsername, botLive);
          if (video_url) await tgSendVideo(channel, video_url, text, keyboard);
          else if (photo_url) await tgSendPhoto(channel, photo_url, text, keyboard);
          else await tgSendMessage(channel, text, keyboard);

          sentSet.add(key); sentCount++;

          if (OWNER_ID){
            await tgSendMessage(
              OWNER_ID,
              `✅ Опубликовано: ${date} ${time}\n→ ${channel}\nТип: ${video_url?"video":(photo_url?"photo":"text")}\nКнопки: ${
                customButtonsFromRow(r).length ? "custom" : (botLive ? "fallback+deeplink" : "fallback")
              }\nТекст: ${short(text)}`
            ).catch(()=>{});
          }
        }
      }
    }

    saveSent();

    if (dueToday>0 && sentCount===0 && OWNER_ID){
      await tgSendMessage(OWNER_ID, `⚠️ GitHub Cron: постов в окне ${WINDOW_MINUTES} мин не найдено.
(сегодня «должны быть»: ${dueToday}, фактически отправлено: 0)`);
    }

    console.log(`Done: dueToday=${dueToday}, sent=${sentCount}, botLive=${botLive}, window=${WINDOW_MINUTES}m`);
  }catch(e){
    console.error(e);
    if (OWNER_ID) await tgSendMessage(OWNER_ID, `❌ Fatal: ${e?.message||e}`);
    process.exit(1);
  }
})();
