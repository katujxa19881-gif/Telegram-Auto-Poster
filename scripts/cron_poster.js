// scripts/cron_poster.js — GitHub Actions автопостер с URL-кнопками из CSV
// Требуется Node 20+, package.json с { "type": "module", "dependencies": { "csv-parser": "^3" } }

import fs from "fs";
import csv from "csv-parser";

// ==== ENV ====
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID; // @username или -100...
const OWNER_ID    = process.env.OWNER_ID || "";
const WINDOW_MIN  = Number(process.env.WINDOW_MINUTES || 12); // окно «догонялки», мин

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// ==== ANTI-DUP ====
const SENT_FILE = "sent.json";
let sent = new Set();
try {
  if (fs.existsSync(SENT_FILE)) {
    sent = new Set(JSON.parse(fs.readFileSync(SENT_FILE, "utf8")));
  }
} catch (_) {}

function saveSent() {
  fs.writeFileSync(SENT_FILE, JSON.stringify([...sent], null, 2));
}
function keyOf({date,time,channel_id,text,photo_url,video_url}) {
  const payload = `${date}|${time}|${channel_id}|${text||""}|${photo_url||""}|${video_url||""}`;
  return Buffer.from(payload).toString("base64").slice(0,32);
}
function short(s, n=140){ return String(s||"").replace(/\s+/g," ").slice(0,n); }

// ==== Google Drive → direct URL ====
function extractDriveId(url = "") {
  try {
    const u = new URL(url);
    if (!u.hostname.includes("drive.google.com")) return null;
    const m1 = u.pathname.match(/\/file\/d\/([A-Za-z0-9_-]{10,})/);
    if (m1) return m1[1];
    const id2 = u.searchParams.get("id");
    if (id2) return id2;
    if (u.pathname.startsWith("/uc")) {
      const id3 = u.searchParams.get("id");
      if (id3) return id3;
    }
    return null;
  } catch { return null; }
}
function convertDriveUrl(url=""){
  const id = extractDriveId(url);
  return id ? `https://drive.google.com/uc?export=download&id=${id}` : url;
}

// Нормализация колонок media + алиасы photo/video
function normRow(row){
  if (!row.photo_url && row.photo) row.photo_url = row.photo;
  if (!row.video_url && row.video) row.video_url = row.video;
  if (row.photo_url) row.photo_url = convertDriveUrl(String(row.photo_url).trim());
  if (row.video_url) row.video_url = convertDriveUrl(String(row.video_url).trim());
  return row;
}

// ==== Telegram API ====
async function tg(method, body) {
  const r = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(body)
  });
  const j = await r.json();
  if (!j.ok) throw new Error(j.description || "TG API error");
  return j.result;
}
async function tgSend(chat_id, text, extra={}) { return tg("sendMessage", {chat_id, text, ...extra}); }
async function tgGetMe() { return tg("getMe", {}); }

// ==== Кнопки: кастом из CSV или дефолтные ====
function collectCustomButtons(row) {
  // Ожидаемые пары в CSV: btn1_text, btn1_url, ... btn8_text, btn8_url
  const buttons = [];
  for (let i = 1; i <= 8; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`]  || "").trim();
    if (!t || !u) continue;
    try {
      const url = new URL(u); // валидация URL
      buttons.push({ text: t, url: url.toString() });
    } catch { /* пропускаем невалидные URL */ }
  }
  return buttons;
}
function chunkButtons(btns, perRow = 2) {
  const rows = [];
  for (let i = 0; i < btns.length; i += perRow) rows.push(btns.slice(i, i+perRow));
  return rows;
}

let BOT_USERNAME = "";
async function ensureBotUsername(){
  if (!BOT_USERNAME) {
    const me = await tgGetMe();
    BOT_USERNAME = me.username;
  }
}

function buildKeyboardCustomOrDefault(customButtons, botUsername) {
  if (customButtons.length > 0) {
    return { reply_markup: { inline_keyboard: chunkButtons(customButtons, 2) } };
  }
  const base = `https://t.me/${botUsername}`;
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "🧠 Что умеет?", url: `${base}?start=skills` },
          { text: "💰 Цены",       url: `${base}?start=prices` }
        ],
        [
          { text: "💬 Отзывы",     url: `${base}?start=feedback` },
          { text: "📝 Заказать",   url: `${base}?start=order` }
        ]
      ]
    }
  };
}

async function sendPost({channel, text, photo_url, video_url, keyboardExtra}) {
  if (video_url) {
    return tg("sendVideo", { chat_id: channel, video: video_url, caption: text, ...keyboardExtra });
  } else if (photo_url) {
    return tg("sendPhoto", { chat_id: channel, photo: photo_url, caption: text, ...keyboardExtra });
  } else {
    return tg("sendMessage", { chat_id: channel, text, ...keyboardExtra });
  }
}

// ==== Чтение CSV и публикация ====
const rows = [];
fs.createReadStream("avtopost.csv")
  .pipe(csv())
  .on("data", (r) => rows.push(normRow(r)))
  .on("end", async () => {
    const now = new Date();             // TZ берём из workflow (env TZ=Europe/Kaliningrad)
    const windowMs = WINDOW_MIN * 60 * 1000;
    let done = 0, skipped = 0;

    await ensureBotUsername();

    for (const r of rows) {
      const date = (r.date||"").trim();
      const time = (r.time||"").trim();
      const text = r.text;
      const channel = (r.channel_id||"").trim() || CHANNEL_ID;
      const photo_url = (r.photo_url||"").trim();
      const video_url = (r.video_url||"").trim();

      if (!date || !time || !text) { skipped++; continue; }

      const [Y,M,D] = date.split("-").map(Number);
      const [h,m]   = time.split(":").map(Number);
      const when    = new Date(Y,(M||1)-1,D,h||0,m||0);
      if (isNaN(when)) { skipped++; continue; }

      // публикуем, если запись попала в окно [now - WINDOW_MIN; now]
      if (when <= now && (now - when) <= windowMs) {
        const k = keyOf({date,time,channel_id:channel,text,photo_url,video_url});
        if (sent.has(k)) continue;

        try {
          const customButtons = collectCustomButtons(r);
          const keyboardExtra = buildKeyboardCustomOrDefault(customButtons, BOT_USERNAME);

          await sendPost({ channel, text, photo_url, video_url, keyboardExtra });
          sent.add(k); done++;

          if (OWNER_ID) {
            await tgSend(OWNER_ID,
`✅ GitHub Cron: опубликовано
${date} ${time} → ${channel}
Тип: ${video_url ? "video" : (photo_url ? "photo" : "text")}
Кнопки: ${customButtons.length ? customButtons.length : "дефолтные"}
Текст: ${short(text)}`
            ).catch(()=>{});
          }
        } catch(e) {
          const errText = e?.message || String(e);
          if (OWNER_ID) {
            await tgSend(OWNER_ID,
`❌ GitHub Cron: сбой публикации
${date} ${time} → ${channel}
Фото: ${photo_url || "-"}
Видео: ${video_url || "-"}
Ошибка: ${errText}`
            ).catch(()=>{});
          }
          console.error("Send error:", errText);
        }
      }
    }
    saveSent();
    console.log(`Done: ${done}, skipped: ${skipped}, window: ${WINDOW_MIN}m`);
  })
  .on("error", (e) => {
    console.error("CSV read error:", e);
    process.exit(1);
  });
