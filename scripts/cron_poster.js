// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// =================== ENV ===================
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID;      // -100XXXXXXXXXX (НЕ @username)
const OWNER_ID    = process.env.OWNER_ID || "";  // отчеты в ЛС (опц.)
const WINDOW_MIN  = parseInt(process.env.WINDOW_MIN || "12", 10);  // окно, мин
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10);  // лимит за запуск
const COOL_DOWN   = parseInt(process.env.COOL_DOWN_MIN || "180", 10); // антидубль, мин
const KEEPALIVE   = (process.env.KEEPALIVE_URL || "").trim();

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// Публикуем ОТ ИМЕНИ КАНАЛА (можно переопределить секретом SENDER_CHAT_ID)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// =================== Утилиты ===================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function pingKeepalive() {
  if (!KEEPALIVE) return;
  try { await fetch(KEEPALIVE, { method: "GET" }); } catch (_) {}
}

function convertDriveUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch (_) {}
  return u.trim();
}

function toISOLocal(dateStr, timeStr) {
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m]    = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0); // local TZ
}

function withinWindow(dt, now, windowMin) {
  const diffMin = (dt.getTime() - now.getTime()) / 60000;
  return diffMin >= 0 && diffMin <= windowMin;
}

// =================== «Толстый» CSV-парсер ===================
function detectSepFromHeader(src) {
  let inQ = false, commas = 0, semis = 0;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (ch === '"') {
      if (inQ && src[i + 1] === '"') { i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === "\n") break;
    else if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";")   semis++;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(filePath) {
  let s = fs.readFileSync(filePath, "utf8");
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!s.trim()) return { rows: [], sep: "," };

  const sep = detectSepFromHeader(s);

  const records = [];
  let row = [], field = "", inQ = false;

  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') {
      if (inQ && s[i + 1] === '"') { field += '"'; i++; }
      else inQ = !inQ;
      continue;
    }
    if (!inQ && ch === sep) { row.push(field); field = ""; continue; }
    if (!inQ && ch === "\n") {
      row.push(field); field = "";
      if (row.some(c => String(c).trim() !== "")) records.push(row);
      row = [];
      continue;
    }
    field += ch;
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    if (row.some(c => String(c).trim() !== "")) records.push(row);
  }
  if (!records.length) return { rows: [], sep };

  const headers = records[0].map(h => String(h || "").trim());
  const data = records.slice(1);

  const rows = [];
  for (const rec of data) {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = (rec[i] ?? "").toString();
    }
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;

    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);

    if (obj.text) obj.text = obj.text.replace(/\\n/g, "\n");

    const meaningful = Object.values(obj).some(v => String(v).trim() !== "");
    if (meaningful) rows.push(obj);
  }
  return { rows, sep };
}

// =================== Кнопки ===================
function buildInlineKeyboard(row) {
  const btns = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`]  || "").trim();
    if (t && u) btns.push([{ text: t, url: u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

// =================== Telegram API ===================
const TG = {
  async call(method, payload) {
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(() => ({}));
    if (!j.ok) throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    return j.result;
  },

  async sendText(text, reply_markup) {
    return this.call("sendMessage", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      text,
      parse_mode: "HTML",
      disable_web_page_preview: false,
      allow_sending_without_reply: true,
      reply_markup
    });
  },

  async sendPhoto(photo, caption, reply_markup) {
    return this.call("sendPhoto", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      photo,
      caption,
      parse_mode: "HTML",
      allow_sending_without_reply: true,
      reply_markup
    });
  },

  async sendVideo(video, caption, reply_markup) {
    return this.call("sendVideo", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      video,
      caption,
      parse_mode: "HTML",
      allow_sending_without_reply: true,
      reply_markup
    });
  },

  async notifyOwner(text) {
    if (!OWNER_ID) return;
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch (_) {}
  }
};

// =================== Sent-лог ===================
// теперь сохраняем ВРЕМЯ публикации, чтобы учитывать анти-дубль по COOL_DOWN_MIN
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(x) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

// Хэш-подпись поста: дата+время+медиа+сжатый текст (без пробелов)
function makeKey(row) {
  const textNorm = (row.text || "").replace(/\s+/g, " ").trim().slice(0, 200);
  return `${(row.date||"")} ${(row.time||"")} ${(row.photo_url||"")}${(row.video_url||"")} ${textNorm}`;
}
function isCooledDown(sent, key, now) {
  const ts = sent[key]; // millis
  if (!ts) return true;
  const diffMin = (now - ts) / 60000;
  return diffMin >= COOL_DOWN;
}

// =================== MAIN ===================
async function main() {
  await pingKeepalive();

  const csv  = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();
  const now  = new Date();

  let due = 0, posted = 0;

  for (const row of csv.rows) {
    if (posted >= MAX_PER_RUN) break; // лимит за прогон

    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN)) continue;

    const key = makeKey(row);

    // анти-дубль: если недавно публиковали такой же пост — пропускаем
    if (!isCooledDown(sent, key, now)) continue;

    due++;

    const kb = buildInlineKeyboard(row);
    try {
      if (row.photo_url) {
        const cap = text.length > 1000 ? text.slice(0, 1000) + "…" : text;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000) {
          await sleep(500);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url) {
        const cap = text.length > 1000 ? text.slice(0, 1000) + "…" : text;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000) {
          await sleep(500);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      sent[key] = Date.now(); // сохраняем время публикации
      posted++;
      await sleep(700);
    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // ---- УВЕДОМЛЕНИЯ: только по факту и вечерний отчёт ----
  writeSent(sent);

  if (posted > 0) {
    await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно ${WINDOW_MIN} мин, лимит ${MAX_PER_RUN}, антидубль ${COOL_DOWN} мин)`);
  }

 // 2) Вечерний отчёт — строго в REPORT_HOUR:00 (минуту в минуту)
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10);
const nowLocal = new Date(); // TZ берём из workflow (env TZ)
const isReportTime =
  nowLocal.getHours() === REPORT_HOUR &&
  nowLocal.getMinutes() === 0; // только 21:00

const todayStr = nowLocal.toISOString().slice(0, 10); // дата для флага

const needDailyReport = isReportTime && (sent.__report_date !== todayStr);

if (needDailyReport) {
  let totalToday = 0, sentToday = 0;
  for (const row of csv.rows) {
    if ((row.date || "").trim() === todayStr) {
      totalToday++;
      const key = `${row.date} ${row.time} ${(row.photo_url||"")}${(row.video_url||"")}`;
      if (sent[key]) sentToday++;
    }
  }

  await TG.notifyOwner(
    `🗓 Ежедневный отчёт (${todayStr}):\n` +
    `Запланировано на сегодня: ${totalToday}\n` +
    `Фактически опубликовано: ${sentToday}`
  );

  sent.__report_date = todayStr;
  writeSent(sent); // сохраним флаг
}
}
  
main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
