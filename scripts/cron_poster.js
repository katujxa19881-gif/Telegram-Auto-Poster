// scripts/cron_poster.js
// Node.js 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// =================== ENV ===================
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100xxxxxxxxxxx (НЕ @username!)
const OWNER_ID = process.env.OWNER_ID || ""; // для уведомлений в ЛС
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// окно поиска постов (минуты)
const WINDOW_MIN = parseInt(
  process.env.WINDOW_MIN ?? process.env.WINDOW_MINUTES ?? "30",
  10
);

// лимит постов за один прогон (защита от «залпа»)
const X_PER_RUN = parseInt(process.env.X_PER_RUN ?? "1", 10);

// антидубли: не постить чаще, чем раз в N минут
const ANTI_DUP_MIN = parseInt(process.env.ANTI_DUP_MIN ?? "180", 10);

// час ежедневного отчёта (по локальному TZ раннера, задаётся во workflow через TZ)
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR ?? "21", 10);

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// =================== Утилиты ===================
const SENT_FILE = path.resolve("sent.json");

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(obj) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(obj, null, 2));
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
  // ожидаем YYYY-MM-DD и HH:MM
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m] = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0);
}

function withinWindow(when, now, windowMin) {
  const diffMin = (when - now) / 60000;
  return diffMin >= 0 && diffMin <= windowMin;
}

function hashKey(row) {
  // Ключ для sent.json — включим дату/время/медиа и часть текста
  const base = `${row.date || ""} ${row.time || ""} ${(row.photo_url || "")}${(row.video_url || "")} ${String(row.text || "").slice(0, 80)}`;
  return base.trim().replace(/\s+/g, " ");
}

// ============== «Толстый» CSV-парсер ==============
function detectSepFromHeader(src) {
  let inQ = false, commas = 0, semis = 0;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (ch === '"') {
      if (inQ && src[i + 1] === '"') { i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === "\n") break;
    else if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";") semis++;
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
    // алиасы
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

// ============== Кнопки ==============
function buildInlineKeyboard(row) {
  const btns = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`] || "").trim();
    if (t && u) btns.push([{ text: t, url: u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

// ============== Telegram API ==============
const TG = {
  async call(method, payload) {
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(() => ({}));
    if (!j.ok) {
      throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    }
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

// ============== MAIN ==============
async function main() {
  const csv = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();

  // служебное хранилище времени последней публикации (для антидублей)
  sent.__last_post_at = sent.__last_post_at || 0;

  const now = new Date();
  const nowTs = Date.now();

  let due = 0;
  let posted = 0;

  for (const row of csv.rows) {
    if (posted >= X_PER_RUN) break; // лимит на прогон

    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN)) continue;

    const key = hashKey(row);
    if (sent[key]) continue; // уже публиковали

    // антидубли по времени (между постами)
    const minsSinceLast = (nowTs - (sent.__last_post_at || 0)) / 60000;
    if (minsSinceLast < ANTI_DUP_MIN) continue;

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

      sent[key] = true;
      posted++;
      sent.__last_post_at = Date.now();
      writeSent(sent); // фиксируем сразу после успешной публикации
      await sleep(700);
    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // Уведомление «по факту» — только если что-то опубликовано
  if (posted > 0) {
    await TG.notifyOwner(
      `✅ Опубликовано: ${posted} (окно ${WINDOW_MIN} мин, лимит ${X_PER_RUN}, антидубль ${ANTI_DUP_MIN} мин)`
    );
  }

  // ===== Ежедневный отчёт один раз в день =====
  const todayLocal = new Date(); // локальный TZ раннера (см. TZ во workflow)
  const todayStr = todayLocal.toISOString().slice(0, 10); // дата «год-месяц-день»

  const needDailyReport =
    todayLocal.getHours() >= REPORT_HOUR &&
    sent.__report_date !== todayStr;

  if (needDailyReport) {
    let totalToday = 0;
    let sentToday = 0;
    for (const row of csv.rows) {
      const d = (row.date || "").trim();
      if (d === todayStr) {
        totalToday++;
        const k = hashKey(row);
        if (sent[k]) sentToday++;
      }
    }

    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${todayStr}):\n` +
      `Запланировано на сегодня: ${totalToday}\n` +
      `Фактически опубликовано: ${sentToday}`
    );

    sent.__report_date = todayStr;
    writeSent(sent);
  }
}

main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
