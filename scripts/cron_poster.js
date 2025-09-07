// scripts/cron_poster.js
// Node 18+ (fetch встроен). ES-модуль.

import fs from "fs";
import path from "path";

// -------------------- ENV --------------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100XXXXXXXXXX (ID канала)
const OWNER_ID = process.env.OWNER_ID || ""; // для уведомлений в ЛС (опционально)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// «окно» и защитные параметры
const WINDOW_MIN = toInt(process.env.WINDOW_MIN, 30); // сколько минут после времени поста мы ждём
const LAG_MIN = toInt(process.env.LAG_MIN, 10); // сколько минут ДО времени поста можно начать (чтобы не опоздать)
const MISS_GRACE_MIN = toInt(process.env.MISS_GRACE_MIN, 15);// «попали чуть позже» — ещё можно
const ANTI_DUP_MIN = toInt(process.env.ANTI_DUP_MIN, 180); // антидубль: мин. разрыв между публикациями
const X_PER_RUN = toInt(process.env.X_PER_RUN, 1); // «не больше N постов за прогон»

// вечерний отчёт
const REPORT_HOUR = toInt(process.env.REPORT_HOUR, 21); // локальный час раннера

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// -----------------------------------------------------------
// Утилиты
// -----------------------------------------------------------
function toInt(v, d) {
  const n = parseInt(v ?? "", 10);
  return Number.isFinite(n) ? n : d;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// безопасный перевод Google Drive ссылок в прямые
function convertDriveUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch {} // no-op
  return u.trim();
}

// локальная дата из «YYYY-MM-DD» + «HH:MM»
function toISOLocal(dateStr, timeStr) {
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m] = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0, 0, 0);
}

// в «окне»? ([-LAG; +WINDOW] от now относительно when)
function withinWindow(when, now, windowMin, lagMin) {
  const diffMin = (when.getTime() - now.getTime()) / 60000; // when - now
  return diffMin <= windowMin && diffMin >= -lagMin;
}

// очень простой хэш текста для ключа
function hash(s) {
  s = String(s || "");
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = (h * 33) ^ s.charCodeAt(i);
  return (h >>> 0).toString(36);
}

// -----------------------------------------------------------
// «Толстый» CSV-парсер (автоопределение ; или , , поддержка "" и многострочных полей)
// -----------------------------------------------------------
function detectSepFromHeader(src) {
  let inQ = false, commas = 0, semis = 0;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (ch === '"') {
      if (inQ && src[i + 1] === '"') { i++; } else { inQ = !inQ; }
      continue;
    }
    if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";") semis++;
    else if (!inQ && ch === "\n") break;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(filePath) {
  if (!fs.existsSync(filePath)) return { rows: [], sep: "," };

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

    // алиасы + правка ссылок
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;
    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);

    // \n в тексте
    if (obj.text) obj.text = obj.text.replace(/\\n/g, "\n");

    const meaningful = Object.values(obj).some(v => String(v).trim() !== "");
    if (meaningful) rows.push(obj);
  }

  return { rows, sep };
}

// -----------------------------------------------------------
// Кнопки (btn1_text, btn1_url … btn4_…)
// -----------------------------------------------------------
function buildInlineKeyboard(row) {
  const btns = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`] || "").trim();
    if (t && u) btns.push([{ text: t, url: u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

// -----------------------------------------------------------
// Telegram API
// -----------------------------------------------------------
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
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch {}
  }
};

// -----------------------------------------------------------
/* sent.json helpers */
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(x) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

// ключ для конкретной строки csv — стабильный (защита от повтора)
function makeKey(row) {
  const text = (row.text || "").trim();
  const short = text.length > 120 ? text.slice(0, 120) : text;
  return [
    (row.date || "").trim(),
    (row.time || "").trim(),
    hash(short),
    hash((row.photo_url || "") + "|" + (row.video_url || ""))
  ].join("|");
}

// -----------------------------------------------------------
// MAIN
// -----------------------------------------------------------
async function main() {
  const csv = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();
  const now = new Date();

  // 0) Антидубль по времени между публикациями
  const lastTs = sent.__lastTs ? new Date(sent.__lastTs) : null;
  if (lastTs) {
    const mins = Math.floor((now.getTime() - lastTs.getTime()) / 60000);
    if (mins < ANTI_DUP_MIN) {
      await TG.notifyOwner(
        `⏳ Антидубль: с последней публикации прошло ${mins} мин (< ${ANTI_DUP_MIN}). Пропускаю прогон.`
      );
      return;
    }
  }

  // 1) Собираем кандидатов в «окне»
  const candidates = [];
  for (const row of csv.rows) {
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    const inMainWindow = withinWindow(when, now, WINDOW_MIN, LAG_MIN);
    const slightlyLate = !inMainWindow &&
                          Math.abs((now - when) / 60000) <= MISS_GRACE_MIN;

    if (!(inMainWindow || slightlyLate)) continue;

    const key = makeKey(row);
    if (sent[key]) continue;

    candidates.push({ row, when, key });
  }

  // 2) Отсортируем по времени (на всякий случай)
  candidates.sort((a, b) => a.when - b.when);

  // 3) Публикуем не более X_PER_RUN
  let posted = 0;
  for (const { row, key } of candidates) {
    if (posted >= X_PER_RUN) break;

    const kb = buildInlineKeyboard(row);
    const text = (row.text || "").trim();

    try {
      if (row.photo_url) {
        const cap = text.length > 1000 ? text.slice(0, 1000) + "…" : text;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url) {
        const cap = text.length > 1000 ? text.slice(0, 1000) + "…" : text;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      // отметим
      sent[key] = true;
      sent.__lastTs = new Date().toISOString();
      writeSent(sent);

      posted++;
      await TG.notifyOwner(`✅ Опубликовано: 1 (окно +${WINDOW_MIN} / -${LAG_MIN} мин; авто-доп. ${MISS_GRACE_MIN} мин)`);
      await sleep(600); // чуть «остынем» между постами
    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации:\n${err?.message || err}`);
    }
  }

  // 4) Вечерний отчёт — один раз в сутки
  const nowLocal = new Date();
  const todayStr = nowLocal.toISOString().slice(0, 10); // только дата
  if (nowLocal.getHours() >= REPORT_HOUR && sent.__report_date !== todayStr) {
    let plan = 0, fact = 0;
    for (const row of csv.rows) {
      if ((row.date || "").trim() === todayStr) {
        plan++;
        if (sent[makeKey(row)]) fact++;
      }
    }
    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${todayStr}):\nЗапланировано на сегодня: ${plan}\nФактически опубликовано: ${fact}`
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
