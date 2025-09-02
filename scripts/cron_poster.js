// scripts/cron_poster.js  — анти-дубликаты, стабильные id и cool-down
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";
import crypto from "crypto";

// ====== ENV ======
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID;           // -100...
const OWNER_ID    = process.env.OWNER_ID || "";
const WINDOW_MIN  = parseInt(process.env.WINDOW_MIN || process.env.WINDOW_MINUTES || "12", 10);
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10);

// Новые опции защиты от дублей:
const COOL_DOWN_MIN   = parseInt(process.env.COOL_DOWN_MIN   || "180", 10); // не повторять в течение N мин
const MAX_PER_RUN     = parseInt(process.env.MAX_PER_RUN     || "1",   10); // макс. постов за прогон
const SENDER_CHAT_ID  = process.env.SENDER_CHAT_ID || CHANNEL_ID;

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID");
  process.exit(1);
}

// ====== helpers ======
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const normText = (s = "") =>
  String(s)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")   // пробелы перед переводами
    .replace(/\u00A0/g, " ")      // неразрывные пробелы
    .trim();

function convertDriveUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch {}
  return u.trim();
}

function toISOLocal(dateStr, timeStr) {
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m]    = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0);
}

function withinWindow(when, now, windowMin) {
  const diff = (when - now) / 60000;
  return diff >= 0 && diff <= windowMin;
}

// «толстый» CSV-парсер c автодетектом ; или ,
function detectSepFromHeader(src) {
  let inQ = false, commas = 0, semis = 0;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (ch === '"') {
      if (inQ && src[i + 1] === '"') i++;
      else inQ = !inQ;
    } else if (!inQ && ch === "\n") break;
    else if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";") semis++;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(file) {
  let s = fs.readFileSync(file, "utf8");
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
  const rows = records.slice(1).map(r => {
    const o = {};
    headers.forEach((h, i) => (o[h] = (r[i] ?? "").toString()));
    // алиасы
    if (!o.photo_url && o.photo) o.photo_url = o.photo;
    if (!o.video_url && o.video) o.video_url = o.video;

    // нормализация
    o.photo_url = convertDriveUrl(o.photo_url || "");
    o.video_url = convertDriveUrl(o.video_url || "");
    o.text      = normText(o.text || "");
    return o;
  }).filter(o => Object.values(o).some(v => String(v).trim() !== ""));

  return { rows, sep };
}

// стабильный ключ публикации
function postKey(row) {
  const base = {
    date: (row.date || "").trim(),
    time: (row.time || "").trim(),
    channel: CHANNEL_ID,
    sender: SENDER_CHAT_ID,
    media: row.photo_url ? "photo" : (row.video_url ? "video" : "text"),
    media_url: row.photo_url || row.video_url || "",
    // хэш текста (чтобы не тянуть килотексты в ключ)
    text_hash: crypto.createHash("sha256").update(normText(row.text || "")).digest("hex"),
  };
  const raw = JSON.stringify(base);
  return crypto.createHash("sha256").update(raw).digest("hex");
}

// проверка cool-down (если похожие ключи не старше N минут)
function isInsideCooldown(sent, key, now) {
  const rec = sent[key];
  if (!rec || !rec.at) return false;
  const diffMin = (now - new Date(rec.at)) / 60000;
  return diffMin >= 0 && diffMin < COOL_DOWN_MIN;
}

// ====== Telegram API ======
const TG = {
  async call(method, payload) {
    const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
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
      photo, caption, parse_mode: "HTML",
      allow_sending_without_reply: true,
      reply_markup
    });
  },
  async sendVideo(video, caption, reply_markup) {
    return this.call("sendVideo", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      video, caption, parse_mode: "HTML",
      allow_sending_without_reply: true,
      reply_markup
    });
  },
  async notifyOwner(text) {
    if (!OWNER_ID) return;
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch {}
  }
};

// ====== кнопки из CSV (btn1_text/btn1_url...) ======
function buildInlineKeyboard(row) {
  const btns = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`]  || "").trim();
    if (t && u) btns.push([{ text: t, url: u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

// ====== sent.json ======
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(x) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

// ====== MAIN ======
async function main() {
  const csv  = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();
  const now  = new Date();

  let posted = 0, plannedToday = 0, sentToday = 0;

  // считаем сегодняшние для вечернего отчёта
  const todayStr = new Date().toISOString().slice(0, 10);

  for (const row of csv.rows) {
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    if (date === todayStr) plannedToday++;

    if (!date || !time || !row.text) continue;
    const when = toISOLocal(date, time);

    // только внутри окна
    if (!withinWindow(when, now, WINDOW_MIN)) continue;

    // стабильный ключ и cool-down
    const key = postKey(row);
    if (sent[key]) continue;                   // уже отправляли
    if (isInsideCooldown(sent, key, now)) continue;  // слишком скоро

    // ограничение на один прогон
    if (posted >= MAX_PER_RUN) break;

    const kb = buildInlineKeyboard(row);

    try {
      if (row.photo_url) {
        const cap = row.text.length > 1000 ? row.text.slice(0, 1000) + "…" : row.text;
        const m = await TG.sendPhoto(row.photo_url, cap, kb);
        if (row.text.length > 1000) {
          await sleep(400);
          await TG.sendText(row.text.slice(1000));
        }
        sent[key] = { at: new Date().toISOString(), msg_id: m.message_id, date, time };
      } else if (row.video_url) {
        const cap = row.text.length > 1000 ? row.text.slice(0, 1000) + "…" : row.text;
        const m = await TG.sendVideo(row.video_url, cap, kb);
        if (row.text.length > 1000) {
          await sleep(400);
          await TG.sendText(row.text.slice(1000));
        }
        sent[key] = { at: new Date().toISOString(), msg_id: m.message_id, date, time };
      } else {
        const m = await TG.sendText(row.text, kb);
        sent[key] = { at: new Date().toISOString(), msg_id: m.message_id, date, time };
      }
      posted++;
      if (date === todayStr) sentToday++;
      await sleep(600);
    } catch (e) {
      await TG.notifyOwner(`❌ Ошибка публикации ${date} ${time}\n${e?.message || e}`);
    }
  }

  writeSent(sent);

  if (posted > 0) {
    await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно ${WINDOW_MIN} мин, max/run=${MAX_PER_RUN})`);
  }

  // вечерний отчёт
  const nowLocal = new Date();
  const needReport =
    nowLocal.getHours() >= REPORT_HOUR &&
    sent.__report_date !== todayStr;

  if (needReport) {
    await TG.notifyOwner(
      `🗓 Итог за ${todayStr}\n` +
      `Запланировано: ${plannedToday}\n` +
      `Опубликовано: ${Object.values(sent).filter(v => v && v.date === todayStr).length}`
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
