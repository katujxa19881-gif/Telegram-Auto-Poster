// scripts/cron_poster.js
import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync"; // ✅ правильный импорт
import fetch from "node-fetch";

// === Чтение CSV ===
const csvPath = path.join(process.cwd(), "avtopost.csv");
if (!fs.existsSync(csvPath)) {
  console.error("Файл avtopost.csv не найден!");
  process.exit(1);
}
const csvData = fs.readFileSync(csvPath, "utf-8");

const rows = parse(csvData, {
  columns: true,
  skip_empty_lines: true,
});

// === env ===
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100XXXXXXXXXX (НЕ @username!)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID; // публикуем от имени канала
const OWNER_ID = process.env.OWNER_ID || ""; // ваш user_id для уведомлений

const WINDOW_MIN = nenv("WINDOW_MIN", 30); // окно поиска: в пределах +N минут
const LAG_MIN = nenv("LAG_MIN", 10); // можно «чуть раньше» (минус N)
const MISS_GRACE_MIN = nenv("MISS_GRACE_MIN", 15); // авто-допубликация, если прозевали
const ANTI_DUP_MIN = nenv("ANTI_DUP_MIN", 180); // защита от дублей: минимум между постами (мин)
const MAX_PER_RUN = nenv("MAX_PER_RUN", 1); // максимум постов за один прогон
const REPORT_HOUR = nenv("REPORT_HOUR", 21); // вечерний отчёт после этого часа

// Кнопка «Открыть бота» под постом в КАНАЛЕ
const LINK_ORDER = (process.env.LINK_ORDER || "").trim(); // например https://t.me/YourBot?start=from_channel

function nenv(name, def) {
  const v = process.env[name];
  const n = parseInt(v ?? "", 10);
  return Number.isFinite(n) ? n : def;
}

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

// === sent.json для антидублей ===
const sentFile = path.join(process.cwd(), "sent.json");
let sent = {};
if (fs.existsSync(sentFile)) {
  sent = JSON.parse(fs.readFileSync(sentFile, "utf-8"));
}

// === Помощники ===
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function convertDriveUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      // /file/d/<id>/view → https://drive.google.com/uc?export=download&id=<id>
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch (_) {}
  return u.trim();
}

function normalizeText(s) {
  if (!s) return "";
  // 1) экранированные переносы из CSV: "\n" → реальный перенос
  let t = s.replace(/\\n/g, "\n");
  // 2) часто в таблицах пишут "/n" — аккуратно заменяем на перенос:
  // - одиночный /n на границе строки или окружённый пробелами
  t = t.replace(/(^|[\s])\/n($|[\s])/g, (m, p1, p2) => `${p1}\n${p2}`);
  // 3) редкий случай — в конце строки
  t = t.replace(/\/n$/g, "\n");
  return t;
}

function toISOLocal(dateStr, timeStr) {
  // Ожидаем YYYY-MM-DD и HH:MM в локальном TZ раннера (TZ задаём в воркфлоу)
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m] = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0);
}

function diffMin(a, b) { return (a.getTime() - b.getTime()) / 60000; }

function withinWindow(when, now, plusMin, minusMin) {
  const d = diffMin(when, now);
  return d >= -Math.abs(minusMin) && d <= Math.abs(plusMin);
}

function makeKey(row) {
  // Ключ публикации: дата+время+ссылка(и)+хэш текста
  const txt = normalizeText(row.text || "");
  const hash = crypto.createHash("md5").update(txt).digest("hex").slice(0, 8);
  return `${(row.date||"").trim()} ${(row.time||"").trim()} ${(row.photo_url||"")}${(row.video_url||"")}#${hash}`;
}

// =================== «Толстый» CSV-парсер (без зависимостей) ===================
function detectSepFromHeader(src) {
  let inQ = false, commas = 0, semis = 0;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (ch === '"') {
      if (inQ && src[i + 1] === '"') { i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === "\n") {
      break;
    } else if (!inQ && ch === ",") commas++;
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
    if (!inQ && ch === sep) {
      row.push(field); field = ""; continue;
    }
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

    if (obj.text) obj.text = normalizeText(obj.text);

    const meaningful = Object.values(obj).some(v => String(v).trim() !== "");
    if (meaningful) rows.push(obj);
  }
  return { rows, sep };
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
    if (!j.ok) throw new Error(`${method} failed: ${JSON.stringify(j)}`);
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
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try {
    const raw = fs.readFileSync(SENT_FILE, "utf8").trim();
    return raw ? JSON.parse(raw) : {};
  } catch { return {}; }
}
function writeSent(x) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

// =================== MAIN ===================
async function main() {
  const csvPath = path.resolve("avtopost.csv"); // имя файла не меняем
  const csv = parseCSV(csvPath);
  const sent = readSent();
  const now = new Date();

  // анти-дубль по частоте (в минутах)
  const lastAt = sent.__last_ts ? new Date(sent.__last_ts) : null;
  const coolOk = !lastAt || diffMin(now, lastAt) >= ANTI_DUP_MIN;

  let posted = 0;

  for (const row of csv.rows) {
    if (posted >= MAX_PER_RUN) break;

    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    const key = makeKey(row);

    // окно публикации (раньше/позже) + авто-допубликация
    const inMainWindow = withinWindow(when, now, WINDOW_MIN, LAG_MIN);
    const missedButGrace =
      diffMin(now, when) > WINDOW_MIN && diffMin(now, when) <= WINDOW_MIN + MISS_GRACE_MIN;

    if (!inMainWindow && !missedButGrace) continue;
    if (sent[key]) continue; // уже отправляли когда-то
    if (!coolOk) continue; // слишком часто

    // кнопки из CSV (btn1_text/btn1_url ... btn4_*)
    const kb = buildInlineKeyboard(row);

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

      // ==== ПРОМО-КАРТОЧКА В КАНАЛ (под постом) ====
      if (LINK_ORDER) {
        const promoText =
          'Мой ассистент\n' +
          '✨ Хочу показать, как ваш бизнес может сэкономить часы работы каждую неделю.\n' +
          'Откройте меня в личке — пришлю PDF и 3 коротких вопроса для первого шага.';
        const promoKb = {
          inline_keyboard: [[
            { text: '📩 Открыть бота', url: `${LINK_ORDER}${LINK_ORDER.includes("?") ? "&" : "?"}start=from_channel` }
          ]]
        };
        await sleep(300);
        await TG.sendText(promoText, promoKb);
      }
      // ============================================

      sent[key] = true;
      sent.__last_ts = new Date().toISOString();
      posted++;
      await sleep(600);
    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${err?.message || err}`);
    }
  }

  writeSent(sent);

  // Уведомление только если действительно что-то опубликовали
  if (posted > 0) {
    await TG.notifyOwner(
      `✅ Опубликовано: ${posted} ` +
      `(окно +${WINDOW_MIN}/-${LAG_MIN} мин; авто-доп. ${MISS_GRACE_MIN} мин)`
    );
  }

  // Вечерний отчёт раз в день
  const todayStr = new Date().toISOString().slice(0, 10);
  const nowLocal = new Date();
  const needDailyReport = nowLocal.getHours() >= REPORT_HOUR && (sent.__report_date !== todayStr);

  if (needDailyReport) {
    let totalToday = 0, sentToday = 0;

    for (const row of csv.rows) {
      const d = (row.date || "").trim();
      if (d === todayStr) {
        totalToday++;
        const k = makeKey(row);
        if (sent[k]) sentToday++;
      }
    }

    await TG.notifyOwner(
      `🗓 Ежедневный отчёт (${todayStr}):\n` +
      `Запланировано на сегодня: ${totalToday}\n` +
      `Фактически опубликовано: ${sentToday}`
    );
    sent.__report_date = todayStr;
    writeSent(sent);
  }
}

function buildInlineKeyboard(row) {
  const btns = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`] || "").trim();
    if (t && u) btns.push([{ text: t, url: u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
