// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

/* ===================== ENV ===================== */
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // строго -100XXXXXXXX
const OWNER_ID = process.env.OWNER_ID || ""; // опционально
const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10); // окно +мин
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10); // окно -мин
const MISS_GRACE = parseInt(process.env.MISS_GRACE_MIN || "15", 10); // «догоним после»
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10);
const ANTI_DUP_MIN= parseInt(process.env.ANTI_DUP_MIN || "15", 10);
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10);
const NOTIFY_MODE = (process.env.NOTIFY_MODE || "post_only"); // 'post_only' | 'all' | 'silent'

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID");
  process.exit(1);
}

// публикуем от имени канала (можно переопределить секретом SENDER_CHAT_ID)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

/* ===================== Утилиты ===================== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function ymdLocal(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function toISOLocal(dateStr, timeStr) {
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m] = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0, 0, 0); // локальная зона
}

// dt в окне [now - lagMin; now + windowMin]
function withinWindow(dt, now, windowMin, lagMin) {
  const diffMin = (dt.getTime() - now.getTime()) / 60000;
  return diffMin >= -lagMin && diffMin <= windowMin;
}

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

function convertDriveUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      // /file/d/<id>/view → uc?id=<id>
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch (_) {}
  return u.trim();
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

function buildInlineKeyboard(row) {
  const kb = [];
  for (let i = 1; i <= 4; i++) {
    const t = (row[`btn${i}_text`] || "").trim();
    const u = (row[`btn${i}_url`] || "").trim();
    if (t && u) kb.push([{ text: t, url: u }]);
  }
  return kb.length ? { inline_keyboard: kb } : undefined;
}

function sha1(str) {
  // простенький, чтобы сделать ключ из текста
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h << 5) - h + str.charCodeAt(i);
    h |= 0;
  }
  return String(h);
}
function makeKey(row) {
  const base = `${(row.date||"").trim()} ${(row.time||"").trim()} ${(row.photo_url||"")}${(row.video_url||"")}`;
  const text = normalizeText(row.text);
  return `${base} #${sha1(txt.slice(0, 200))}`; // ключ стабилен, но короткий
}
function normalizeText(s) {
  return String(s ?? "")
    // нормализуем все типы переносов
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    // поддержка экранированных переносов из CSV
    .replace(/\\n/gi, "\n") // "\n" -> перенос
    .replace(/\/n/gi, "\n") // "/n" -> перенос (на всякий случай)
    .replace(/\t/g, " ")
    // убираем лишние пробелы перед переводами строк
    .replace(/[ \u00A0]+\n/g, "\n")
    .trim();
}

/* ===================== Telegram API ===================== */
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
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch {}
  }
};

// централизованный фильтр уведомлений
async function notify(kind, text) {
  // kind: 'post' | 'report' | 'error'
  if (NOTIFY_MODE === "silent" && kind !== "error") return;
  if (NOTIFY_MODE === "post_only" && kind !== "post" && kind !== "error") return;
  await TG.notifyOwner(text);
}

/* ===================== sent.json ===================== */
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(x) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

/* ===================== MAIN ===================== */
async function main() {
  const csvPath = path.resolve("avtopost.csv");
  const csv = parseCSV(csvPath);
  const sent = readSent();

  const now = new Date();
  const todayLocal = ymdLocal(now);

  // антидубль между прогонами: не чаще раза в ANTI_DUP_MIN
  const lastAt = sent.__last_post_at ? new Date(sent.__last_post_at) : null;
  if (lastAt) {
    const dtMin = (now.getTime() - lastAt.getTime()) / 60000;
    if (dtMin < ANTI_DUP_MIN) {
      // слишком рано после предыдущей публикации — выходим молча
      // (ничего не публиковали => уведомлений не будет)
      return finishReports(csv, sent, now, todayLocal);
    }
  }

  let posted = 0;
  let attempted = 0;

  // 1) обычная публикация в окно [-LAG_MIN ; +WINDOW_MIN]
  for (const row of csv.rows) {
    if (posted >= MAX_PER_RUN) break;

    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = normalizeText(row.text);
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sent[key]) continue; // уже отправляли

    attempted++;
    const kb = buildInlineKeyboard(row);

    try {
      if (row.photo_url) {
        const cap = full.length > 1000 ? full.slice(0, 1000) + "…" : full;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url) {
        const cap = full.length > 1000 ? full.slice(0, 1000) + "…" : full;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      sent[key] = true;
      posted++;
      sent.__last_post_at = new Date().toISOString();
      writeSent(sent);

      await notify("post", `✅ Опубликовано: 1 (окно +${WINDOW_MIN} / −${LAG_MIN} мин; авто-доп. после ${MISS_GRACE} мин)`);
      await sleep(600);
    } catch (err) {
      await notify("error", `❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // 2) автодогон «проспавших» постов (если ничего не опубликовали сейчас)
  if (posted === 0) {
    for (const row of csv.rows) {
      if (posted >= MAX_PER_RUN) break;

      const date = (row.date || "").trim();
      const time = (row.time || "").trim();
      const text = normalizeText(row.text);
      if (!date || !time || !text) continue;

      const when = toISOLocal(date, time);
      const minsAgo = (now.getTime() - when.getTime()) / 60000;

      if (minsAgo >= MISS_GRACE && minsAgo <= WINDOW_MIN + LAG_MIN + 120 /* страховка */) {
        const key = makeKey(row);
        if (sent[key]) continue;

                const kb = buildInlineKeyboard(row);
         try {
  // ограничение длины подписи для фото/видео
  const textLen = text.length;
  const cap = textLen > 1000 ? text.slice(0, 1000) + "…" : text;

  if (row.photo_url) {
    await TG.sendPhoto(row.photo_url, cap, kb);
    if (textLen > 1000) {
      await sleep(500);
      await TG.sendText(text.slice(1000), undefined);
    }
  } else if (row.video_url) {
    await TG.sendVideo(row.video_url, cap, kb);
    if (textLen > 1000) {
      await sleep(500);
      await TG.sendText(text.slice(1000), undefined);
    }
  } else {
    await TG.sendText(text, kb);
  }

  sent[key] = true;
  posted++;
  await sleep(700);

          sent.__last_post_at = new Date().toISOString();
          writeSent(sent);

          await notify("post", `✅ Догон по расписанию: 1 (просрочка ≥ ${MISS_GRACE} мин)`);
          await sleep(600);
        } catch (err) {
  await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
}
      }
    }
  }

  // 3) дневной отчёт (один раз после REPORT_HOUR, локальная дата)
  await finishReports(csv, sent, now, todayLocal);
}

async function finishReports(csv, sent, now, todayLocal) {
  // отчёт шлём один раз после REPORT_HOUR
  const nowLocal = new Date();
  const todayStr = todayLocal; // уже локальная дата

  const needDaily = nowLocal.getHours() >= REPORT_HOUR &&
                    sent.__report_date !== todayStr;

  if (needDaily) {
    let totalToday = 0, sentToday = 0;

    for (const row of csv.rows) {
      const d = (row.date || "").trim();
      if (d === todayStr) {
        totalToday++;
        const key = makeKey(row);
        if (sent[key]) sentToday++;
      }
    }

    // не шумим, если и план, и факт нули
    if (totalToday > 0 || sentToday > 0) {
      await notify("report",
        `📅 Ежедневный отчёт (${todayStr}):\n` +
        `Запланировано на сегодня: ${totalToday}\n` +
        `Фактически опубликовано: ${sentToday}`
      );
    }

    sent.__report_date = todayStr;
    writeSent(sent);
  }
}

/* ===================== run ===================== */
main().catch(async (e) => {
  console.error(e);
  await notify("error", `❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
