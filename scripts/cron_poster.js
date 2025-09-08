// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// =================== ENV ===================
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID;         // -100XXXXXXXXXX (НЕ @username)
const OWNER_ID    = process.env.OWNER_ID || "";     // опционально — отсылать отчёты в ЛС

// окно поиска постов: от -LAG до +WINDOW минут от текущего времени
const WINDOW_MIN  = parseInt(process.env.WINDOW_MIN  || process.env.WINDOW_MINUTES || "30", 10); // по умолчанию 30
const LAG_MIN     = parseInt(process.env.LAG_MIN     || "10", 10);    // «опоздание», допустим -10 мин назад
const MISS_GRACE  = parseInt(process.env.MISS_GRACE_MIN || "15", 10); // «перепроверка пропуска» (мин)

// антидублирование: если с момента последней публикации прошло меньше X минут — не публикуем
const ANTI_DUP_MIN = parseInt(process.env.ANTI_DUP_MIN || "180", 10); // 3 часа

// публиковать не больше N постов за один прогон (подстраховка)
const X_PER_RUN   = parseInt(process.env.X_PER_RUN || "1", 10);

// вечерний отчёт (час суток локального времени раннера)
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10);

// Публикуем от имени канала (важно для «Обсудить»)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// keepalive опционально — если задан, пингуем перед работой
const KEEPALIVE_URL = process.env.KEEPALIVE_URL || "";

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// =================== Helpers ===================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function sha1(x){
  return crypto.createHash("sha1").update(String(x)).digest("hex");
}

// Превращаем любые Google Drive ссылки в прямые download-ссылки
function convertDriveUrl(u) {
  if (!u) return "";
  const s = String(u).trim();

  // Уже прямая?
  if (/drive\.google\.com\/uc\b/i.test(s) && /[?&](id|export)=/i.test(s)) return s;

  let id = null;

  // /file/d/<ID>/view
  let m = s.match(/\/file\/d\/([^/]+)\//i);
  if (m) id = m[1];

  // open?id=<ID> / uc?id=<ID> / ?id=<ID>
  if (!id) {
    m = s.match(/[?&]id=([^&]+)/i);
    if (m) id = m[1];
  }

  // «произвольные» drive-ссылки, где просто встречается ID
  if (!id) {
    m = s.match(/drive\.google\.com\/(?:file\/d\/|u\/\d\/|thumbnail\?id=)?([a-zA-Z0-9_-]{10,})/i);
    if (m) id = m[1];
  }

  if (id) return `https://drive.google.com/uc?export=download&id=${id}`;
  return s;
}

function toISOLocal(dateStr, timeStr) {
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m]    = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0);
}

function withinWindow(when, now, windowMin, lagMin) {
  const diffMin = (when - now) / 60000;
  // разрешаем немного «в прошлое» (lag) и вперёд (window)
  return diffMin >= -Math.abs(lagMin) && diffMin <= Math.abs(windowMin);
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
    else if (!inQ && ch === ";") semis++;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(filePath) {
  let s = "";
  try { s = fs.readFileSync(filePath, "utf8"); } catch { return { rows: [], sep: "," }; }
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!s.trim()) return { rows: [], sep: "," };

  const sep = detectSepFromHeader(s);

  const recs = [];
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
      if (row.some(c => String(c).trim() !== "")) recs.push(row);
      row = [];
      continue;
    }
    field += ch;
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    if (row.some(c => String(c).trim() !== "")) recs.push(row);
  }
  if (!recs.length) return { rows: [], sep };

  const headers = recs[0].map(h => String(h || "").trim());
  const data    = recs.slice(1);

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
    if (!j.ok) {
      throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    }
    return j.result;
  },

  async sendText(text, reply_markup) {
    return this.call("sendMessage", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,   // Публикуем от имени канала → появится «Обсудить»
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

// =================== sent.json ===================
const SENT_FILE = path.resolve("sent.json");
function readSent() {
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(obj) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(obj, null, 2));
}
function makeKey(row) {
  // ключ поста — дата + время + тип/ссылка медиа (если есть)
  return `${(row.date||"").trim()} ${(row.time||"").trim()} ${(row.photo_url||"")}${(row.video_url||"")}`;
}

// =================== MAIN ===================
async function main() {
  // keepalive (если задан)
  if (KEEPALIVE_URL) {
    try { await fetch(KEEPALIVE_URL).catch(()=>{}); } catch {}
  }

  const csvPath = path.resolve("avtopost.csv");
  const csv = parseCSV(csvPath);
  const sent = readSent();
  const now  = new Date();

  let posted = 0;

  // антидубли по времени последней отправки
  const lastSentAtISO = sent.__last_sent_at || "";
  const lastSentAt    = lastSentAtISO ? new Date(lastSentAtISO) : null;
  const antiDupActive = lastSentAt && ((now - lastSentAt) / 60000 < ANTI_DUP_MIN);

  for (const row of csv.rows) {
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sent[key]) continue; // уже отправляли

    // антидубль по «паузе»
    if (antiDupActive) continue;

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
      sent.__last_sent_at = new Date().toISOString(); // отметим факт отправки
      writeSent(sent);

      // небольшой бридж между постами
      await sleep(700);

      // лимит «не больше N постов за прогон»
      if (posted >= X_PER_RUN) break;

    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // мгновенное уведомление — только если что-то отправили
  if (posted > 0) {
    await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно +${WINDOW_MIN}/-${LAG_MIN} мин; антидубль ${ANTI_DUP_MIN} мин; лимит за прогон ${X_PER_RUN})`);
  }

  // Разовый вечерний отчёт
  const todayStr = new Date().toISOString().slice(0, 10);
  const nowLocal = new Date();
  const needDailyReport = nowLocal.getHours() >= REPORT_HOUR && (sent.__report_date !== todayStr);

  if (needDailyReport) {
    let totalToday = 0;
    let sentToday  = 0;

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

main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
