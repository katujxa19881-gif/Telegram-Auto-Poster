// scripts/cron_poster.js
// Node.js 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// =================== ENV ===================
const BOT_TOKEN      = process.env.BOT_TOKEN;
const CHANNEL_ID     = process.env.CHANNEL_ID;           // -100xxxxxxxxxxx (НЕ @username!)
const OWNER_ID       = process.env.OWNER_ID || "";       // для уведомлений в ЛС (опц.)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// окно вперёд (минуты) и допуск назад (минуты)
const WINDOW_MIN     = parseInt(process.env.WINDOW_MIN || "30", 10);
const LAG_MIN        = parseInt(process.env.LAG_MIN || "10", 10);

// через сколько минут после планового времени считать пост «пропущенным» и допубликовать
const MISS_GRACE_MIN = parseInt(process.env.MISS_GRACE_MIN || "15", 10);

// час вечернего отчёта (строго в HH:00 по локальному TZ раннера — см. TZ в workflow)
const REPORT_HOUR    = parseInt(process.env.REPORT_HOUR || "21", 10);

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// =================== Утилиты ===================
const SENT_FILE = path.resolve("sent.json");
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

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
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m]    = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M || 1) - 1, D, h || 0, m || 0); // локальная зона
}

// берём посты, если они попадают в окно [-LAG_MIN; +WINDOW_MIN] относительно текущего запуска
function withinWindow(when, now, windowMin, lagMin) {
  const diffMin = (when - now) / 60000;
  return diffMin >= -lagMin && diffMin <= windowMin;
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
      sender_chat_id: SENDER_CHAT_ID,   // публикуем от имени канала
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

// =================== Хэш-ключ строки для sent.json ===================
function makeKey(row) {
  const trim = (s) => String(s || "").trim();
  const textFrag = trim(row.text).replace(/\s+/g, " ").slice(0, 120);
  return `${trim(row.date)} ${trim(row.time)} ${(row.photo_url||"")}${(row.video_url||"")} ${textFrag}`;
}

// =================== MAIN ===================
async function main() {
  const csv  = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();
  const now  = new Date();

  let posted = 0;

  // 1) Обычная публикация в окно [-LAG_MIN; +WINDOW_MIN]
  for (const row of csv.rows) {
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sent[key]) continue; // уже отправляли

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
      sent[key] = true;
      posted++;
      await sleep(600);
    } catch (err) {
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // 2) «Страховка»: если пост на сегодня пропущен дольше MISS_GRACE_MIN → допубликовать сейчас
  const todayStr = new Date().toISOString().slice(0, 10);
  for (const row of csv.rows) {
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (date !== todayStr || !time || !text) continue;

    const key  = makeKey(row);
    if (sent[key]) continue;

    const when = toISOLocal(date, time);
    const minutesLate = (Date.now() - when.getTime()) / 60000;

    if (minutesLate >= MISS_GRACE_MIN) {
      try {
        await TG.notifyOwner(
          `⚠️ Пропущено расписание: ${date} ${time}\n` +
          `Пробую опубликовать сейчас (задержка ≥ ${MISS_GRACE_MIN} мин).`
        );

        const kb = buildInlineKeyboard(row);

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

        sent[key] = true;
        posted++;
        await TG.notifyOwner(`✅ Поздняя публикация выполнена: ${date} ${time}`);
        await sleep(600);
      } catch (err) {
        await TG.notifyOwner(
          `❌ Не удалось допубликовать пропущенный пост ${date} ${time}:\n` +
          `${(err && err.message) || err}`
        );
      }
    }
  }

  // Сохраняем состояние
  writeSent(sent);

  // Уведомление «по факту» — только если что-то опубликовали
  if (posted > 0) {
    await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно +${WINDOW_MIN} / -${LAG_MIN} мин; авто-доп. после ${MISS_GRACE_MIN} мин)`);
  }

  // 3) Ежедневный отчёт — строго один раз в HH:00
  const nowLocal = new Date();
  const isReportTime =
    nowLocal.getHours() === REPORT_HOUR &&
    nowLocal.getMinutes() === 0;

  const todayForFlag = nowLocal.toISOString().slice(0, 10);
  if (isReportTime && sent.__report_date !== todayForFlag) {
    let totalToday = 0;
    let sentToday  = 0;
    for (const row of csv.rows) {
      const d = (row.date || "").trim();
      if (d === todayForFlag) {
        totalToday++;
        const k = makeKey(row);
        if (sent[k]) sentToday++;
      }
    }
    await TG.notifyOwner(
      `🗓 Ежедневный отчёт (${todayForFlag}):\n` +
      `Запланировано на сегодня: ${totalToday}\n` +
      `Фактически опубликовано: ${sentToday}`
    );
    sent.__report_date = todayForFlag;
    writeSent(sent);
  }
}

main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
