// scripts/cron_poster.js
import fs from "fs";
import path from "path";
import { parse as csvParse } from "csv-parse/sync";

// === настройки окружения ===
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;
const OWNER_ID = process.env.OWNER_ID;

const TZ = process.env.TZ || "Europe/Kaliningrad";
const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10);
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10);
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10);

const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10);
const ANTI_DUP_MIN = parseInt(process.env.ANTI_DUP_MIN || "180", 10);
const MISS_GRACE_MIN = parseInt(process.env.MISS_GRACE_MIN || "15", 10);

// === файлы ===
const CSV_FILE = path.join(process.cwd(), "avtopost.csv");
const SENT_FILE = path.join(process.cwd(), "sent.json");

// === вспомогательные ===
function readCSV() {
  const raw = fs.readFileSync(CSV_FILE, "utf8");
  const rows = csvParse.parse(raw, { columns: true, skip_empty_lines: true });
  return rows.map((obj) => {
    if (obj.text)
      obj.text = obj.text
        // заменяем \n
        .replace(/\\n/g, "\n")
        // заменяем /n
        .replace(/\/n/g, "\n");
    return obj;
  });
}

function readSent() {
  if (!fs.existsSync(SENT_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(SENT_FILE, "utf8"));
  } catch {
    return {};
  }
}

function writeSent(sent) {
  fs.writeFileSync(SENT_FILE, JSON.stringify(sent, null, 2));
}

function toISODateTime(dateStr, timeStr) {
  return new Date(`${dateStr}T${timeStr}:00.000Z`);
}

function withinWindow(when, now) {
  const diffMin = (when - now) / 60000;
  return diffMin >= -LAG_MIN && diffMin <= WINDOW_MIN;
}

// === Telegram API ===
async function tg(method, body) {
  const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!data.ok) throw new Error(`${method} failed: ${JSON.stringify(data)}`);
  return data;
}

async function sendToChannel(row) {
  const text = row.text || "";
  const kb = {
    inline_keyboard: [
      [
        {
          text: "🧠 Хочу бота",
          url: process.env.LINK_ORDER || "https://t.me/" + OWNER_ID,
        },
      ],
    ],
  };

  if (row.photo_url) {
    await tg("sendPhoto", {
      chat_id: CHANNEL_ID,
      photo: row.photo_url,
      caption: text,
      reply_markup: kb,
    });
  } else if (row.video_url) {
    await tg("sendVideo", {
      chat_id: CHANNEL_ID,
      video: row.video_url,
      caption: text,
      reply_markup: kb,
    });
  } else {
    await tg("sendMessage", {
      chat_id: CHANNEL_ID,
      text: text,
      reply_markup: kb,
    });
  }
}

// === основная логика ===
async function main() {
  const csv = readCSV();
  const sent = readSent();

  const now = new Date();
  let published = 0;

  for (const row of csv) {
    if (!row.date || !row.time) continue;

    const when = toISODateTime(row.date, row.time);
    if (!withinWindow(when, now)) continue;

    const key = `${row.date}_${row.time}`;
    if (sent[key]) continue;

    // публикуем
    await sendToChannel(row);

    sent[key] = true;
    writeSent(sent);

    published++;
    if (published >= MAX_PER_RUN) break;
  }
}

main().catch(async (e) => {
  console.error(e);
  await tg("sendMessage", {
    chat_id: OWNER_ID,
    text: `❌ Скрипт упал: ${e.message}`,
  });
  process.exit(1);
});
