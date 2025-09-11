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

// === Telegram API ===
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;
const OWNER_ID = process.env.OWNER_ID;

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Нет BOT_TOKEN или CHANNEL_ID в переменных окружения!");
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
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function buildInlineKeyboard(row) {
  return {
    inline_keyboard: [
      [
        {
          text: "📩 Хочу бота",
          url: process.env.LINK_ORDER || "https://t.me/Ka_terina8",
        },
      ],
    ],
  };
}

// === Основная логика публикации ===
let due = 0;
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10);

for (const row of rows) {
  if (due >= MAX_PER_RUN) break;

  const date = (row.date || "").trim();
  const time = (row.time || "").trim();
  const text = (row.text || "").replace(/\\n/g, "\n").trim(); // ✅ норм переносы строк
  const photo = (row.photo || "").trim();

  if (!date || !time || !text) continue;

  const key = `${date}_${time}_${text.slice(0, 30)}`;
  if (sent[key]) continue;

  const kb = buildInlineKeyboard(row);

  try {
    if (photo) {
      // если есть картинка
      await fetch(`${TG_API}/sendPhoto`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: CHANNEL_ID,
          photo,
          caption: text,
          parse_mode: "HTML",
          reply_markup: kb,
        }),
      });
    } else {
      // только текст
      await fetch(`${TG_API}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: CHANNEL_ID,
          text,
          parse_mode: "HTML",
          reply_markup: kb,
        }),
      });
    }

    sent[key] = true;
    fs.writeFileSync(sentFile, JSON.stringify(sent, null, 2));
    due++;
    await sleep(1500);
  } catch (err) {
    console.error("Ошибка публикации:", err.message);
    if (OWNER_ID) {
      await fetch(`${TG_API}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: OWNER_ID,
          text: `❌ Ошибка публикации: ${err.message}`,
        }),
      });
    }
  }
}

console.log(`Завершено. Опубликовано: ${due}`);
