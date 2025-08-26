// scripts/cron_poster.js — Zero-deps GitHub Actions автопостер
import fs from "fs";
import https from "https";

// ==== ENV ====
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;
const OWNER_ID = process.env.OWNER_ID;
const TZ = process.env.TZ || "Europe/Kaliningrad";
const WINDOW_MINUTES = parseInt(process.env.WINDOW_MINUTES || "12", 10);

// ==== Helpers ====
function sendMessage(chatId, text, extra = {}) {
  return new Promise((resolve) => {
    const data = JSON.stringify({ chat_id: chatId, text, ...extra });
    const req = https.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${BOT_TOKEN}/sendMessage`,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": data.length },
      },
      (res) => res.on("data", () => {}) && res.on("end", () => resolve())
    );
    req.on("error", () => resolve());
    req.write(data);
    req.end();
  });
}

function sendPhoto(chatId, url, caption, extra = {}) {
  return new Promise((resolve) => {
    const data = JSON.stringify({ chat_id: chatId, photo: url, caption, ...extra });
    const req = https.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${BOT_TOKEN}/sendPhoto`,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": data.length },
      },
      (res) => res.on("data", () => {}) && res.on("end", () => resolve())
    );
    req.on("error", () => resolve());
    req.write(data);
    req.end();
  });
}

function sendVideo(chatId, url, caption, extra = {}) {
  return new Promise((resolve) => {
    const data = JSON.stringify({ chat_id: chatId, video: url, caption, ...extra });
    const req = https.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${BOT_TOKEN}/sendVideo`,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": data.length },
      },
      (res) => res.on("data", () => {}) && res.on("end", () => resolve())
    );
    req.on("error", () => resolve());
    req.write(data);
    req.end();
  });
}

// ==== CSV Parser ====
function parseCSV(content) {
  const lines = content.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0) return [];
  const delimiter = lines[0].includes(";") ? ";" : ",";
  const headers = lines[0].split(delimiter).map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const parts = line.split(delimiter);
    const obj = {};
    headers.forEach((h, i) => (obj[h] = (parts[i] || "").trim()));
    return obj;
  });
}

// ==== Main ====
async function main() {
  if (!BOT_TOKEN || !CHANNEL_ID) {
    console.error("Missing BOT_TOKEN or CHANNEL_ID");
    return;
  }

  const csv = fs.readFileSync("avtopost.csv", "utf8");
  const rows = parseCSV(csv);

  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  const windowStart = new Date(now.getTime() - WINDOW_MINUTES * 60000);
  const today = now.toISOString().split("T")[0];

  let dueToday = 0,
    sent = 0;

  for (const r of rows) {
    if (!r.date || !r.time) continue;
    const dtStr = `${r.date}T${r.time}:00`;
    const dt = new Date(dtStr);

    if (dt.toISOString().split("T")[0] === today) {
      dueToday++;
      if (dt >= windowStart && dt <= now) {
        const caption = r.text || "";
        const buttons = [];

        if (r.btn1_text && r.btn1_url)
          buttons.push([{ text: r.btn1_text, url: r.btn1_url }]);
        if (r.btn2_text && r.btn2_url)
          buttons.push([{ text: r.btn2_text, url: r.btn2_url }]);
        if (r.btn3_text && r.btn3_url)
          buttons.push([{ text: r.btn3_text, url: r.btn3_url }]);
        if (r.btn4_text && r.btn4_url)
          buttons.push([{ text: r.btn4_text, url: r.btn4_url }]);

        const extra = buttons.length ? { reply_markup: { inline_keyboard: buttons } } : {};

        if (r.photo_url) await sendPhoto(CHANNEL_ID, r.photo_url, caption, extra);
        else if (r.video_url) await sendVideo(CHANNEL_ID, r.video_url, caption, extra);
        else await sendMessage(CHANNEL_ID, caption, extra);

        sent++;
      }
    }
  }

  if (dueToday > 0 && sent === 0) {
    await sendMessage(
      OWNER_ID,
      `⚠️ GitHub Cron: постов в окне ${WINDOW_MINUTES} мин не найдено.\n` +
        `(сегодня строк: ${dueToday}, из них уже «должны быть»: ${dueToday}, фактически отправлено: ${sent})`
    );
  }

  console.log(`Done: today=${dueToday}, sent=${sent}`);
}

main();
