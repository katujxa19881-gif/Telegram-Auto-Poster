// scripts/cron_poster.js
// Node 20+. Внешних зависимостей нет.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "node:crypto";

// === DIR helpers (ESM) ===
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// === ENV ===
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100xxxxxxxxxxx (не @username)
const OWNER_ID = process.env.OWNER_ID || ""; // user_id для уведомлений (опц.)
const TZ = process.env.TZ || "Europe/Kaliningrad";

const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10); // +окно, мин
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10); // -опоздание, мин
const MISS_GRACE = parseInt(process.env.MISS_GRACE_MIN || "15", 10); // автодоп. после пропуска, мин
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10); // лимит за один прогон
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10); // час ежедневного отчёта

const LINK_ORDER = (process.env.LINK_ORDER || "").trim(); // https://t.me/your_bot

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// Публикуем строго от имени канала, чтобы кнопка была в КАНАЛЕ
const SENDER_CHAT_ID = CHANNEL_ID;

// === utils ===
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

function convertDriveUrl(u){
  if (!u) return "";
  try{
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")){
      // /file/d/<id>/... → прямой скачиваемый
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  }catch(_){}
  return u.trim();
}

function toLocalDate(dateStr,timeStr){
  // YYYY-MM-DD + HH:MM -> дата в локальном TZ раннера
  const [y,m,d] = (dateStr||"").split("-").map(Number);
  const [H,M] = (timeStr||"").split(":").map(Number);
  return new Date(y,(m||1)-1,d,H||0,M||0,0,0);
}

function withinWindow(when, now, plusMin, minusMin){
  const diffMin = (when.getTime() - now.getTime())/60000;
  return diffMin >= -minusMin && diffMin <= plusMin;
}

// Хэш-ключ для антидублей
function makeKey(row){
  const src =
    (row.date||"") + " " + (row.time||"") + "\n" +
    (row.text||"") + "|" + (row.photo_url||"") + "|" + (row.video_url||"");
  return createHash("sha256").update(src).digest("hex");
}

// === «толстый» CSV-парсер (одна строка — один пост) ===
function detectSepFromHeader(src){
  let inQ=false, c=0, s=0;
  for (let i=0;i<src.length;i++){
    const ch=src[i];
    if (ch === '"'){
      if (inQ && src[i+1]==='"'){ i++; }
      else inQ=!inQ;
    }else if(!inQ && ch===",") c++;
    else if(!inQ && ch===";") s++;
    else if(!inQ && ch==="\n") break;
  }
  return s>c ? ";" : ",";
}

function parseCSV(filePath){
  let s = fs.readFileSync(filePath,"utf8");
  s = s.replace(/^\uFEFF/,"").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return { rows:[], sep:"," };

  const sep = detectSepFromHeader(s);

  const records = [];
  let row=[], field="", inQ=false;

  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (ch === '"'){
      if (inQ && s[i+1]==='"'){ field+='"'; i++; }
      else inQ=!inQ;
      continue;
    }
    if (!inQ && ch === sep){ row.push(field); field=""; continue; }
    if (!inQ && ch === "\n"){
      row.push(field); field="";
      if (row.some(c => String(c).trim() !== "")) records.push(row);
      row=[];
      continue;
    }
    field += ch;
  }
  if (field.length>0 || row.length>0){
    row.push(field);
    if (row.some(c => String(c).trim() !== "")) records.push(row);
  }
  if (!records.length) return { rows:[], sep };

  const headers = records[0].map(h => String(h||"").trim());
  const data = records.slice(1);

  const rows=[];
  for (const rec of data){
    const obj={};
    for (let i=0;i<headers.length;i++){
      obj[headers[i]] = (rec[i] ?? "").toString();
    }
    // алиасы
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;

    // починить переносы: "\n" ИЛИ "/n" → реальный перенос
    if (obj.text){
      obj.text = obj.text.replace(/\\n|\/n/g, "\n");
    }

    // починить Google Drive
    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);

    const meaningful = Object.values(obj).some(v => String(v).trim() !== "");
    if (meaningful) rows.push(obj);
  }
  return { rows, sep };
}

// === Telegram API ===
const TG = {
  async call(method, payload){
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=>({}));
    if (!j.ok){
      throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    }
    return j.result;
  },

  async sendText(text, reply_markup){
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

  async sendPhoto(photo, caption, reply_markup){
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

  async sendVideo(video, caption, reply_markup){
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

  async notifyOwner(text){
    if (!OWNER_ID) return;
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch(_){}
  }
};

// === sent.json (map) ===
const SENT_FILE = path.resolve(__dirname, "..", "sent.json");
function readSent(){
  try{
    const raw = fs.readFileSync(SENT_FILE,"utf8").trim();
    if (!raw) return {};
    const data = JSON.parse(raw);
    // если вдруг был массив — преобразуем
    if (Array.isArray(data)){
      const map = {};
      for (const k of data) map[k]=true;
      return map;
    }
    return data;
  }catch{
    return {};
  }
}
function writeSent(x){
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}

// === клавиатура (CTA в КАНАЛ) ===
function buildCTA(){
  if (!LINK_ORDER) return undefined;
  return {
    inline_keyboard: [[
      { text: "📩 Открыть бота", url: `${LINK_ORDER}?start=from_channel` }
    ]]
  };
}

// === MAIN ===
async function main(){
  // чтобы раннер работал в нужном TZ (для .getHours())
  try { process.env.TZ = TZ; } catch(_){}

  const csvPath = path.resolve(__dirname, "..", "avtopost.csv");
  const { rows } = parseCSV(csvPath);
  const sent = readSent();
  const now = new Date();

  let posted = 0;

  for (const row of rows){
    if (posted >= MAX_PER_RUN) break;

    const date = (row.date||"").trim();
    const time = (row.time||"").trim();
    let text = (row.text||"").trim();

    if (!date || !time || !text) continue;

    const when = toLocalDate(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sent[key]) continue; // уже публиковали этот материал

    const kbCTA = buildCTA();

    try{
      if (row.photo_url){
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await TG.sendPhoto(row.photo_url, cap, kbCTA);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      }else if (row.video_url){
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await TG.sendVideo(row.video_url, cap, kbCTA);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      }else{
        await TG.sendText(text, kbCTA);
      }

      sent[key] = true;
      posted++;
      await sleep(600);

    }catch(err){
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${err?.message || err}`);
    }
  }

  // автодоп. пропущенного поста (после MISS_GRACE минут от планового)
  if (posted === 0){
    for (const row of rows){
      const date = (row.date||"").trim();
      const time = (row.time||"").trim();
      let text = (row.text||"").trim();
      if (!date || !time || !text) continue;

      const when = toLocalDate(date, time);
      const diffMin = (now.getTime() - when.getTime())/60000; // сколько минут прошло после планового
      if (diffMin >= MISS_GRACE && diffMin <= MISS_GRACE + WINDOW_MIN){
        const key = makeKey(row);
        if (sent[key]) continue;

        try{
          const kbCTA = buildCTA();
          if (row.photo_url){
            const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
            await TG.sendPhoto(row.photo_url, cap, kbCTA);
            if (text.length > 1000){
              await sleep(400);
              await TG.sendText(text.slice(1000), undefined);
            }
          }else if (row.video_url){
            const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
            await TG.sendVideo(row.video_url, cap, kbCTA);
            if (text.length > 1000){
              await sleep(400);
              await TG.sendText(text.slice(1000), undefined);
            }
          }else{
            await TG.sendText(text, kbCTA);
          }
          sent[key] = true;
          posted++;
          break;
        }catch(err){
          await TG.notifyOwner(`❌ Автодоп.: ${date} ${time}\n${err?.message || err}`);
        }
      }
    }
  }

  // сохранить лог (антидубли)
  writeSent(sent);

  // мгновенное уведомление только если публиковали
  if (posted > 0){
    await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно +${WINDOW_MIN}/-${LAG_MIN} мин; авто-доп. после ${MISS_GRACE} мин)`);
  }

  // разовый ежедневный отчёт
  const todayLocal = new Date();
  const dayStr = todayLocal.toISOString().slice(0,10); // YYYY-MM-DD
  if (todayLocal.getHours() >= REPORT_HOUR && sent.__report_date !== dayStr){
    let totalToday = 0;
    let sentToday = 0;
    for (const row of rows){
      if ((row.date||"").trim() === dayStr){
        totalToday++;
        const key = makeKey(row);
        if (sent[key]) sentToday++;
      }
    }
    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${dayStr}):\n` +
      `Запланировано на сегодня: ${totalToday}\n` +
      `Фактически опубликовано: ${sentToday}`
    );
    sent.__report_date = dayStr;
    writeSent(sent);
  }
}

main().catch(async (e)=>{
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e?.message || e}`);
  process.exit(1);
});
