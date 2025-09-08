// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// ============== ENV =================
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100xxxxxxxxxxx (НЕ @username!)
const OWNER_ID = process.env.OWNER_ID || ""; // кому слать уведомления (user_id)
const TZ = process.env.TZ || "Europe/Moscow";

const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10); // окно +N мин
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10); // допуск -N мин
const MISS_GRACE = parseInt(process.env.MISS_GRACE_MIN || "15", 10); // автодоп.публикация через N мин, если пропустили
const ANTI_DUP = parseInt(process.env.ANTI_DUP_MIN || "180", 10); // анти-дубль: не чаще, чем раз в N мин
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10); // не более X постов за один прогон
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10); // час для дневного отчёта

const KEEPALIVE_URL = (process.env.KEEPALIVE_URL || "").trim();

const LINK_SKILLS = (process.env.LINK_SKILLS || "").trim();
const LINK_PRICES = (process.env.LINK_PRICES || "").trim();
const LINK_FEEDBACK = (process.env.LINK_FEEDBACK || "").trim();
const LINK_ORDER = (process.env.LINK_ORDER || "").trim(); // CTA

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// публикуем ОТ ИМЕНИ КАНАЛА (можно переопределить секретом SENDER_CHAT_ID)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// ============== helpers ==============
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

function convertDriveUrl(u){
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      // https://drive.google.com/file/d/<id>/view -> direct
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch(_) {}
  return u.trim();
}

function toISOLocal(dateStr, timeStr){
  // YYYY-MM-DD, HH:MM — интерпретируем в локальной TZ раннера (выставляем во workflow)
  const [Y,M,D] = (dateStr||"").split("-").map(Number);
  const [h,m] = (timeStr||"").split(":").map(Number);
  return new Date(Y,(M||1)-1,D,h||0,m||0);
}

function diffMin(a,b){ return (a.getTime()-b.getTime())/60000; }

function withinWindow(when, now, winPlus, lagMinus){
  const d = diffMin(when, now);
  return d >= -lagMinus && d <= winPlus;
}

// ============== «толстый» CSV-парсер ==============
function detectSepFromHeader(src){
  let inQ=false, c=0, s=0;
  for (let i=0;i<src.length;i++){
    const ch=src[i];
    if (ch === '"'){
      if (inQ && src[i+1]==='"'){ i++; }
      else inQ=!inQ;
      continue;
    }
    if (!inQ && ch === ",") c++;
    else if (!inQ && ch === ";") s++;
    else if (!inQ && ch === "\n") break;
  }
  return s>c ? ";" : ",";
}

function parseCSV(filePath){
  let s = fs.readFileSync(filePath, "utf8");
  s = s.replace(/^\uFEFF/,"").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return { rows:[], sep:"," };

  const sep = detectSepFromHeader(s);

  const rowsRaw=[]; let row=[], field="", inQ=false;
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
      if (row.some(v => String(v).trim() !== "")) rowsRaw.push(row);
      row=[]; continue;
    }
    field += ch;
  }
  if (field.length>0 || row.length>0){
    row.push(field);
    if (row.some(v => String(v).trim() !== "")) rowsRaw.push(row);
  }
  if (!rowsRaw.length) return { rows:[], sep };

  const headers = rowsRaw[0].map(h => String(h||"").trim());
  const data = rowsRaw.slice(1);

  const rows = [];
  for (const rec of data){
    const obj = {};
    for (let i=0;i<headers.length;i++){
      obj[headers[i]] = (rec[i] ?? "").toString();
    }
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;

    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);

    if (obj.text) obj.text = obj.text.replace(/\\n/g,"\n");

    const meaningful = Object.values(obj).some(v => String(v).trim() !== "");
    if (meaningful) rows.push(obj);
  }
  return { rows, sep };
}

// ============== кнопки (в канал под пост) ==============
function buildInlineKeyboard(row){
  const list = [];

  // постовые (из CSV): btn1_text/btn1_url ... btn4_text/btn4_url
  for (let i=1;i<=4;i++){
    const t=(row[`btn${i}_text`]||"").trim();
    const u=(row[`btn${i}_url`] ||"").trim();
    if (t && u) list.push([{ text:t, url:u }]);
  }

  // глобальный ряд
  const extra=[];
  if (LINK_SKILLS) extra.push({ text:"🧠 Что умеет?", url:LINK_SKILLS });
  if (LINK_PRICES) extra.push({ text:"💰 Цены", url:LINK_PRICES });
  if (LINK_FEEDBACK) extra.push({ text:"💬 Отзывы", url:LINK_FEEDBACK });
  if (LINK_ORDER) extra.push({ text:"🛒 Хочу бота", url:LINK_ORDER }); // ← CTA в канал

  if (extra.length) list.push(extra);
  return list.length ? { inline_keyboard:list } : undefined;
}

// ============== Telegram API ==============
const TG = {
  async call(method, payload){
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=>({}));
    if (!j.ok){
      throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    }
    return j.result;
  },

  async sendText(text, reply_markup){
    return this.call("sendMessage",{
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
    return this.call("sendPhoto",{
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
    return this.call("sendVideo",{
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
    try { await this.call("sendMessage",{ chat_id:OWNER_ID, text }); } catch(_) {}
  }
};

// ============== sent.json ==============
const SENT_FILE = path.resolve("sent.json");
function readSent(){
  try { return JSON.parse(fs.readFileSync(SENT_FILE,"utf8")); }
  catch { return {}; }
}
function writeSent(x){
  fs.writeFileSync(SENT_FILE, JSON.stringify(x,null,2));
}

// ключ для уникальности публикации
function makeKey(row){
  const date = (row.date||"").trim();
  const time = (row.time||"").trim();
  const media = (row.photo_url||row.video_url||"").trim();
  const text = (row.text||"").trim().slice(0,80);
  return `${date} ${time} | ${media} | ${text}`;
}

// ============== MAIN ====================
async function main(){
  // 0) keepalive (опционально)
  if (KEEPALIVE_URL){
    try { await fetch(KEEPALIVE_URL, { method:"GET" }); } catch(_) {}
  }

  // 1) CSV
  const csvPath = path.resolve("avtopost.csv");
  const csv = parseCSV(csvPath);

  // 2) Sent-лог
  const sent = readSent();

  // 3) текущее время в локальной зоне раннера (TZ задаём во workflow)
  const now = new Date();

  // 4) анти-дубли и лимит на прогон
  const lastAt = sent.__last_post_at ? new Date(sent.__last_post_at) : null;
  const minutesSinceLast = lastAt ? diffMin(now, lastAt) : Infinity;
  let publishedThisRun = 0;

  // 5) основной цикл
  for (const row of csv.rows){
    const date = (row.date||"").trim();
    const time = (row.time||"").trim();
    const text = (row.text||"").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    const key = makeKey(row);

    // окно публикации: [-LAG_MIN ; +WINDOW_MIN]
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    // уже публиковали этот пост ранее?
    if (sent[key]) continue;

    // анти-дубль по времени
    if (minutesSinceLast < ANTI_DUP) {
      // пропустим в этом прогоне, автодополнение через MISS_GRACE сделает позже
      continue;
    }

    // лимит на один прогон
    if (publishedThisRun >= MAX_PER_RUN) break;

    // подготовка кнопок
    const kb = buildInlineKeyboard(row);

    try {
      if (row.photo_url){
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url){
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      // отметим отправку
      sent[key] = true;
      publishedThisRun++;
      sent.__last_post_at = new Date().toISOString();
      writeSent(sent);

      await TG.notifyOwner(`✅ Опубликовано: 1 (окно +${WINDOW_MIN} / −${LAG_MIN} мин; авто-доп. после ${MISS_GRACE} мин)`);
      await sleep(600); // лёгкая пауза

    } catch (err){
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // 6) если сегодня есть пост по расписанию и мы его не поймали — автодогон через MISS_GRACE
  // (публикуем, если текущее время > when + MISS_GRACE и записи ещё нет)
  const todayISO = new Date().toISOString().slice(0,10);
  for (const row of csv.rows){
    const d=(row.date||"").trim();
    const t=(row.time||"").trim();
    const text=(row.text||"").trim();
    if (!d || !t || !text) continue;
    if (d !== todayISO) continue;

    const when = toISOLocal(d,t);
    const key = makeKey(row);
    if (sent[key]) continue;

    if (diffMin(now, when) >= MISS_GRACE){ // прошло >= MISS_GRACE минут после планового времени
      // проверим анти-дубль и лимит
      if (sent.__last_post_at){
        const last = new Date(sent.__last_post_at);
        if (diffMin(now,last) < ANTI_DUP) break;
      }
      if (publishedThisRun >= MAX_PER_RUN) break;

      // публикуем догон
      const kb = buildInlineKeyboard(row);
      try{
        if (row.photo_url){
          const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
          await TG.sendPhoto(row.photo_url, cap, kb);
          if (text.length > 1000){
            await sleep(400);
            await TG.sendText(text.slice(1000), undefined);
          }
        } else if (row.video_url){
          const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
          await TG.sendVideo(row.video_url, cap, kb);
          if (text.length > 1000){
            await sleep(400);
            await TG.sendText(text.slice(1000), undefined);
          }
        } else {
          await TG.sendText(text, kb);
        }
        sent[key] = true;
        publishedThisRun++;
        sent.__last_post_at = new Date().toISOString();
        writeSent(sent);

        await TG.notifyOwner(`✅ Догон по расписанию: 1 (просрочка ≥ ${MISS_GRACE} мин)`);
      } catch(err){
        await TG.notifyOwner(`❌ Ошибка догон-публикации: ${d} ${t}\n${(err && err.message) || err}`);
      }
      break; // одного достаточно
    }
  }

  // 7) дневной отчёт — ровно один раз в день после REPORT_HOUR (по локальной TZ раннера)
  const nowLocal = new Date();
  const needDaily = nowLocal.getHours() >= REPORT_HOUR && sent.__report_date !== todayISO;

  if (needDaily){
    let totalToday=0, sentToday=0;
    for (const row of csv.rows){
      const d=(row.date||"").trim();
      if (d === todayISO){
        totalToday++;
        const key = makeKey(row);
        if (sent[key]) sentToday++;
      }
    }
    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${todayISO}):\n` +
      `Запланировано на сегодня: ${totalToday}\n` +
      `Фактически опубликовано: ${sentToday}`
    );
    sent.__report_date = todayISO;
    writeSent(sent);
  }
}

main().catch(async (e)=>{
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
