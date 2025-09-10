// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// ============= ENV =================
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID;            // -100xxxxxxxxxxx
const OWNER_ID    = process.env.OWNER_ID || "";        // user_id для уведомлений
const TZ          = process.env.TZ || "UTC";           // для логики времени раннера
const WINDOW_MIN  = parseInt(process.env.WINDOW_MIN || "30", 10); // +сколько минут после планового времени ловим пост
const LAG_MIN     = parseInt(process.env.LAG_MIN     || "10", 10); // -сколько минут ДО времени тоже считаем окном
const ANTI_DUP_MIN= parseInt(process.env.ANTI_DUP_MIN|| "180",10); // не постить одинаковое в пределах Х минут
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10);  // не больше N постов за один прогон
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10); // час (локальный TZ раннера) для отчёта

// публикуем от имени канала — это важно для появления кнопки "Обсудить"
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID");
  process.exit(1);
}

// ============= Утилиты =============
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

// Google Drive «view» -> прямой «uc?export=download&id=...»
function convertDriveUrl(u){
  if (!u) return "";
  try{
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")){
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  }catch(_){}
  return u.trim();
}

function toLocalDate(dateStr, timeStr){
  // date: YYYY-MM-DD, time: HH:MM
  const [Y,M,D] = (dateStr||"").split("-").map(Number);
  const [h,m]   = (timeStr||"").split(":").map(Number);
  return new Date(Y, (M||1)-1, D||1, h||0, m||0, 0, 0); // локальная зона раннера (TZ задаётся в workflow)
}

// в окне [-LAG_MIN; +WINDOW_MIN] от now?
function withinWindow(when, now, wPlusMin, wMinusMin){
  const diffMin = (when.getTime() - now.getTime())/60000;
  return diffMin <= wPlusMin && diffMin >= -wMinusMin;
}

// ключ для де-дубля
function makeKey(row){
  const date = (row.date||"").trim();
  const time = (row.time||"").trim();
  const media = (row.photo_url || row.video_url || "").trim();
  // на случай длинных текстов усечём для ключа
  const text = (row.text||"").trim().slice(0, 80);
  return `${date} ${time} | ${media} | ${text}`;
}

// ============= Толстый CSV-парсер =============
function detectSepFromHeader(src){
  let inQ=false, c=0, s=0;
  for (let i=0;i<src.length;i++){
    const ch=src[i];
    if (ch === '"'){
      if (inQ && src[i+1]==='"'){ i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === ",") c++;
    else if (!inQ && ch === ";")   s++;
    else if (!inQ && ch === "\n")  break;
  }
  return s>c ? ";" : ",";
}

function parseCSV(filePath){
  let s = fs.readFileSync(filePath, "utf8");
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return { rows: [], sep: "," };

  const sep = detectSepFromHeader(s);
  const records = [];
  let row=[], field="", inQ=false;

  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (ch === '"'){
      if (inQ && s[i+1]==='"'){ field+='"'; i++; }
      else inQ = !inQ;
      continue;
    }
    if (!inQ && ch === sep){ row.push(field); field=""; continue; }
    if (!inQ && ch === "\n"){
      row.push(field); field="";
      if (row.some(x => String(x).trim()!== "")) records.push(row);
      row=[]; continue;
    }
    field += ch;
  }
  if (field.length>0 || row.length>0){
    row.push(field);
    if (row.some(x => String(x).trim()!== "")) records.push(row);
  }
  if (!records.length) return { rows: [], sep };

  const headers = records[0].map(h => String(h||"").trim());
  const data = records.slice(1);

  const out=[];
  for (const rec of data){
    const obj={};
    headers.forEach((h,idx)=> obj[h] = (rec[idx] ?? "").toString());

    // алиасы
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;

    // нормализуем
    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);
    if (obj.text)      obj.text      = obj.text.replace(/\\n/g, "\n");

    const meaningful = Object.values(obj).some(v => String(v).trim()!=="");
    if (meaningful) out.push(obj);
  }
  return { rows: out, sep };
}

// ============= Telegram API =============
const TG = {
  async call(method, payload){
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=> ({}));
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
    try{ await this.call("sendMessage", { chat_id: OWNER_ID, text }); }catch(_){}
  }
};

// ============= Кнопки (из CSV) =============
function buildInlineKeyboard(row){
  const btns=[];
  for (let i=1;i<=4;i++){
    const t=(row[`btn${i}_text`]||"").trim();
    const u=(row[`btn${i}_url`] ||"").trim();
    if (t && u) btns.push([{ text:t, url:u }]);
  }
  return btns.length ? { inline_keyboard: btns } : undefined;
}

// ============= Лог отправленного =============
const SENT_FILE = path.resolve("sent.json");

// формат: массив объектов { key, ts }
function readSent(){
  try{
    const raw = fs.readFileSync(SENT_FILE, "utf8").trim();
    if (!raw) return [];
    const v = JSON.parse(raw);
    return Array.isArray(v) ? v : [];
  }catch(_){ return []; }
}
function writeSent(arr){
  fs.writeFileSync(SENT_FILE, JSON.stringify(arr, null, 2));
}

function sentHasRecentDuplicate(sentArr, key, now, antiDupMin){
  const since = now.getTime() - antiDupMin*60*1000;
  return sentArr.some(it => it.key === key && it.ts >= since);
}

// ============= MAIN =============
async function main(){
  // чтобы локальный Date шёл в нужной зоне (для логов)
  process.env.TZ = TZ;

  const csvPath = path.resolve("avtopost.csv");
  if (!fs.existsSync(csvPath)){
    await TG.notifyOwner("⚠️ Не найден avtopost.csv");
    return;
  }

  const { rows } = parseCSV(csvPath);
  const sentArr  = readSent();
  const now      = new Date();

  let posted = 0;

  for (const row of rows){
    if (posted >= MAX_PER_RUN) break;

    const date = (row.date||"").trim();
    const time = (row.time||"").trim();
    let   text = (row.text||"").trim();

    if (!date || !time || !text) continue;

    const when = toLocalDate(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sentHasRecentDuplicate(sentArr, key, now, ANTI_DUP_MIN)){
      // недавно уже такое публиковали — пропускаем
      continue;
    }

    // кнопки из CSV
    const kb = buildInlineKeyboard(row);

    try{
      if (row.photo_url){
        const cap = text.length>1000 ? text.slice(0,1000)+"…" : text;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000){
          await sleep(500);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url){
        const cap = text.length>1000 ? text.slice(0,1000)+"…" : text;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000){
          await sleep(500);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      // записываем факт публикации
      sentArr.push({ key, ts: Date.now() });
      writeSent(sentArr);

      posted++;
      await TG.notifyOwner(`✅ Опубликовано: 1 (окно +${WINDOW_MIN}/-${LAG_MIN} мин; лимит ${MAX_PER_RUN}, антидубль ${ANTI_DUP_MIN} мин)`);
      await sleep(700);
    }catch(err){
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // Вечерний отчёт 1 раз в сутки
  const todayISO = new Date().toISOString().slice(0,10); // только дата
  // флажок храним в sent.json как спец-запись
  const hadReport = sentArr.some(x => x.key === `__report:${todayISO}`);

  const nowLocal = new Date();
  if (!hadReport && nowLocal.getHours() >= REPORT_HOUR){
    // считаем план/факт за сегодня
    let totalToday=0, factToday=0;
    for (const row of rows){
      if ((row.date||"").trim() === todayISO){
        totalToday++;
        const k = makeKey(row);
        if (sentArr.some(x => x.key === k)) factToday++;
      }
    }
    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${todayISO}):\n`+
      `Запланировано на сегодня: ${totalToday}\n`+
      `Фактически опубликовано: ${factToday}`
    );
    sentArr.push({ key:`__report:${todayISO}`, ts: Date.now() });
    writeSent(sentArr);
  }
}

main().catch(async (e)=>{
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e && e.message ? e.message : String(e)}`);
  process.exit(1);
});
