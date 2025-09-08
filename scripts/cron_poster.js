// scripts/cron_poster.js
// Node.js 18+ (fetch встроен). ESM (type: "module").

import fs from "fs";
import path from "path";
import crypto from "crypto";

// =============== НАСТРОЙКИ ЧЕРЕЗ ENV (секреты GitHub) ===============
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // ВИД: -100xxxxxxxxxxx (НЕ @username)
const OWNER_ID = process.env.OWNER_ID || ""; // опционально: кому слать уведомления в ЛС

// Временное окно публикации:
// now попадает в диапазон [ when - LAG_MIN ; when + WINDOW_MIN ]
const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10); // вправо от времени поста (мин)
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10); // влево от времени (мин, «отставание»)

const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10); // час суток локально (21 = 21:00)
const ANTI_DUP_MIN = parseInt(process.env.ANTI_DUP_MIN || "180", 10); // защита от повторов (мин)
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10); // максимум постов за 1 прогон

// опциональные «ссылки-кнопки» (из секретов)
const LINK_SKILLS = process.env.LINK_SKILLS || "";
const LINK_PRICES = process.env.LINK_PRICES || "";
const LINK_FEEDBACK = process.env.LINK_FEEDBACK || "";
const LINK_ORDER = process.env.LINK_ORDER || "";

// публикуем от имени канала (можно переопределить SENDER_CHAT_ID секретом)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// =============== ПРОВЕРКИ ===============
if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// =============== УТИЛИТЫ ===============
const SENT_FILE = path.resolve("sent.json");

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

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

// YYYY-MM-DD + HH:MM → локальная Date (TZ берётся из раннера; в workflow можно задать TZ)
function toISOLocal(dateStr, timeStr){
  const [Y, M, D] = (dateStr || "").split("-").map(Number);
  const [h, m] = (timeStr || "").split(":").map(Number);
  return new Date(Y, (M||1)-1, D||1, h||0, m||0, 0, 0);
}

// now в окне [when - LAG_MIN ; when + WINDOW_MIN]
function withinWindow(when, now, windowMin, lagMin){
  const diffMin = (now.getTime() - when.getTime())/60000; // now - when
  return diffMin >= -lagMin && diffMin <= windowMin;
}

// Читаем/пишем sent.json
function readSent(){
  try { return JSON.parse(fs.readFileSync(SENT_FILE, "utf8")); }
  catch { return {}; }
}
function writeSent(obj){
  fs.writeFileSync(SENT_FILE, JSON.stringify(obj, null, 2));
}

// «толстый» CSV-парсер с кавычками и autodetect ,/;
function detectSepFromHeader(src){
  let inQ=false, commas=0, semis=0;
  for (let i=0;i<src.length;i++){
    const ch = src[i];
    if (ch === '"'){
      if (inQ && src[i+1] === '"'){ i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === "\n") break;
    else if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";") semis++;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(filePath){
  let s = fs.readFileSync(filePath, "utf8");
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return { rows: [], sep: "," };

  const sep = detectSepFromHeader(s);

  const rowsRaw = [];
  let row=[], field="", inQ=false;

  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (ch === '"'){
      if (inQ && s[i+1] === '"'){ field+='"'; i++; }
      else inQ = !inQ;
      continue;
    }
    if (!inQ && ch === sep){ row.push(field); field=""; continue; }
    if (!inQ && ch === "\n"){
      row.push(field); field="";
      if (row.some(c => String(c).trim()!=="")) rowsRaw.push(row);
      row=[];
      continue;
    }
    field += ch;
  }
  if (field.length>0 || row.length>0){
    row.push(field);
    if (row.some(c => String(c).trim()!=="")) rowsRaw.push(row);
  }
  if (!rowsRaw.length) return { rows: [], sep };

  const headers = rowsRaw[0].map(h => String(h||"").trim());
  const data = rowsRaw.slice(1);

  const rows = [];
  for (const rec of data){
    const obj = {};
    for (let i=0;i<headers.length;i++){
      obj[headers[i]] = (rec[i] ?? "").toString();
    }

    // алиасы
    if (!obj.photo_url && obj.photo) obj.photo_url = obj.photo;
    if (!obj.video_url && obj.video) obj.video_url = obj.video;

    // правим Drive
    if (obj.photo_url) obj.photo_url = convertDriveUrl(obj.photo_url);
    if (obj.video_url) obj.video_url = convertDriveUrl(obj.video_url);

    // \n → перенос
    if (obj.text) obj.text = obj.text.replace(/\\n/g, "\n");

    // только осмысленные строки
    if (Object.values(obj).some(v => String(v).trim()!=="")) rows.push(obj);
  }
  return { rows, sep };
}

// Клавиатура: btn1_text/btn1_url ... btn4_text/btn4_url + общие ссылки (если заданы)
function buildInlineKeyboard(row){
  const list = [];

  for (let i=1;i<=4;i++){
    const t = (row[`btn${i}_text`]||"").trim();
    const u = (row[`btn${i}_url`] ||"").trim();
    if (t && u) list.push([{ text: t, url: u }]);
  }

  // Глобальные ссылки (если нет кастомных строк)
  if (LINK_SKILLS || LINK_PRICES || LINK_FEEDBACK || LINK_ORDER){
    const extra = [];
    if (LINK_SKILLS) extra.push({ text:"🧠 Что умеет?", url: LINK_SKILLS });
    if (LINK_PRICES) extra.push({ text:"💰 Цены", url: LINK_PRICES });
    if (LINK_FEEDBACK) extra.push({ text:"💬 Отзывы", url: LINK_FEEDBACK });
    if (LINK_ORDER) extra.push({ text:"🛒 Заказать", url: LINK_ORDER });
    if (extra.length) list.push(extra);
  }

  return list.length ? { inline_keyboard: list } : undefined;
}

// Telegram API
const TG = {
  async call(method, payload){
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=> ({}));
    if (!j.ok) throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
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
    try { await this.call("sendMessage", { chat_id: OWNER_ID, text }); } catch(_) {}
  }
};

// =============== ОСНОВНОЙ СКРИПТ ===============
async function main(){
  const csvPath = path.resolve("avtopost.csv");
  if (!fs.existsSync(csvPath)){
    await TG.notifyOwner("⚠️ Не найден файл avtopost.csv");
    return;
  }

  const { rows } = parseCSV(csvPath);
  const sent = readSent();
  const now = new Date();

  let posted = 0;

  // антидубль по времени
  const lastTs = sent.__last_post_ts || 0;
  if (lastTs && (now.getTime() - lastTs) < ANTI_DUP_MIN*60000){
    // слишком рано после предыдущего поста — пропускаем весь прогон
    return;
  }

  for (const row of rows){
    if (posted >= MAX_PER_RUN) break;

    const date = (row.date||"").trim();
    const time = (row.time||"").trim();
    const text = (row.text||"").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    // ключ дублирования: дата/время + sha1(медиассылок + текст)
    const mediaPart = `${row.photo_url||""}|${row.video_url||""}`;
    const key = `${date} ${time} ${sha1(mediaPart+"|"+text)}`;
    if (sent[key]) continue; // уже отправляли

    // антидубль: если есть запись на +-WINDOW_MIN/LAG_MIN с тем же хэшем, тоже скипаем
    const dayPrefix = `${date} ${time.split(":")[0]}`; // грубо по часу
    const similar = Object.keys(sent).some(k => k.includes(date) && k.endsWith(sha1(mediaPart+"|"+text)));
    if (similar) continue;

    const kb = buildInlineKeyboard(row);

    try{
      if (row.photo_url){
        const cap = text.length > 1000 ? (text.slice(0,1000)+"…") : text;
        await TG.sendPhoto(row.photo_url, cap, kb);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else if (row.video_url){
        const cap = text.length > 1000 ? (text.slice(0,1000)+"…") : text;
        await TG.sendVideo(row.video_url, cap, kb);
        if (text.length > 1000){
          await sleep(400);
          await TG.sendText(text.slice(1000), undefined);
        }
      } else {
        await TG.sendText(text, kb);
      }

      sent[key] = true;
      posted++;
      sent.__last_post_ts = now.getTime(); // отметим время
      writeSent(sent);

      // уведомление только по факту публикации
      await TG.notifyOwner(`✅ Опубликовано: 1 (окно +${WINDOW_MIN} / -${LAG_MIN} мин; антидубль ${ANTI_DUP_MIN} мин)`);
      await sleep(600); // немного подышим, если вдруг MAX_PER_RUN>1
    } catch(err){
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message)||err}`);
    }
  }

  // Ежедневный отчёт — ровно один раз после REPORT_HOUR
  const todayStrUTC = new Date().toISOString().slice(0,10); // дата (UTC, достаточно)
  const nowLocal = new Date(); // в TZ раннера (см. workflow -> TZ)
  if (nowLocal.getHours() >= REPORT_HOUR && sent.__report_date !== todayStrUTC){
    let totalToday=0, sentToday=0;
    for (const row of rows){
      const d = (row.date||"").trim();
      if (d === todayStrUTC){
        totalToday++;
        const media = `${row.photo_url||""}|${row.video_url||""}`;
        const k = `${row.date} ${row.time} ${sha1(media+"|"+(row.text||"").trim())}`;
        if (sent[k]) sentToday++;
      }
    }
    await TG.notifyOwner(
      `📅 Ежедневный отчёт (${todayStrUTC}):\n`+
      `Запланировано на сегодня: ${totalToday}\n`+
      `Фактически опубликовано: ${sentToday}`
    );
    sent.__report_date = todayStrUTC;
    writeSent(sent);
  }
}

// Запуск
main().catch(async (e) => {
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
