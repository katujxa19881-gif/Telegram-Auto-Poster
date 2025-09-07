// scripts/cron_poster.js
// Node 18+ (fetch встроен)

import fs from "fs";
import path from "path";

// ========= ENV =========
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100xxxxxxxxxxx (НЕ @username)
const OWNER_ID = process.env.OWNER_ID || ""; // ЛС владельцу (опционально)
const DEBUG = (process.env.DEBUG || "").toLowerCase() === "true";

const WINDOW_MIN = parseInt(process.env.WINDOW_MIN || "30", 10); // окно вперёд (мин)
const LAG_MIN = parseInt(process.env.LAG_MIN || "10", 10); // лаг назад (мин)
const REPORT_HOUR = parseInt(process.env.REPORT_HOUR || "21", 10); // разовый вечерний отчёт

// антидубль / защита от «спама»
const MAX_PER_RUN = parseInt(process.env.MAX_PER_RUN || "1", 10); // не более N постов за один прогон
const COOL_DOWN_MIN = parseInt(process.env.COOL_DOWN_MIN || "15", 10); // пауза между постами (мин)

// публикуем от имени канала (можно переопределить секретом SENDER_CHAT_ID)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// ========= Утилиты =========
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

function convertDriveUrl(u){
  if (!u) return "";
  try {
    const url = new URL(u.trim());
    if (url.hostname.includes("drive.google.com")) {
      const m = url.pathname.match(/\/file\/d\/([^/]+)/);
      if (m) return `https://drive.google.com/uc?export=download&id=${m[1]}`;
    }
  } catch(_) {}
  return u.trim();
}

function toISOLocal(dateStr, timeStr) {
  const [Y,M,D] = dateStr.split("-").map(Number);
  const [h,m] = timeStr.split(":").map(Number);
  return new Date(Y, (M||1)-1, D, h||0, m||0);
}

// now ∈ [when - LAG_MIN ; when + WINDOW_MIN]
function withinWindow(when, now, windowMin, lagMin){
  const diffMin = (when.getTime() - now.getTime())/60000;
  return diffMin >= -Math.abs(lagMin) && diffMin <= Math.abs(windowMin);
}

// ========= «Толстый» CSV-парсер (авто-разделитель, кавычки, переносы) =========
function detectSepFromHeader(src){
  let inQ=false, commas=0, semis=0;
  for (let i=0;i<src.length;i++){
    const ch = src[i];
    if (ch === '"'){
      if (inQ && src[i+1] === '"'){ i++; }
      else inQ = !inQ;
    } else if (!inQ && ch === "\n"){ break; }
    else if (!inQ && ch === ",") commas++;
    else if (!inQ && ch === ";") semis++;
  }
  return semis > commas ? ";" : ",";
}

function parseCSV(filePath){
  let s = fs.readFileSync(filePath, "utf8");
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return {rows:[], sep:","};

  const sep = detectSepFromHeader(s);

  const rowsRaw=[], row=[], headers=[];
  let field="", inQ=false, atHeader=true;

  const pushRow = (arr) => {
    if (arr.length && arr.some(v => String(v).trim() !== "")) rowsRaw.push([...arr]);
  };

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
      pushRow(row); row.length=0; continue;
    }
    field += ch;
  }
  if (field.length || row.length){ row.push(field); pushRow(row); }

  if (!rowsRaw.length) return {rows:[], sep};

  rowsRaw[0].forEach(h => headers.push(String(h||"").trim()));
  const data = rowsRaw.slice(1);

  const rows = data.map(rec => {
    const o={};
    for (let i=0;i<headers.length;i++) o[headers[i]] = (rec[i] ?? "").toString();
    if (!o.photo_url && o.photo) o.photo_url = o.photo;
    if (!o.video_url && o.video) o.video_url = o.video;

    if (o.photo_url) o.photo_url = convertDriveUrl(o.photo_url);
    if (o.video_url) o.video_url = convertDriveUrl(o.video_url);

    if (o.text) o.text = o.text.replace(/\\n/g,"\n");
    return o;
  }).filter(o => Object.values(o).some(v => String(v).trim() !== ""));

  return {rows, sep};
}

// ========= Кнопки =========
function buildInlineKeyboard(row){
  const kb=[];
  for (let i=1;i<=4;i++){
    const t=(row[`btn${i}_text`]||"").trim();
    const u=(row[`btn${i}_url`] ||"").trim();
    if (t && u) kb.push([{text:t, url:u}]);
  }
  return kb.length ? {inline_keyboard:kb} : undefined;
}

// ========= Telegram API =========
const TG = {
  async call(m,p){
    const url=`https://api.telegram.org/bot${BOT_TOKEN}/${m}`;
    const res = await fetch(url,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify(p)
    });
    const j = await res.json().catch(()=>({}));
    if (!j.ok) throw new Error(`${m} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
    return j.result;
  },
  async sendText(text, reply_markup){
    return this.call("sendMessage",{
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      text, parse_mode:"HTML",
      disable_web_page_preview:false,
      allow_sending_without_reply:true,
      reply_markup
    });
  },
  async sendPhoto(photo, caption, reply_markup){
    return this.call("sendPhoto",{
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      photo, caption,
      parse_mode:"HTML",
      allow_sending_without_reply:true,
      reply_markup
    });
  },
  async sendVideo(video, caption, reply_markup){
    return this.call("sendVideo",{
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      video, caption,
      parse_mode:"HTML",
      allow_sending_without_reply:true,
      reply_markup
    });
  },
  async notifyOwner(text){
    if (!OWNER_ID) return;
    try { await this.call("sendMessage",{chat_id:OWNER_ID, text}); } catch(_){}
  }
};

// ========= sent.json (лог) =========
const SENT_FILE = path.resolve("sent.json");
function readSent(){
  try { return JSON.parse(fs.readFileSync(SENT_FILE,"utf8")); }
  catch { return {}; }
}
function writeSent(x){
  fs.writeFileSync(SENT_FILE, JSON.stringify(x, null, 2));
}
function getMeta(sent){
  if (!sent.__meta) sent.__meta = { last_post_ts: 0, last_report_date: "" };
  if (typeof sent.__meta.last_post_ts !== "number") sent.__meta.last_post_ts = 0;
  if (typeof sent.__meta.last_report_date !== "string") sent.__meta.last_report_date = "";
  return sent.__meta;
}

// ========= MAIN =========
async function main(){
  const {rows} = parseCSV(path.resolve("avtopost.csv"));
  const sent = readSent();
  const meta = getMeta(sent);
  const now = new Date();

  let posted = 0;

  // 1) Обычная публикация в окно [-LAG_MIN; +WINDOW_MIN]
  for (const row of rows){
    const date = (row.date || "").trim();
    const time = (row.time || "").trim();
    const text = (row.text || "").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = `${date} ${time} ${(row.photo_url||"")}${(row.video_url||"")}`;
    if (sent[key]) continue; // уже отправляли

    // ---- Лимит «не больше N за прогон»
    if (posted >= MAX_PER_RUN) break;

    // ---- Кулдаун между публикациями
    if (meta.last_post_ts){
      const mins = (Date.now() - meta.last_post_ts)/60000;
      if (mins < COOL_DOWN_MIN) {
        // рано – дадим возможности следующему прогону
        continue;
      }
    }

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
      posted++;
      meta.last_post_ts = Date.now();

      // уведомление только по факту публикации
      await TG.notifyOwner(`✅ Опубликовано: 1 (окно +${WINDOW_MIN} / -${LAG_MIN} мин; антидубль ${COOL_DOWN_MIN} мин; лимит ${MAX_PER_RUN})`);

      await sleep(600); // небольшая пауза, на всякий

    } catch(err){
      await TG.notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }

    if (posted >= MAX_PER_RUN) break;
  }

  // 2) Разовый вечерний отчёт (один раз в день)
  const todayIso = new Date().toISOString().slice(0,10);
  const nowLocal = new Date();
  if (nowLocal.getHours() >= REPORT_HOUR && meta.last_report_date !== todayIso){
    let totalToday = 0, sentToday = 0;
    for (const r of rows){
      const d=(r.date||"").trim();
      if (d === todayIso){
        totalToday++;
        const k = `${r.date} ${r.time} ${(r.photo_url||"")}${(r.video_url||"")}`;
        if (sent[k]) sentToday++;
      }
    }
    await TG.notifyOwner(
      `🗓 Ежедневный отчёт (${todayIso}):\n`+
      `Запланировано на сегодня: ${totalToday}\n`+
      `Фактически опубликовано: ${sentToday}`
    );
    meta.last_report_date = todayIso;
  }

  writeSent(sent);
  // мгновенное уведомление, если публиковали
if (posted > 0) {
  await TG.notifyOwner(`✅ Опубликовано: ${posted} (окно ${WINDOW_MIN} мин)`);
}

// если ничего не опубликовали — скажем почему (только при DEBUG)
if (posted === 0 && DEBUG) {
  // краткая раскладка за этот прогон
  await TG.notifyOwner(
    `⚠️ Публикаций нет.\n` +
    `Сегодняшних строк: ${csv.rows.filter(r => (r.date||"").trim() === new Date().toISOString().slice(0,10)).length}\n` +
    `Окно: +${WINDOW_MIN} / -${LAG_MIN} мин\n` +
    `Лимит: max_per_run=${MAX_PER_RUN}, cooldown=${COOL_DOWN_MIN} мин, anti-dup=${ANTI_DUP_MIN} мин`
  );
}
}
  
main().catch(async (e)=>{
  console.error(e);
  await TG.notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
