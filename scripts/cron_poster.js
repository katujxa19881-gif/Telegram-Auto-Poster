// scripts/cron_poster.js
// Node 18+ (встроенный fetch, FormData, Blob). ESM.

import fs from "fs";
import path from "path";
import crypto from "crypto";

// ================= ENV =================
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID; // -100xxxxxxxxxxx (НЕ @username)
const OWNER_ID = process.env.OWNER_ID || "";

const WINDOW_MIN = toInt(process.env.WINDOW_MIN ?? process.env.WINDOW_MINUTES, 30); // +вперёд, мин
const LAG_MIN = toInt(process.env.LAG_MIN, 10); // -назад, мин
const REPORT_HOUR = toInt(process.env.REPORT_HOUR, 21);

const ANTI_DUP_MIN = toInt(process.env.ANTI_DUP_MIN, 180); // пауза между публикациями
const X_PER_RUN = toInt(process.env.X_PER_RUN ?? process.env.MAX_PER_RUN, 1); // максимум за прогон

// публикуем ОТ ИМЕНИ КАНАЛА (важно для «Обсудить»)
const SENDER_CHAT_ID = process.env.SENDER_CHAT_ID || CHANNEL_ID;

// опционально, глобальные URL-кнопки
const LINK_SKILLS = process.env.LINK_SKILLS || "";
const LINK_PRICES = process.env.LINK_PRICES || "";
const LINK_FEEDBACK = process.env.LINK_FEEDBACK || "";
const LINK_ORDER = process.env.LINK_ORDER || "";

if (!BOT_TOKEN || !CHANNEL_ID) {
  console.error("Missing BOT_TOKEN or CHANNEL_ID env");
  process.exit(1);
}

// =============== helpers ===============
function toInt(v, d){ const n = parseInt(v ?? "", 10); return Number.isFinite(n) ? n : d; }
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function sha1(x){ return crypto.createHash("sha1").update(String(x)).digest("hex"); }

function toISOLocal(dateStr, timeStr){
  const [Y,M,D] = (dateStr||"").split("-").map(Number);
  const [h,m] = (timeStr||"").split(":").map(Number);
  return new Date(Y, (M||1)-1, D||1, h||0, m||0, 0, 0);
}

function withinWindow(when, now, windowMin, lagMin){
  const diffMin = (now - when) / 60000; // now - when
  return diffMin >= -lagMin && diffMin <= windowMin;
}

// --- Google Drive: конвертер ссылок ---
function convertDriveUrl(u) {
  if (!u) return "";
  const s = String(u).trim();

  // Уже прямая?
  if (/drive\.google\.com\/uc\b/i.test(s) && /[?&](id|export)=/i.test(s)) return s;

  let id = null;
  let m = s.match(/\/file\/d\/([^/]+)\//i); // /file/d/<ID>/view
  if (m) id = m[1];
  if (!id){ m = s.match(/[?&]id=([^&]+)/i); if (m) id = m[1]; } // ?id=<ID>
  if (!id){
    m = s.match(/drive\.google\.com\/(?:file\/d\/|u\/\d\/|thumbnail\?id=)?([a-zA-Z0-9_-]{10,})/i);
    if (m) id = m[1];
  }
  if (id) return `https://drive.google.com/uc?export=download&id=${id}`;
  return s;
}

// --- парсинг CSV (толстый, autodetect ,/;, кавычки, переносы) ---
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
  let s = "";
  try { s = fs.readFileSync(filePath, "utf8"); } catch { return { rows:[], sep:"," }; }
  s = s.replace(/^\uFEFF/, "").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  if (!s.trim()) return { rows:[], sep:"," };

  const sep = detectSepFromHeader(s);
  const recs = [];
  let row=[], field="", inQ=false;

  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (ch === '"'){
      if (inQ && s[i+1] === '"'){ field+='"'; i++; }
      else inQ = !inQ; continue;
    }
    if (!inQ && ch === sep){ row.push(field); field=""; continue; }
    if (!inQ && ch === "\n"){
      row.push(field); field="";
      if (row.some(c => String(c).trim()!=="")) recs.push(row);
      row=[]; continue;
    }
    field += ch;
  }
  if (field.length>0 || row.length>0){
    row.push(field);
    if (row.some(c => String(c).trim()!=="")) recs.push(row);
  }
  if (!recs.length) return { rows:[], sep };

  const headers = recs[0].map(h => String(h||"").trim());
  const data = recs.slice(1);

  const rows = [];
  for (const rec of data){
    const o={};
    for (let i=0;i<headers.length;i++) o[headers[i]] = (rec[i] ?? "").toString();

    if (!o.photo_url && o.photo) o.photo_url = o.photo;
    if (!o.video_url && o.video) o.video_url = o.video;

    if (o.photo_url) o.photo_url = convertDriveUrl(o.photo_url);
    if (o.video_url) o.video_url = convertDriveUrl(o.video_url);

    if (o.text) o.text = o.text.replace(/\\n/g,"\n");

    if (Object.values(o).some(v => String(v).trim()!=="")) rows.push(o);
  }
  return { rows, sep };
}

// --- клавиатура (кастомные + глобальные) ---
function buildInlineKeyboard(row){
  const list = [];
  for (let i=1;i<=4;i++){
    const t=(row[`btn${i}_text`]||"").trim();
    const u=(row[`btn${i}_url`] ||"").trim();
    if (t && u) list.push([{ text:t, url:u }]);
  }
  const extra=[];
  if (LINK_SKILLS) extra.push({ text:"🧠 Что умеет?", url:LINK_SKILLS });
  if (LINK_PRICES) extra.push({ text:"💰 Цены", url:LINK_PRICES });
  if (LINK_FEEDBACK) extra.push({ text:"💬 Отзывы", url:LINK_FEEDBACK });
  if (LINK_ORDER) extra.push({ text:"🛒 Заказать", url:LINK_ORDER });
  if (extra.length) list.push(extra);
  return list.length ? { inline_keyboard:list } : undefined;
}

// ============== Telegram API (с fallback-аплоадом фото) ==============
async function tgCall(method, body, isForm = false){
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const res = await fetch(url, {
    method: "POST",
    headers: isForm ? undefined : { "Content-Type": "application/json" },
    body: isForm ? body : JSON.stringify(body)
  });
  const j = await res.json().catch(()=> ({}));
  if (!j.ok) {
    throw new Error(`${method} failed: ${res.status} ${res.statusText} ${JSON.stringify(j)}`);
  }
  return j.result;
}

// пробуем обычный sendPhoto по URL → если Telegram не смог скачать, качаем сами и загружаем как файл
async function sendPhotoSmart(photoUrl, caption, reply_markup){
  try {
    return await tgCall("sendPhoto", {
      chat_id: CHANNEL_ID,
      sender_chat_id: SENDER_CHAT_ID,
      photo: photoUrl,
      caption,
      parse_mode: "HTML",
      allow_sending_without_reply: true,
      reply_markup
    });
  } catch (e) {
    const msg = String(e.message || e);
    const retry =
      /failed to get http url content/i.test(msg) ||
      /wrong type of the web page content/i.test(msg) ||
      /http url not found/i.test(msg);
    if (!retry) throw e;

    // fallback: сами скачиваем и грузим как multipart
    const { buffer, contentType, filename } = await downloadBinary(photoUrl);
    const fd = new FormData();
    fd.append("chat_id", CHANNEL_ID);
    fd.append("sender_chat_id", SENDER_CHAT_ID);
    if (caption) {
      fd.append("caption", caption);
      fd.append("parse_mode", "HTML");
    }
    if (reply_markup) fd.append("reply_markup", JSON.stringify(reply_markup));
    // имя файла обязательно надо передать, иначе телеграм может не распознать
    fd.append("photo", new Blob([buffer], { type: contentType || "application/octet-stream" }), filename || "photo.jpg");
    return await tgCall("sendPhoto", fd, true);
  }
}

async function sendVideoSmart(videoUrl, caption, reply_markup){
  // для видео оставим отправку по URL; при необходимости можно сделать аналогичный fallback
  return tgCall("sendVideo", {
    chat_id: CHANNEL_ID,
    sender_chat_id: SENDER_CHAT_ID,
    video: videoUrl,
    caption,
    parse_mode: "HTML",
    allow_sending_without_reply: true,
    reply_markup
  });
}

async function notifyOwner(text){
  if (!OWNER_ID) return;
  try { await tgCall("sendMessage", { chat_id: OWNER_ID, text }); } catch {}
}

// --- скачивание бинарника с учётом Google Drive (confirm-token) ---
async function downloadBinary(srcUrl){
  // нормализуем drive
  let url = convertDriveUrl(srcUrl);

  // 1-я попытка
  let res = await fetch(url, { redirect: "follow" });
  // если HTML — возможно защита Google (confirm)
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (res.ok && !ct.includes("text/html")) {
    const buf = new Uint8Array(await res.arrayBuffer());
    return { buffer: buf, contentType: ct, filename: guessFilename(url, ct) };
  }

  // пробуем вытащить подтверждающую ссылку из HTML Google Drive
  if (ct.includes("text/html")) {
    const html = await res.text().catch(()=> "");
    const m = html.match(/href="([^"]+confirm=[^"]+)"/i) || html.match(/id="uc-download-link".*?href="([^"]+)"/i);
    if (m) {
      // у Google ссылки относительные
      const confirmHref = m[1].replace(/&amp;/g, "&");
      const nextUrl = new URL(confirmHref, "https://drive.google.com").toString();
      // переносим cookies (если даны)
      const cookie = res.headers.get("set-cookie");
      res = await fetch(nextUrl, {
        redirect: "follow",
        headers: cookie ? { cookie } : undefined
      });
      const ct2 = (res.headers.get("content-type") || "").toLowerCase();
      if (res.ok && !ct2.includes("text/html")) {
        const buf = new Uint8Array(await res.arrayBuffer());
        return { buffer: buf, contentType: ct2, filename: guessFilename(url, ct2) };
      }
    }
  }

  // если всё равно HTML/ошибка — бросаем, пусть верхний слой покажет ошибку
  const txt = await res.text().catch(()=> "");
  throw new Error(`downloadBinary failed for ${url}: status=${res.status}, ct=${ct}, body_head=${txt.slice(0,200)}`);
}

function guessFilename(u, ct){
  try {
    const url = new URL(u);
    const base = path.basename(url.pathname) || "file";
    const safe = base.split("?")[0].replace(/[^\w.\-]+/g, "_");
    if (safe && safe.includes(".")) return safe;
  } catch {}
  // fallback по content-type
  if (ct && ct.includes("image/")) {
    const ext = ct.split("/")[1] || "jpg";
    return `photo.${ext}`;
  }
  return "file.bin";
}

// ============== sent.json ==============
const SENT_FILE = path.resolve("sent.json");
function readSent(){ try { return JSON.parse(fs.readFileSync(SENT_FILE,"utf8")); } catch { return {}; } }
function writeSent(o){ fs.writeFileSync(SENT_FILE, JSON.stringify(o, null, 2)); }
function makeKey(row){
  const media = `${row.photo_url||""}|${row.video_url||""}`;
  const text = (row.text||"").trim();
  return `${(row.date||"").trim()} ${(row.time||"").trim()} ${sha1(media+"|"+text)}`;
}

// ============== MAIN ==============
async function main(){
  const csvPath = path.resolve("avtopost.csv");
  if (!fs.existsSync(csvPath)){
    await notifyOwner("⚠️ avtopost.csv не найден в корне репозитория");
    return;
  }

  const { rows } = parseCSV(csvPath);
  const sent = readSent();
  const now = new Date();

  // антидубль по времени: если с последнего поста прошло меньше ANTI_DUP_MIN — пропускаем прогон
  const lastISO = sent.__last_post_at || "";
  if (lastISO) {
    const last = new Date(lastISO).getTime();
    if (Date.now() - last < ANTI_DUP_MIN * 60000) return;
  }

  let posted = 0;

  for (const row of rows){
    if (posted >= X_PER_RUN) break;

    const date = (row.date||"").trim();
    const time = (row.time||"").trim();
    const text = (row.text||"").trim();
    if (!date || !time || !text) continue;

    const when = toISOLocal(date, time);
    if (!withinWindow(when, now, WINDOW_MIN, LAG_MIN)) continue;

    const key = makeKey(row);
    if (sent[key]) continue;

    const kb = buildInlineKeyboard(row);

    try {
      if (row.photo_url) {
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await sendPhotoSmart(row.photo_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await tgCall("sendMessage", {
            chat_id: CHANNEL_ID,
            sender_chat_id: SENDER_CHAT_ID,
            text: text.slice(1000),
            parse_mode: "HTML",
            allow_sending_without_reply: true
          });
        }
      } else if (row.video_url) {
        const cap = text.length > 1000 ? text.slice(0,1000) + "…" : text;
        await sendVideoSmart(row.video_url, cap, kb);
        if (text.length > 1000) {
          await sleep(400);
          await tgCall("sendMessage", {
            chat_id: CHANNEL_ID,
            sender_chat_id: SENDER_CHAT_ID,
            text: text.slice(1000),
            parse_mode: "HTML",
            allow_sending_without_reply: true
          });
        }
      } else {
        await tgCall("sendMessage", {
          chat_id: CHANNEL_ID,
          sender_chat_id: SENDER_CHAT_ID,
          text,
          parse_mode: "HTML",
          disable_web_page_preview: false,
          allow_sending_without_reply: true,
          reply_markup: kb
        });
      }

      sent[key] = true;
      posted++;
      sent.__last_post_at = new Date().toISOString();
      writeSent(sent);

      await notifyOwner(`✅ Опубликовано: 1 (URL/fallback OK)`);
      await sleep(600);

    } catch (err) {
      await notifyOwner(`❌ Ошибка публикации: ${date} ${time}\n${(err && err.message) || err}`);
    }
  }

  // один вечерний отчёт
  const today = new Date().toISOString().slice(0,10);
  const nowLocal = new Date();
  if (nowLocal.getHours() >= REPORT_HOUR && sent.__report_date !== today){
    let plan=0, fact=0;
    for (const r of rows){
      if ((r.date||"").trim() === today){
        plan++;
        if (sent[ makeKey(r) ]) fact++;
      }
    }
    await notifyOwner(`📅 Отчёт (${today}):\nЗапланировано: ${plan}\nОпубликовано: ${fact}`);
    sent.__report_date = today;
    writeSent(sent);
  }
}

main().catch(async (e)=>{
  console.error(e);
  await notifyOwner(`❌ Скрипт упал: ${e.message || e}`);
  process.exit(1);
});
