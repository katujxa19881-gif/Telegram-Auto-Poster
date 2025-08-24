# Telegram Auto Poster (GitHub Actions)

Этот репозиторий публикует посты в Telegram **без серверов и Replit**.
Каждые 5 минут GitHub Actions читает `avtopost.csv`, конвертирует Google Drive ссылки и отправляет посты в канал.

## Как запустить (3 шага)

1) **Секреты** (Settings → Secrets and variables → Actions):
   - `BOT_TOKEN` = токен бота
   - `CHANNEL_ID` = @neiro_ai_sale (или -100...)
   - `OWNER_ID` = 1091395234 (чтобы получать отчёты в личку, опционально)

2) **Файлы** (как в этом архиве):
   - `.github/workflows/cron-poster.yml`
   - `scripts/cron_poster.js`
   - `avtopost.csv`
   - `sent.json` (оставьте пустым `[]`)

3) **Запуск**:
   - Вкладка **Actions** → выберите `Telegram Auto Poster` → **Run workflow** (ручной запуск)
   - Потом он будет сам запускаться каждые 5 минут (UTC).

## avtopost.csv
Заголовки: `date,time,text,channel_id,photo_url,video_url`
- `channel_id` можно оставить пустым → будет взят из секрета `CHANNEL_ID`.
- Можно использовать «сырые» drive-ссылки `/file/d/.../view` — скрипт их сам конвертирует.

Пример строки:
```
2025-08-25,12:00,"Пост с фото",@neiro_ai_sale,https://drive.google.com/file/d/FILE_ID/view?usp=sharing,
```

## Догонялка
Скрипт публикует посты, у которых время попадает в окно `[now - WINDOW_MINUTES; now]`.
По умолчанию 12 минут (можно поменять в workflow).

## Антидубли
Отправленные посты фиксируются в `sent.json`. Он коммитится обратно в репозиторий.
Чтобы отправить ту же строку повторно — очистите `sent.json` (оставьте `[]`).

