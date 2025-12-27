import os
import asyncio
from dotenv import load_dotenv
from ollama import AsyncClient
from aiogram import Bot, Dispatcher, types, F
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


load_dotenv()

BASE_PROMPT = """
Ты — эксперт по SQL. Твоя задача: на основе вопроса выдать ОДИН SQL-запрос для PostgreSQL.

ТАБЛИЦЫ:
"videos": [id, creator_id, views_count, video_created_at] - текущие данные.
"video_snapshots": [id, video_id, delta_views_count, created_at] - данные о приросте.

ЗОЛОТЫЕ ПРАВИЛА (ИТОГОМ ВСЕГДА ДОЛЖНО БЫТЬ ОДНО ЧИСЛО):
- "Сколько видео" -> SELECT COUNT(*) FROM videos;
- "Прирост/Выросли/Набрали просмотров" -> SELECT SUM(delta_views_count) FROM video_snapshots;
- "Уникальные/Разные видео" -> SELECT COUNT(DISTINCT video_id) FROM video_snapshots;
- "Сколько видео набрали/выросли более чем на X" -> SELECT COUNT(*) FROM (
SELECT video_id 
FROM video_snapshots 
JOIN videos v ON vs.video_id = v.id 
WHERE v.creator_id = 'aca1061a-9d32-4ecf-8c3f-a2bb32d7be63' 
GROUP BY video_id HAVING SUM(delta_views_count) > X) as sub;

-  "Сколько разных креаторов имеют хотя бы одно видео, которое в итоге набрало больше 100 000 просмотров" -> SELECT COUNT(DISTINCT creator_id) 
FROM videos 
WHERE views_count > 100000;


- "Замеры, в которых просмотров стало меньше/отрицательный рост" -> 
SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;


ПРАВИЛА ДЛЯ ДАТЫ И ВРЕМЕНИ (UTC):
- Всегда используй (created_at AT TIME ZONE 'UTC').
- Фильтр по дате: (created_at AT TIME ZONE 'UTC')::date = 'YYYY-MM-DD'.
- Фильтр по времени: (created_at AT TIME ZONE 'UTC')::time >= 'HH:MM:SS' И (created_at AT TIME ZONE 'UTC')::time <= 'HH:MM:SS'.
- Если указан месяц: video_created_at >= '2025-06-01' AND video_created_at < '2025-07-01'.

ПРАВИЛО МЕСЯЦА: 
- Если указан месяц (например, июнь 2025), ЗАПРЕЩЕНО использовать ::date =. Используй СТРОГО интервал: video_created_at >= '2025-06-01' AND video_created_at < '2025-07-01'.

ПРАВИЛО СУММЫ: 
- Для вопроса "сколько набрали видео, опубликованные в..." — используй SUM(views_count) из таблицы videos.

ШАБЛОНЫ:
- "Прирост автора [ID] за [Дата]":
SELECT SUM(vs.delta_views_count)
FROM video_snapshots vs
JOIN videos v ON vs.video_id = v.id
WHERE v.creator_id = '[ID]' AND (vs.created_at AT TIME ZONE 'UTC')::date = '[Дата]';


- "Сколько видео опубликовал автор [ID] за период [Дата1]-[Дата2]":
  SELECT COUNT(*) 
  FROM videos 
  WHERE creator_id = '[ID]' 
    AND (video_created_at AT TIME ZONE 'UTC')::date >= '[Дата1]' 
    AND (video_created_at AT TIME ZONE 'UTC')::date <= '[Дата2]';


- "Прирост автора [ID] в интервале времени":
SELECT SUM(vs.delta_views_count)
FROM video_snapshots vs
JOIN videos v ON vs.video_id = v.id
WHERE v.creator_id = '[ID]'
AND (vs.created_at AT TIME ZONE 'UTC')::date = '2025-11-28'
AND (vs.created_at AT TIME ZONE 'UTC')::time >= '10:00:00'
AND (vs.created_at AT TIME ZONE 'UTC')::time <= '15:00:00';

- "Суммарные просмотры видео, ОПУБЛИКОВАННЫХ в [Период]" -> 
SELECT SUM(views_count) FROM videos WHERE video_created_at >= '[Начало]' AND video_created_at < '[Конец_следующего_месяца]';


ТРЕБОВАНИЯ:
- ПРАВИЛО "ЧИСТОГО ЛИСТА": Для каждого нового вопроса игнорируй данные (ID, даты, интервалы) из шаблонов. Используй ТОЛЬКО те значения, которые указаны в текущем вопросе.
- БРИТВА ОККАМА: Не используй JOIN и подзапросы, если вся необходимая информация есть в одной таблице.
- ЗАПРЕТ ГАЛЛЮЦИНАЦИЙ: Если в вопросе не указан конкретный автор (ID), не добавляй фильтр по creator_id. Если не указано время, не добавляй фильтр по часам.
- СТРОГОЕ СООТВЕТСТВИЕ: Если вопрос звучит "Сколько всего...", это означает запрос по всей таблице без фильтров по конкретным сущностям, если они не упомянуты.

- Итоговый запрос должен возвращать только ОДНО число.
- Агрегатные функции (SUM, COUNT) в WHERE не использовать, только в HAVING через подзапрос.
- UUID всегда в одинарных кавычках.
- Выводи ТОЛЬКО SQL код без пояснений.
"""

# ------------------- Настройки -------------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_URL = os.getenv("DB_URL")
MODEL_NAME = os.getenv("MODEL_NAME")

if not TOKEN:
    exit("Ошибка: TELEGRAM_TOKEN не найден в .env файле")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ------------------- SQLAlchemy -------------------
engine = create_async_engine(DB_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# ------------------- Генерация SQL через Ollama -------------------
async def sql_from_natural_language(user_text: str) -> str:
    response = await AsyncClient().chat(model=MODEL_NAME, messages=[
            {'role': 'system', 'content': BASE_PROMPT},
            {'role': 'user', 'content': user_text},
        ],
        options={
            "temperature": 0
        }
    )
    
    content = response['message']['content'].strip()
    
    if "```" in content:
        # Извлекаем текст только внутри блоков ```sql или ```
        import re
        sql_match = re.search(r'```(?:sql)?\s*(.*?)\s*```', content, re.DOTALL)
        if sql_match:
            query = sql_match.group(1)
        else:
            query = content.replace('```sql', '').replace('```', '')
    else:
        query = content

    return query.strip().rstrip(';')


# ------------------- Выполнение SQL -------------------
async def fetch_result(sql: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(sql))
        # scalar() возвращает первое значение (число)
        val = result.scalar()
        return val if val is not None else 0

# ------------------- Обработчик сообщений -------------------
@dp.message(F.text == "/start")
async def cmd_start(message: types.Message):
    await message.answer("Привет! Я ИИ-аналитик. Задавай вопросы по базе видео, и я отвечу числом.")

@dp.message(F.text)
async def handle_message(message: types.Message):
    if message.text.startswith("/"): return

    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    try:
        # 1. Генерируем SQL через Ollama
        sql_query = await sql_from_natural_language(message.text)
        
        # --- ВОТ ЭТА СТРОКА ДЛЯ ВАС ---
        print(f"\n--- НОВЫЙ ЗАПРОС ---")
        print(f"ВОПРОС: {message.text}")
        print(f"SQL ОТ ИИ: {sql_query}")
        print(f"--------------------\n")
        
        # 2. Выполняем в БД
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(sql_query))
            final_answer = result.scalar()
            
        # 3. Отправляем ответ
        await message.answer(str(final_answer if final_answer is not None else 0))
        
    except Exception as e:
        print(f"❌ ОШИБКА В SQL: {e}")
        await message.answer("0")


# ------------------- Запуск -------------------
async def main():
    print(f"🚀 Бот запущен! Модель: {MODEL_NAME}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")

