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
1. "videos": [id, creator_id, views_count, video_created_at] - текущие данные.
2. "video_snapshots": [id, video_id, delta_views_count, created_at] - прирост.

ЗОЛОТЫЕ ПРАВИЛА (ВСЕГДА ИСПОЛЬЗУЙ COUNT ИЛИ SUM ДЛЯ ПОЛУЧЕНИЯ ОДНОГО ЧИСЛА):
- "Сколько видео в системе" -> SELECT COUNT(*) FROM videos;
- "Видео набрало больше X" -> SELECT COUNT(*) FROM videos WHERE views_count > X;
- "Прирост/Выросли за [Дата]" -> SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '[Дата]';
- "Разные/Уникальные видео" -> SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0 AND created_at::date = '[Дата]';
- "Сколько видео у автора [ID] за период" -> SELECT COUNT(*) FROM videos WHERE creator_id = '[ID]' AND video_created_at::date >= '[Дата1]' AND video_created_at::date <= '[Дата2]';

ТРЕБОВАНИЯ:
- Итоговый запрос должен возвращать только ОДНО число (используй COUNT или SUM).
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


# @dp.message(F.text)
# async def handle_message(message: types.Message):
#     await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
#     try:
#         sql_query = await sql_from_natural_language(message.text)
#         print(f"\n--- НОВЫЙ ЗАПРОС ---")
#         print(f"ВОПРОС: {message.text}")
#         print(f"SQL ОТ ИИ: {sql_query}")
#         print(f"--------------------\n")

#         result = await fetch_result(sql_query)
#         await message.answer(str(result))
        
#     except Exception as e:
#         print(f"Error: {e}")
#         await message.answer("0")