import os
import random
import time
import re
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found")

# بيانات البوت
TEXTS = [
    "شمس ≈ تشرق ≈ في ≈ الصباح ≈ الجميل ≈ فوق ≈ الجبال ≈ الخضراء ≈ تملأ ≈ الدنيا",
    "كتاب ≈ مفتاح ≈ المعرفة ≈ والعلم ≈ ينير ≈ العقول ≈ ويوسع ≈ الآفاق ≈ نحو ≈ المستقبل"
]

REPEAT_WORDS = ["في", "كان", "كيف", "من", "طير", "كسر", "خشب", "طوب", "بيت", "سين"]

user_data = {}

CHARACTER_MAPPING = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه'}

def normalize_text(text):
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    normalized_chars = []
    for char in text:
        normalized_char = CHARACTER_MAPPING.get(char, char)
        normalized_chars.append(normalized_char)
    return ''.join(normalized_chars)

def texts_match(original, user_input):
    return normalize_text(original) == normalize_text(user_input)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("مرحباً! اكتب 11223 للجمل أو 22334 للتكرار")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    message_text = update.message.text
    
    if message_text == '11223':
        text = random.choice(TEXTS)
        user_data[user_id] = {'type': 'text', 'text': text, 'time': time.time()}
        await update.message.reply_text(text)
    
    elif message_text == '22334':
        words = random.sample(REPEAT_WORDS, 4)
        pattern = " ".join([f"{w}({random.randint(2,4)})" for w in words])
        user_data[user_id] = {'type': 'repeat', 'text': pattern, 'time': time.time()}
        await update.message.reply_text(pattern)
    
    elif user_id in user_data:
        data = user_data[user_id]
        if texts_match(data['text'], message_text):
            time_taken = time.time() - data['time']
            words = len(message_text.split())
            wpm = (words / time_taken) * 60
            await update.message.reply_text(f"وحش ذي سرعتك {wpm:.2f} WPM")
            del user_data[user_id]

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler('start', start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.run_polling()

if __name__ == '__main__':
    main()
