import os
import random
import time
import re
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found")

TEXTS = [
    "شمس ≈ تشرق ≈ في ≈ الصباح ≈ الجميل ≈ فوق ≈ الجبال ≈ الخضراء ≈ تملأ ≈ الدنيا",
    "كتاب ≈ مفتاح ≈ المعرفة ≈ والعلم ≈ ينير ≈ العقول ≈ ويوسع ≈ الآفاق ≈ نحو ≈ المستقبل"
]

REPEAT_WORDS = ["في", "كان", "كيف", "من", "طير", "كسر", "خشب", "طوب", "بيت", "سين"]
user_data = {}

def normalize_text(text):
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    mapping = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه'}
    result = []
    for char in text:
        result.append(mapping.get(char, char))
    return ''.join(result)

def start(update, context):
    update.message.reply_text("مرحباً! اكتب 11223 للجمل أو 22334 للتكرار")

def handle_message(update, context):
    user_id = update.message.from_user.id
    text = update.message.text
    
    if text == '11223':
        msg = random.choice(TEXTS)
        user_data[user_id] = {'type': 'text', 'text': msg, 'time': time.time()}
        update.message.reply_text(msg)
    
    elif text == '22334':
        words = random.sample(REPEAT_WORDS, 4)
        pattern = " ".join([f"{w}({random.randint(2,4)})" for w in words])
        user_data[user_id] = {'type': 'repeat', 'text': pattern, 'time': time.time()}
        update.message.reply_text(pattern)
    
    elif user_id in user_data:
        data = user_data[user_id]
        if normalize_text(data['text']) == normalize_text(text):
            time_taken = time.time() - data['time']
            words_count = len(text.split())
            wpm = (words_count / time_taken) * 60
            update.message.reply_text(f"وحش ذي سرعتك {wpm:.2f} WPM")
            del user_data[user_id]

def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text, handle_message))
    
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
