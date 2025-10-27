import os
import logging
import random
import time
import re
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# إعدادات البوت
BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not found")

# قائمة النصوص الجديدة (10 كلمات مع الرمز ≈)
TEXTS = [
    "شمس ≈ تشرق ≈ في ≈ الصباح ≈ الجميل ≈ فوق ≈ الجبال ≈ الخضراء ≈ تملأ ≈ الدنيا",
    "كتاب ≈ مفتاح ≈ المعرفة ≈ والعلم ≈ ينير ≈ العقول ≈ ويوسع ≈ الآفاق ≈ نحو ≈ المستقبل",
    "طفل ≈ يلعب ≈ في ≈ الحديقة ≈ الكبيرة ≈ مع ≈ أصدقائه ≈ بسعادة ≈ وفرح ≈ غامر",
    "سفر ≈ يجلب ≈ تجارب ≈ جديدة ≈ وذكريات ≈ جميلة ≈ تبقى ≈ في ≈ القلب ≈ للأبد",
    "قهوة ≈ صباحية ≈ تمنح ≈ الطاقة ≈ والنشاط ≈ لبداية ≈ يوم ≈ مليء ≈ بالإنجازات ≈ والعمل"
]

# قائمة الكلمات للتكرار
REPEAT_WORDS = [
    "في", "كان", "كيف", "من", "طير", "كسر", "خشب", "طوب", "بيت", "سين",
    "عين", "جيم", "كتب", "خبر", "حلم", "جمل", "تعب", "حسد", "نار", "برد"
]

# قاموس لتتبع حالة كل مستخدم
user_data = {}
used_texts = {}
used_repeat_patterns = {}

CHARACTER_MAPPING = {
    'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه',
    'ئ': 'ي', 'ؤ': 'و', 'ٱ': 'ا', 'ٳ': 'ا'
}

def normalize_text(text):
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    normalized_chars = []
    for char in text:
        normalized_char = CHARACTER_MAPPING.get(char, char)
        normalized_chars.append(normalized_char)
    normalized_text = ''.join(normalized_chars)
    normalized_text = re.sub(r'\s+', ' ', normalized_text).strip()
    return normalized_text

def texts_match(original, user_input):
    original_normalized = normalize_text(original)
    user_normalized = normalize_text(user_input)
    return original_normalized == user_normalized

def get_unique_text_for_user(user_id):
    if user_id not in used_texts:
        used_texts[user_id] = set()
    available_texts = [text for text in TEXTS if text not in used_texts[user_id]]
    if not available_texts:
        used_texts[user_id] = set()
        available_texts = TEXTS.copy()
    selected_text = random.choice(available_texts)
    used_texts[user_id].add(selected_text)
    return selected_text

def generate_unique_repeat_pattern(user_id):
    if user_id not in used_repeat_patterns:
        used_repeat_patterns[user_id] = set()
    
    for attempt in range(100):
        selected_words = random.sample(REPEAT_WORDS, 4)
        repeat_pattern_parts = []
        pattern_signature = []
        
        for word in selected_words:
            repeat_count = random.randint(2, 4)
            repeat_pattern_parts.append(f"{word}({repeat_count})")
            pattern_signature.append(f"{word}_{repeat_count}")
        
        pattern_key = "|".join(pattern_signature)
        
        if pattern_key not in used_repeat_patterns[user_id]:
            used_repeat_patterns[user_id].add(pattern_key)
            return " ".join(repeat_pattern_parts)
    
    used_repeat_patterns[user_id] = set()
    return generate_unique_repeat_pattern(user_id)

def validate_repeat_text(expected_text, user_input):
    pattern = r'(\S+)\((\d+)\)'
    expected_matches = re.findall(pattern, expected_text)
    input_words = user_input.split()
    
    total_expected_words = sum(int(count) for word, count in expected_matches)
    if len(input_words) != total_expected_words:
        return False, f"عدد الكلمات غير صحيح"
    
    current_index = 0
    for word, count in expected_matches:
        expected_word = normalize_text(word)
        repeat_count = int(count)
        
        for i in range(current_index, current_index + repeat_count):
            if i >= len(input_words):
                return False, "عدد الكلمات غير متطابق"
            
            input_word = normalize_text(input_words[i])
            if input_word != expected_word:
                return False, f"الكلمة '{input_words[i]}' يجب أن تكون '{word}'"
        
        current_index += repeat_count
    
    return True, ""

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = """
مرحباً بك في بوت اختبار سرعة الكتابة! ✨

📝 **كيفية الاستخدام:**
• اكتب `11223` لبدء اختبار الجمل
• اكتب `22334` لبدء اختبار التكرار
• اكتب `عرض` لعرض هذه التعليمات مرة أخرى
"""
    await update.message.reply_text(welcome_text)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    message_text = update.message.text
    
    if message_text == '11223':
        unique_text = get_unique_text_for_user(user_id)
        user_data[user_id] = {
            'type': 'text',
            'original_text': unique_text,
            'start_time': time.time()
        }
        await update.message.reply_text(f'{unique_text}')
    
    elif message_text == '22334':
        repeat_text = generate_unique_repeat_pattern(user_id)
        user_data[user_id] = {
            'type': 'repeat',
            'original_text': repeat_text,
            'start_time': time.time()
        }
        await update.message.reply_text(f'{repeat_text}')
    
    elif message_text == 'عرض':
        await start_command(update, context)
    
    elif user_id in user_data and 'original_text' in user_data[user_id]:
        original_text = user_data[user_id]['original_text']
        start_time = user_data[user_id]['start_time']
        end_time = time.time()
        test_type = user_data[user_id]['type']
        
        if test_type == 'text':
            if texts_match(original_text, message_text):
                time_taken = end_time - start_time
                words = len(original_text.split())
                wpm = (words / time_taken) * 60
                response = f"وحش ذي سرعتك {wpm:.2f} WPM"
                await update.message.reply_text(response)
                del user_data[user_id]
        
        else:
            is_valid, error_message = validate_repeat_text(original_text, message_text)
            if is_valid:
                time_taken = end_time - start_time
                words = len(message_text.split())
                wpm = (words / time_taken) * 60
                response = f"وحش ذي سرعتك {wpm:.2f} WPM"
                await update.message.reply_text(response)
                del user_data[user_id]

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f'حدث خطأ: {context.error}')

def main():
    try:
        print("🚀 بدء تشغيل البوت...")
        application = Application.builder().token(BOT_TOKEN).build()
        
        application.add_handler(CommandHandler('start', start_command))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_error_handler(error_handler)
        
        print("✅ البوت يعمل الآن!")
        application.run_polling()
        
    except Exception as e:
        print(f"❌ خطأ في تشغيل البوت: {e}")

if __name__ == '__main__':
    main()
