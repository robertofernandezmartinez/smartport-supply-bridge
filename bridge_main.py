import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Environment and Global Configuration
load_dotenv()
processed_updates = set()

def get_bridge_data():
    """Connects to Google Sheets and retrieves data from the three primary tabs."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(spreadsheet_id)
        
        # Pulling data from the specific worksheets
        vessels = doc.worksheet("risk_alerts").get_all_records()
        predictions = doc.worksheet("stockout_predictions").get_all_records()
        mapping = doc.worksheet("supply_chain_map").get_all_records()
        
        return vessels, predictions, mapping
    except Exception as e:
        print(f"❌ Database Access Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Business logic to detect overlaps between maritime delays and inventory risk."""
    conflicts = []
    # Filtering for risk_score > 75 (representing the 7-14 day logistical shift)
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        ship_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        category = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == ship_id), None)
        
        if category:
            stock_risk = next((p for p in predictions if p['category'] == category), None)
            # Match: Severe delay + Stockout predicted within 14 days
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"vessel": ship_id, "category": category})
    return conflicts

# --- PROACTIVE RANKING BROADCAST ---
async def send_proactive_ranking(app_instance):
    """Triggers the immediate executive report on startup or scheduled intervals."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if conflicts and chat_id:
            ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = (
                f"Data Context: {json.dumps(conflicts)}\n\n"
                "Task: Generate a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
                "1. Rank categories by risk severity (ship volume).\n"
                "2. Detail the impact of the 7-14 day delay on stock availability.\n"
                "3. Format: Bold headers, no '#' characters, mobile-optimized."
            )
            
            completion = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "You are a Supply Chain Analyst."},
                          {"role": "user", "content": prompt}]
            )
            # Use app_instance.bot directly if calling manually, or context.bot if via JobQueue
            target_bot = app_instance.bot if hasattr(app_instance, 'bot') else app_instance
            await target_bot.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
            print("✅ Initial analysis sent to Telegram.")
    except Exception as e:
        print(f"Broadcast Failure: {e}")

# --- INTERACTIVE AI ANALYST ---
async def handle_bridge_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global processed_updates
    if not update.message or update.message.message_id in processed_updates:
        return
    processed_updates.add(update.message.message_id)

    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        system_instruction = (
            "You are an interactive Supply Chain Strategist. Use the live dataset below.\n"
            f"Dataset: {json.dumps(conflicts)}\n"
            "Respond in the user's language. Be concise and professional.\n"
            "Format: Use Bold for headers, avoid '#' symbols."
        )
        
        completion = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_instruction},
                      {"role": "user", "content": update.message.text}]
        )
        await update.message.reply_text(completion.choices[0].message.content, parse_mode='Markdown')
    except Exception as e:
        print(f"Interaction Error: {e}")

if __name__ == '__main__':
    print("🚀 Initializing SmartPort-Bridge Deployment...")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # 1. MANUAL STARTUP TRIGGER: Ensures the bot 'wakes up' immediately
        print("📊 Launching mandatory initial analysis...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_proactive_ranking(app))
        
        # 2. JOB QUEUE: Set to repeat every hour (3600 seconds)
        if app.job_queue:
            app.job_queue.run_repeating(lambda ctx: send_proactive_ranking(ctx.bot), interval=3600)
        
        # 3. MESSAGE HANDLER: Listening for questions
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        
        print("📡 Bot is ONLINE and interactive.")
        app.run_polling(drop_pending_updates=True)