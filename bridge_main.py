import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Environment Loading
load_dotenv()
processed_updates = set()

def get_bridge_data():
    """Retrieves data from risk_alerts, stockout_predictions, and supply_chain_map."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(spreadsheet_id)
        vessels = doc.worksheet("risk_alerts").get_all_records()
        predictions = doc.worksheet("stockout_predictions").get_all_records()
        mapping = doc.worksheet("supply_chain_map").get_all_records()
        return vessels, predictions, mapping
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Core logic to identify stockout risks based on vessel delays (Score > 75)."""
    conflicts = []
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]
    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        category = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        if category:
            stock_risk = next((p for p in predictions if p['category'] == category), None)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"vessel": v_id, "category": category})
    return conflicts

# --- PROACTIVE RANKING SYSTEM ---
async def send_proactive_ranking(context: ContextTypes.DEFAULT_TYPE):
    """Generates the executive report for both manual startup and JobQueue."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if conflicts and chat_id:
            ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            prompt = (
                f"Data: {json.dumps(conflicts)}\n\n"
                "Task: Create a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
                "1. Rank categories by risk severity.\n"
                "2. Explain the 7-14 day delay impact.\n"
                "3. Format: Bold headers, no '#' symbols."
            )
            completion = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "You are a Logistics Analyst."},
                          {"role": "user", "content": prompt}]
            )
            # Use bot from context or directly from app
            target_bot = context.bot if hasattr(context, 'bot') else context
            await target_bot.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
            print("✅ Initial analysis dispatched to Telegram.")
    except Exception as e:
        print(f"Broadcast Failure: {e}")

# --- INTERACTIVE AI ASSISTANT ---
async def handle_bridge_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global processed_updates
    if not update.message or update.message.message_id in processed_updates: return
    processed_updates.add(update.message.message_id)
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        system_instruction = (
            f"You are a Supply Chain Analyst. Context: {json.dumps(conflicts)}. "
            "Respond in the user's language. Executive tone. Bold for headers, no '#' symbols."
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
    print("🚢 SmartPort-Bridge Deployment - Online")
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # --- NEW FORCED STARTUP LOGIC ---
        # 1. We manually trigger the analysis before starting the polling loop
        print("📊 Launching mandatory initial analysis...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_proactive_ranking(app))
        
        # 2. Scheduling for recurring hourly reports
        if app.job_queue:
            app.job_queue.run_repeating(send_proactive_ranking, interval=3600)
        
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        app.run_polling(drop_pending_updates=True)