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
    """Retrieves records from Google Sheets: risk_alerts, stockout_predictions, and supply_chain_map."""
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
        print(f"❌ Spreadsheet Connectivity Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Business logic to cross-reference maritime delays with category stockout risks."""
    conflicts = []
    # Strategic threshold: risk_score > 75 implies a 7-14 day delay impact
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        ship_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        category = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == ship_id), None)
        
        if category:
            stock_risk = next((p for p in predictions if p['category'] == category), None)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"vessel": ship_id, "category": category})
    return conflicts

# --- PROACTIVE BROADCAST LOGIC ---
async def send_proactive_ranking(app_instance):
    """Triggers the initial Supply Chain Ranking report on startup."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if conflicts and chat_id:
            ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = (
                f"Data Context: {json.dumps(conflicts)}\n\n"
                "Task: Generate a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
                "1. Rank categories by risk severity based on vessel count.\n"
                "2. Explain how the 7-14 day delay breaches the 14-day stock window.\n"
                "3. Format: Bold headers, no '#' characters. Executive tone."
            )
            
            completion = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "You are a Logistics Strategy Consultant."},
                          {"role": "user", "content": prompt}]
            )
            
            # Use bot instance to send message
            await app_instance.bot.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
            print("✅ Executive Ranking sent to Telegram.")
    except Exception as e:
        print(f"Broadcast Error: {e}")

# --- INTERACTIVE QUERY HANDLER ---
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
            "You are an interactive Supply Chain Analyst. Use the following context.\n"
            f"Context: {json.dumps(conflicts)}\n"
            "Respond in the user's language. Use Bold for headers, avoid '#' symbols."
        )
        
        completion = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_instruction},
                      {"role": "user", "content": update.message.text}]
        )
        await update.message.reply_text(completion.choices[0].message.content, parse_mode='Markdown')
    except Exception as e:
        print(f"Query Error: {e}")

if __name__ == '__main__':
    print("🚀 Starting SmartPort-Bridge System...")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # 1. IMMEDIATE STARTUP REPORT
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_proactive_ranking(app))
        
        # 2. INTERACTIVE MESSAGE LISTENER
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        
        print("📡 System Online. Listening for queries...")
        app.run_polling(drop_pending_updates=True)