import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Global Environment Setup
load_dotenv()
processed_updates = set()

def get_bridge_data():
    """Fetches data from the three primary worksheets: risk_alerts, predictions, and mapping."""
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
        print(f"❌ Spreadsheet Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Business Logic: Detects 7-14 day lead time gaps based on risk scores."""
    conflicts = []
    # Filtering for risk_score > 75
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        ship_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        category = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == ship_id), None)
        if category:
            stock_risk = next((p for p in predictions if p['category'] == category), None)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"vessel": ship_id, "category": category})
    return conflicts

# --- ANALOGY: PROACTIVE MONITORING (Same as your other project) ---
async def send_proactive_ranking(context: ContextTypes.DEFAULT_TYPE):
    """Task that runs at startup and periodically to send the Risk Ranking."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if conflicts and chat_id:
            ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            prompt = (
                f"Data: {json.dumps(conflicts)}\n\n"
                "Task: Create a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
                "1. Rank categories by number of affected vessels.\n"
                "2. Explain the 7-14 day delay impact on the 14-day stockout window.\n"
                "3. Use BOLD for headers, NO '#' symbols."
            )
            completion = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "You are a Strategic Logistics Analyst."},
                          {"role": "user", "content": prompt}]
            )
            await context.bot.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
            print("✅ Proactive Report Dispatched.")
    except Exception as e:
        print(f"Monitoring Error: {e}")

# --- ANALOGY: AI ASSISTANT (Same as your other project) ---
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
            "You are a Supply Chain Strategist. Answer based on this dataset.\n"
            f"Context: {json.dumps(conflicts)}.\n"
            "Respond in the user's language. Be executive and direct."
        )
        
        completion = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_instruction},
                      {"role": "user", "content": update.message.text}]
        )
        await update.message.reply_text(completion.choices[0].message.content, parse_mode='Markdown')
    except Exception as e:
        print(f"AI Assistant Error: {e}")

if __name__ == '__main__':
    print("🚢 SmartPort-Bridge Deployment - Online")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # Scheduling the analysis: Starts 1 second after deployment (first=1)
        if app.job_queue:
            app.job_queue.run_repeating(send_proactive_ranking, interval=3600, first=1)
        
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        
        # Clears old messages and starts listening
        app.run_polling(drop_pending_updates=True)