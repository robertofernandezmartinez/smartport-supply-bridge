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

# Global state to ensure single responses per message ID
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
        print(f"❌ Bridge Database Connectivity Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Business logic to detect overlaps between maritime delays and inventory risk."""
    conflicts = []
    # Threshold: risk_score > 75 implies a strategic 7-14 day supply chain delay
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        category = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if category:
            stock_risk = next((p for p in predictions if p['category'] == category), None)
            # Match: Severe delay + Stockout predicted within 14 days
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"vessel": v_id, "category": category})
    return conflicts

# --- PROACTIVE RANKING BROADCAST ---
async def send_proactive_ranking(context: ContextTypes.DEFAULT_TYPE):
    """Triggers the Supply Chain Risk Ranking report. Can be called by JobQueue or manually."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if conflicts and chat_id:
            ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = (
                f"Data Context: {json.dumps(conflicts)}\n\n"
                "Task: Create a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
                "1. Rank categories by risk severity based on vessel count.\n"
                "2. Detail the impact of the 7-14 day delay on stock availability.\n"
                "3. Format: Bold headers, no '#' characters. Mobile-optimized."
            )
            
            completion = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "You are a Strategic Logistics Analyst."},
                          {"role": "user", "content": prompt}]
            )
            
            # Using context.bot for scheduled jobs, or the bot instance for manual startup
            bot = context.bot if hasattr(context, 'bot') else context
            await bot.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
            print("✅ Proactive Ranking Analysis dispatched to Telegram.")
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
            "You are a Supply Chain Strategy Expert. Use the provided dataset to answer questions.\n"
            f"Current Conflicts: {json.dumps(conflicts)}.\n"
            "If asked for rankings, count occurrences in the data.\n"
            "Respond in the user's language. Be executive and direct.\n"
            "A 'Critical' delay (risk_score > 75) means a 7-14 day impact including customs/trucking."
        )
        
        completion = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": update.message.text}
            ]
        )
        await update.message.reply_text(completion.choices[0].message.content, parse_mode='Markdown')
    except Exception as e:
        print(f"Interactive Assistant Error: {e}")

if __name__ == '__main__':
    print("🚀 Initializing SmartPort-Bridge System...")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # 1. FORCED STARTUP TRIGGER: Run analysis immediately before polling
        print("📊 Launching mandatory initial analysis...")
        async def startup_report():
            await send_proactive_ranking(app)
        
        # Initialize loop to run the startup report once
        loop = asyncio.get_event_loop()
        loop.run_until_complete(startup_report())
        
        # 2. JOB QUEUE: Scheduled recurring report (every 1 hour)
        if app.job_queue:
            app.job_queue.run_repeating(send_proactive_ranking, interval=3600)
        
        # 3. INTERACTIVE HANDLER: Waiting for user questions
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        
        print("📡 System Online. Listening for interactive queries...")
        app.run_polling(drop_pending_updates=True)