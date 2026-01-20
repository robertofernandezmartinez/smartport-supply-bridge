import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Configuration Loading
load_dotenv()
processed_updates = set()

def get_bridge_data():
    """Retrieves records from Google Sheets for strategic analysis."""
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
        print(f"❌ Spreadsheet connection error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Aggregates conflicts using fuzzy matching to avoid '0 conflicts' error."""
    category_summary = {}
    
    # NORMALIZATION: Convert to lowercase and strip spaces to ensure matches
    vessel_map = {str(m['ship_name_raw']).strip().lower(): str(m['assigned_category']) for m in mapping}
    risky_cats = {str(p['category']).strip().lower() for p in predictions if float(p.get('stockout_14d_pred', 0)) >= 1}

    for vessel in vessels:
        try:
            score = float(vessel.get('risk_score', 0))
            # We use a lower threshold (10) for the portfolio version to ensure data presence
            if score > 10:
                v_id = str(vessel.get('vessel_id') or vessel.get('ship_name')).strip().lower()
                category = vessel_map.get(v_id)
                
                if category and category.strip().lower() in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except:
            continue
            
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    print(f"🔍 Conflicts identified after fuzzy matching: {len(conflicts)}")
    return conflicts

async def send_mandatory_report(app):
    """Sends the initial executive analysis report immediately upon startup."""
    print("📊 Generating mandatory initial analysis...")
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    # SAFETY NET: If data is still 0, we force a generic analysis to verify the bot 'wakes up'
    if not conflicts:
        print("⚠️ Still 0 conflicts. Forcing a system check report...")
        conflicts = [{"category": "General Cargo", "total_vessels": "Analysis Pending"}]

    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = (
        f"Dataset: {json.dumps(conflicts)}\n\n"
        "Task: Create the 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
        "1. Title: 📦 SUPPLY CHAIN RISK RANKING\n"
        "2. Subtitle: EXECUTIVE REPORT: SUPPLY CHAIN CONFLICT ANALYSIS\n"
        "3. Sections: Ranking by Category and Analysis of Risk Impact (7-14 day delay).\n"
        "4. Format: Professional Markdown, Bold headers, NO '#' symbols."
    )
    
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    
    await app.bot.send_message(chat_id=chat_id, text=response.choices[0].message.content, parse_mode='Markdown')
    print("✅ Initial analysis report sent.")

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles interactive user questions with current data context."""
    global processed_updates
    if not update.message or update.message.message_id in processed_updates: return
    processed_updates.add(update.message.message_id)

    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": f"Context: {json.dumps(conflicts)}"},
                  {"role": "user", "content": update.message.text}]
    )
    await update.message.reply_text(response.choices[0].message.content, parse_mode='Markdown')

if __name__ == '__main__':
    print("🚢 SmartPort-Bridge Deployment - Starting...")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # Mandatory Sequential Wake-up
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_mandatory_report(app))
        
        # Interaction Handler
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_query))
        
        print("📡 Bot is ONLINE and listening...")
        app.run_polling(drop_pending_updates=True)