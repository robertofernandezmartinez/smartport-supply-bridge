import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Initialization
load_dotenv()

def get_bridge_data():
    """Fetches data from Google Sheets."""
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
        print(f"❌ Connection Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Ultra-robust logic to fix data mismatches and restore the 21:20 report."""
    category_summary = {}
    
    # 1. CLEAN MAPPING: Normalize ship names and categories
    vessel_map = {
        str(m.get('ship_name_raw', '')).strip().lower(): str(m.get('assigned_category', '')).strip() 
        for m in mapping
    }
    
    # 2. CLEAN PREDICTIONS: Find categories with risk >= 1
    risky_cats = {
        str(p.get('category', '')).strip().lower() 
        for p in predictions if float(p.get('stockout_14d_pred', 0)) >= 1
    }

    print(f"✅ Scanning {len(vessels)} vessels against {len(risky_cats)} risky categories...")

    for vessel in vessels:
        try:
            # Threshold lowered to 10 for safety
            score = float(vessel.get('risk_score', 0))
            if score > 10: 
                # Normalize ship name from alerts
                v_id = str(vessel.get('vessel_id') or vessel.get('ship_name', '')).strip().lower()
                category = vessel_map.get(v_id)
                
                if category:
                    # Check if this category is in our risky list
                    if category.lower() in risky_cats:
                        category_summary[category] = category_summary.get(category, 0) + 1
        except Exception as e:
            continue
            
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    
    # SORTING: Ensure Toys and Electronics are at the top if they have more ships
    conflicts = sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)
    
    print(f"🔍 Found {len(conflicts)} categories with real conflicts.")
    return conflicts
    
async def send_mandatory_report(app):
    """Generates the required Executive Report with real data."""
    print("📊 Generating analysis...")
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    if not conflicts:
        report_content = "⚠️ *DIAGNÓSTICO*: El bot funciona pero el cruce de datos da 0. Revisa los nombres en el Excel."
    else:
        prompt = (
            f"Dataset: {json.dumps(conflicts)}\n\n"
            "Task: Create the 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
            "1. Title: 📦 SUPPLY CHAIN RISK RANKING\n"
            "2. Rank categories and Total vessels affected.\n"
                "3. Use BOLD, NO '#' symbols."
        )
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        report_content = response.choices[0].message.content

    await app.bot.send_message(chat_id=chat_id, text=report_content, parse_mode='Markdown')
    print("✅ Process complete.")

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interaction handler."""
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
    print("🚢 SmartPort-Bridge starting...")
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_mandatory_report(app))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_query))
        app.run_polling(drop_pending_updates=True)