import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Setup
load_dotenv()
processed_updates = set()

def get_bridge_data():
    """Retrieves records from Google Sheets using the exact tab names provided."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(spreadsheet_id)
        
        # Exact tab names from your screenshot
        vessels = doc.worksheet("risk_alerts").get_all_records()
        predictions = doc.worksheet("stockout_predictions").get_all_records()
        mapping = doc.worksheet("supply_chain_map").get_all_records()
        return vessels, predictions, mapping
    except Exception as e:
        print(f"❌ Spreadsheet Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Groups vessels by category using numeric IDs from the screenshots."""
    category_summary = {}
    
    # 1. Create mapping: ship_name_raw (ID) -> assigned_category
    # We convert the key to string to avoid Number vs String conflicts
    vessel_to_cat = {str(m.get('ship_name_raw')): str(m.get('assigned_category')).strip() for m in mapping}
    
    # 2. Identify categories with stockout_14d_pred == 1
    risky_cats = {str(p.get('category')).strip() for p in predictions if str(p.get('stockout_14d_pred')) == "1"}

    for v in vessels:
        try:
            # Match only CRITICAL vessels (risk_score is 1.00 in your screenshot)
            score = float(v.get('risk_score', 0))
            if score >= 0.75: 
                v_id = str(v.get('vessel_id')) # Getting the ID (0, 1, 2...)
                category = vessel_to_cat.get(v_id)
                
                if category and category in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except:
            continue
            
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    return sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)

async def send_mandatory_report(app):
    """Generates the final report based on the ID-to-Category mapping."""
    print("📊 Analyzing Supply Chain IDs...")
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not conflicts:
        # If no strict match, we list the categories found in the map to debug
        print("⚠️ No matches. Ensuring IDs from risk_alerts exist in supply_chain_map.")
        return

    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = (
        f"Dataset: {json.dumps(conflicts)}\n\n"
        "Task: Create a 'SUPPLY CHAIN RISK RANKING' report in Spanish.\n"
        "1. Title: 📦 SUPPLY CHAIN RISK RANKING\n"
        "2. Subtitle: EXECUTIVE REPORT: CONFLICT ANALYSIS\n"
        "3. Detail: Category ranking and 7-14 day delay impact.\n"
        "4. Format: Bold headers, NO '#' symbols."
    )
    
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    
    await app.bot.send_message(chat_id=chat_id, text=response.choices[0].message.content, parse_mode='Markdown')
    print("✅ Executive report sent to Telegram.")

if __name__ == '__main__':
    print("🚢 SmartPort-Bridge starting with Numeric ID support...")
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_mandatory_report(app))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), 
            lambda u, c: asyncio.create_task(send_mandatory_report(app))))
        app.run_polling(drop_pending_updates=True)