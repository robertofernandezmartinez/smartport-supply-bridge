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
    """Retrieves records from Google Sheets for risk analysis."""
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
    """Aggregates conflicts by category to match the 21:20 successful report format."""
    category_summary = {}
    
    # Pre-map vessels to categories for quick lookup
    vessel_map = {str(m['ship_name_raw']).strip().lower(): str(m['assigned_category']) for m in mapping}
    # Pre-identify categories with stockout risk
    risky_cats = {str(p['category']).strip().lower() for p in predictions if float(p.get('stockout_14d_pred', 0)) >= 1}

    for vessel in vessels:
        try:
            # We use a broader threshold to ensure data presence, matching previous success
            score = float(vessel.get('risk_score', 0))
            if score > 10: 
                v_id = str(vessel.get('vessel_id') or vessel.get('ship_name')).strip().lower()
                category = vessel_map.get(v_id)
                
                if category and category.strip().lower() in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except:
            continue
            
    # Format for AI: list of {"category": name, "total_vessels": count}
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    print(f"🔍 Analysis complete. Categories affected: {len(conflicts)}")
    return conflicts

async def send_proactive_ranking(context: ContextTypes.DEFAULT_TYPE):
    """Generates the EXACT report format from the 21:20 success."""
    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not conflicts:
            print("⚠️ No data matches found. Check mapping names.")
            return

        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        prompt = (
            f"Dataset: {json.dumps(conflicts)}\n\n"
            "Task: Generate the EXACT executive report in Spanish.\n"
            "1. Title: 📦 SUPPLY CHAIN RISK RANKING\n"
            "2. Subtitle: EXECUTIVE REPORT: SUPPLY CHAIN CONFLICT ANALYSIS\n"
            "3. IMPACED CATEGORIES RANKING: List categories and 'Total vessels affected'.\n"
            "4. ANALYSIS OF RISK IMPACT: Explain the 7-14 day delay and stockout gap.\n"
            "5. NO '#' symbols, use BOLD for headers."
        )
        
        completion = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a Supply Chain Consultant."},
                      {"role": "user", "content": prompt}]
        )
        
        bot_instance = context.bot if hasattr(context, 'bot') else context.bot
        await bot_instance.send_message(chat_id=chat_id, text=completion.choices[0].message.content, parse_mode='Markdown')
        print("✅ Success: Proactive report dispatched.")
    except Exception as e:
        print(f"Broadcast Failure: {e}")

async def handle_bridge_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interactive response using the summarized category data."""
    global processed_updates
    if not update.message or update.message.message_id in processed_updates: return
    processed_updates.add(update.message.message_id)

    try:
        v, p, m = get_bridge_data()
        conflicts = identify_conflicts(v, p, m)
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        system_instruction = (
            f"You are a Supply Chain Analyst. Current conflicts summary: {json.dumps(conflicts)}. "
            "Answer specifically about these categories. Bold for headers, no '#' symbols."
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
        
        # Immediate Startup Analysis
        print("📊 Launching mandatory initial analysis...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_proactive_ranking(app))
        
        if app.job_queue:
            app.job_queue.run_repeating(send_proactive_ranking, interval=3600)
        
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_bridge_query))
        app.run_polling(drop_pending_updates=True)