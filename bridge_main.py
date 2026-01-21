import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

# 1. Environment and Session Setup
load_dotenv()
processed_updates = set()

# --- SYSTEM INSTRUCTIONS (CLO LOGIC) ---
# This prompt forces the AI to treat vessel counts as operational risks (congestion)
# rather than just stock units.
CLO_PROMPT = (
    "You are a Chief Logistics Officer (CLO). Your analysis must follow these strict rules:\n"
    "1. RISK LOGIC: Higher Vessel Count = Higher Crisis. For example, 170 delayed vessels create a "
    "massive port bottleneck and warehouse saturation risk compared to 140.\n"
    "2. LANGUAGE: Always provide a 'MANDATORY EXECUTIVE SUMMARY' in English first. "
    "Then, provide the detailed analysis in the user's language.\n"
    "3. STRATEGY: Prioritize 'Groceries' due to perishable nature (shelf-life risk) and "
    "'Electronics' due to high capital tie-up.\n"
    "4. FORMAT: Use Bold for key figures and headers. DO NOT use '#' symbols."
)

def get_bridge_data():
    """Retrieves records from risk_alerts, stockout_predictions, and supply_chain_map."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(spreadsheet_id)
        
        return (doc.worksheet("risk_alerts").get_all_records(), 
                doc.worksheet("stockout_predictions").get_all_records(), 
                doc.worksheet("supply_chain_map").get_all_records())
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return [], [], []

def identify_conflicts(vessels, predictions, mapping):
    """Business Logic: Maps numeric IDs to categories and filters by active stockout risk."""
    category_summary = {}
    
    # Map IDs (as strings) to Categories
    vessel_to_cat = {str(m.get('ship_name_raw')): str(m.get('assigned_category')).strip() for m in mapping}
    
    # Identify categories with predicted stockout (Value == 1)
    risky_cats = {str(p.get('category')).strip() for p in predictions if str(p.get('stockout_14d_pred')) == "1"}

    for v in vessels:
        try:
            # Filter for critical vessels (Score >= 0.75)
            score = float(v.get('risk_score', 0))
            if score >= 0.75: 
                v_id = str(v.get('vessel_id'))
                category = vessel_to_cat.get(v_id)
                
                # Intersection: Vessel is in risk and category is predicted to stock out
                if category and category in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except:
            continue
            
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    return sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)

async def send_executive_report(app):
    """Generates the initial CLO assessment report on startup."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if conflicts and chat_id:
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        prompt = (
            f"Current Dataset: {json.dumps(conflicts)}\n\n"
            "Task: Generate the initial Executive Risk Ranking report following the CLO instructions."
        )
        
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": CLO_PROMPT},
                {"role": "user", "content": prompt}
            ]
        )
        
        await app.bot.send_message(chat_id=chat_id, text=response.choices[0].message.content, parse_mode='Markdown')
        print("✅ Startup Executive Report dispatched.")

async def handle_interaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Responds to user queries in the user's language using CLO logic."""
    global processed_updates
    if not update.message or update.message.message_id in processed_updates: return
    processed_updates.add(update.message.message_id)

    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Inject current conflict data into the context
    context_data = f"\n\nCURRENT LOGISTICS CONTEXT: {json.dumps(conflicts)}"
    
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": CLO_PROMPT + context_data},
            {"role": "user", "content": update.message.text}
        ]
    )
    await update.message.reply_text(response.choices[0].message.content, parse_mode='Markdown')

if __name__ == '__main__':
    print("🚢 SmartPort-Bridge CLO System - Deployment Online")
    token = os.getenv("TELEGRAM_TOKEN")
    
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        
        # Immediate Startup Analysis
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_executive_report(app))
        
        # Interaction Handler
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_interaction))
        
        print("🤖 Listening for queries...")
        app.run_polling(drop_pending_updates=True)