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
processed_updates = set()

def get_bridge_data():
    """Accesses Sheets: risk_alerts, stockout_predictions, supply_chain_map."""
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
    """Business Logic: Cross-references numeric IDs with categories and stockout risk."""
    category_summary = {}
    vessel_to_cat = {str(m.get('ship_name_raw')): str(m.get('assigned_category')).strip() for m in mapping}
    # Only categories with stockout_14d_pred == 1 are considered in conflict
    risky_cats = {str(p.get('category')).strip() for p in predictions if str(p.get('stockout_14d_pred')) == "1"}

    for v in vessels:
        try:
            if float(v.get('risk_score', 0)) >= 0.75: # Critical only
                v_id = str(v.get('vessel_id'))
                category = vessel_to_cat.get(v_id)
                if category and category in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except: continue
            
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    return sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)

# --- RECURSO: INFORME INICIAL (SIEMPRE EN INGLÉS) ---
async def send_startup_report(app):
    """Sends the default mandatory report in English on startup."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if conflicts and chat_id:
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        prompt = (
            f"Dataset: {json.dumps(conflicts)}\n\n"
            "Task: Create a 'SUPPLY CHAIN RISK RANKING' report.\n"
            "LANGUAGE: STRICTLY ENGLISH.\n"
            "1. Title: 📦 SUPPLY CHAIN RISK RANKING\n"
            "2. Rank all categories found, listing total vessels per category.\n"
            "3. Explain the 7-14 day delay impact. Bold headers, no '#' symbols."
        )
        res = ai_client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
        await app.bot.send_message(chat_id=chat_id, text=res.choices[0].message.content, parse_mode='Markdown')

# --- RECURSO: CHAT INTERACTIVO (IDIOMA DINÁMICO) ---
async def handle_interaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Responds to user queries in the user's language based on current data."""
    global processed_updates
    if not update.message or update.message.message_id in processed_updates: return
    processed_updates.add(update.message.message_id)

    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Instrucciones dinámicas para que no repita el informe si no se pide un ranking
    system_prompt = (
        f"You are a Supply Chain Analyst. Current Context: {json.dumps(conflicts)}. "
        "INSTRUCTIONS:\n"
        "1. Identify the user's language and respond in that same language.\n"
        "2. If the user asks for a 'Top X', provide exactly that number of categories if available.\n"
        "3. Be conversational. Do not just send a formal report unless requested.\n"
        "4. Use bold for key figures. No '#' symbols."
    )
    
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": system_prompt},
                  {"role": "user", "content": update.message.text}]
    )
    await update.message.reply_text(response.choices[0].message.content, parse_mode='Markdown')

if __name__ == '__main__':
    print("🚢 SmartPort-Bridge Deployment - Online")
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        loop = asyncio.get_event_loop()
        # Startup Report in English
        loop.run_until_complete(send_startup_report(app))
        
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_interaction))
        app.run_polling(drop_pending_updates=True)