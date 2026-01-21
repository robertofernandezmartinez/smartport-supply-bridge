import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

def get_bridge_data():
    """Accesses the Google Spreadsheet tabs: risk_alerts, stockout_predictions, and supply_chain_map."""
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
    """Business Logic: Maps numeric IDs and filters by active stockout risk."""
    category_summary = {}
    vessel_to_cat = {str(m.get('ship_name_raw')): str(m.get('assigned_category')).strip() for m in mapping}
    risky_cats = {str(p.get('category')).strip() for p in predictions if str(p.get('stockout_14d_pred')) == "1"}

    for v in vessels:
        try:
            if float(v.get('risk_score', 0)) >= 0.75: 
                v_id = str(v.get('vessel_id'))
                category = vessel_to_cat.get(v_id)
                if category and category in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except: continue
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    return sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)

# --- CLO SYSTEM PROMPT ---
import os
import json
import gspread
import asyncio
from openai import OpenAI
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

def get_bridge_data():
    """Accesses the Google Spreadsheet tabs: risk_alerts, stockout_predictions, and supply_chain_map."""
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
    """Business Logic: Maps numeric IDs and filters by active stockout risk."""
    category_summary = {}
    vessel_to_cat = {str(m.get('ship_name_raw')): str(m.get('assigned_category')).strip() for m in mapping}
    risky_cats = {str(p.get('category')).strip() for p in predictions if str(p.get('stockout_14d_pred')) == "1"}

    for v in vessels:
        try:
            if float(v.get('risk_score', 0)) >= 0.75: 
                v_id = str(v.get('vessel_id'))
                category = vessel_to_cat.get(v_id)
                if category and category in risky_cats:
                    category_summary[category] = category_summary.get(category, 0) + 1
        except: continue
    conflicts = [{"category": cat, "total_vessels": count} for cat, count in category_summary.items()]
    return sorted(conflicts, key=lambda x: x['total_vessels'], reverse=True)

# --- CLO SYSTEM PROMPT ---
CLO_PROMPT = (
    "You are a Chief Logistics Officer (CLO). Your analysis must follow these rules:\n"
    "1. HIGH VOLUME = HIGH CRISIS. If a category has more vessels, it means higher port congestion and warehouse saturation risk.\n"
    "2. Language: Always provide an Executive Summary in English first, then detailed analysis in the user's language.\n"
    "3. Strategy: Focus on 'Groceries' as a priority due to shelf-life, and 'Electronics' for high capital tie-up.\n"
    "4. Format: Use bold for figures, NO '#' symbols."
)

async def send_executive_report(app):
    """Startup trigger for the CLO report."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if conflicts and chat_id:
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        prompt = f"Data: {json.dumps(conflicts)}. Generate the initial executive report following your CLO instructions."
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": CLO_PROMPT}, {"role": "user", "content": prompt}]
        )
        await app.bot.send_message(chat_id=chat_id, text=response.choices[0].message.content, parse_mode='Markdown')

async def handle_interaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interactive response in the user's language."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": CLO_PROMPT + f"\nContext: {json.dumps(conflicts)}"},
                  {"role": "user", "content": update.message.text}]
    )
    await update.message.reply_text(response.choices[0].message.content, parse_mode='Markdown')

if __name__ == '__main__':
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_executive_report(app))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_interaction))
        app.run_polling(drop_pending_updates=True)
)

async def send_executive_report(app):
    """Startup trigger for the CLO report."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if conflicts and chat_id:
        ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        prompt = f"Data: {json.dumps(conflicts)}. Generate the initial executive report following your CLO instructions."
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": CLO_PROMPT}, {"role": "user", "content": prompt}]
        )
        await app.bot.send_message(chat_id=chat_id, text=response.choices[0].message.content, parse_mode='Markdown')

async def handle_interaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interactive response in the user's language."""
    v, p, m = get_bridge_data()
    conflicts = identify_conflicts(v, p, m)
    ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    response = ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": CLO_PROMPT + f"\nContext: {json.dumps(conflicts)}"},
                  {"role": "user", "content": update.message.text}]
    )
    await update.message.reply_text(response.choices[0].message.content, parse_mode='Markdown')

if __name__ == '__main__':
    token = os.getenv("TELEGRAM_TOKEN")
    if token:
        app = ApplicationBuilder().token(token.strip()).build()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_executive_report(app))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_interaction))
        app.run_polling(drop_pending_updates=True)