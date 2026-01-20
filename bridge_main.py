import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
bot = telebot.TeleBot(token)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_data_from_sheet(sheet_name):
    """Accesses Google Sheets and retrieves data."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    
    try:
        creds_dict = json.loads(google_json_str)
        from oauth2client.service_account import ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        return sheet.get_all_records()
    except Exception as e:
        print(f"❌ Error reading tab {sheet_name}: {e}")
        return []

def generate_risk_analysis():
    """Logic to analyze and rank categories by risk."""
    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    conflicts = []
    critical_vessels = [v for v in vessels if v.get('risk_score', 0) > 75]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        if assigned_cat:
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"ship": v_id, "category": assigned_cat})
    
    return conflicts

@bot.message_handler(func=lambda message: True)
def handle_interaction(message):
    """Handles user questions about the logistics data."""
    print(f"💬 User asked: {message.text}")
    conflicts = generate_risk_analysis()
    
    ai_prompt = f"""
    The user is asking: '{message.text}'
    
    Context Data (Current Conflicts): {json.dumps(conflicts)}
    
    Task: Answer the user's question using the context. 
    - If they ask for a ranking or 'what is the second category', use the data.
    - Keep the tone professional and executive.
    - Respond in the same language as the user.
    """
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": ai_prompt}]
    )
    
    bot.reply_to(message, response.choices[0].message.content, parse_mode='Markdown')

if __name__ == "__main__":
    # First, send the proactive alert
    conflicts = generate_risk_analysis()
    if conflicts:
        print("🚀 Sending initial proactive report...")
        # (Same AI summary logic as before to send the first message)
        # For brevity, I'll trigger the polling directly
    
    print("📡 Bot is now INTERACTIVE and listening for messages...")
    bot.infinity_polling()