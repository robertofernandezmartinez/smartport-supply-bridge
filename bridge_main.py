import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()
telegram_token = os.getenv("TELEGRAM_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")
bot = telebot.TeleBot(telegram_token)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_data_from_sheet(sheet_name):
    """Retrieves all records from a specific Google Sheets worksheet."""
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
        print(f"❌ Error reading sheet {sheet_name}: {e}")
        return []

def run_supply_chain_analysis():
    """Processes maritime and inventory data to identify active conflicts."""
    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    conflicts = []
    # Filter for high-risk vessels (Score > 75)
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if assigned_cat:
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"ship": v_id, "category": assigned_cat})
    return conflicts

def send_initial_report():
    """Generates and sends the mandatory initial Supply Chain Ranking report."""
    current_conflicts = run_supply_chain_analysis()
    
    if not current_conflicts:
        print("🟢 No conflicts found to report.")
        return

    print("🚀 Generating initial proactive report...")
    
    ai_prompt = f"""
    Analyze these supply chain conflicts: {json.dumps(current_conflicts)}
    
    TASK: Create a MANDATORY initial executive report in English.
    1. Title: *SUPPLY CHAIN RISK RANKING*
    2. Subtitle: *EXECUTIVE REPORT: SUPPLY CHAIN CONFLICT ANALYSIS*
    3. IMPACED CATEGORIES RANKING: Rank categories from HIGHEST to LOWEST risk based on the number of vessels.
    4. ANALYSIS OF RISK IMPACT: Explain the 7-14 day delay (customs/trucking) and the stockout gap.
    5. Formatting: Use BOLD for headers, NO '#' symbols.
    6. End with: 'Would you like more details about a specific category or a particular ship?'
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": ai_prompt}]
    )
    
    bot.send_message(chat_id, response.choices[0].message.content, parse_mode='Markdown')

@bot.message_handler(func=lambda message: True)
def handle_interaction(message):
    """Handles conversational follow-ups ensuring the AI always sees the current data context."""
    print(f"💬 User Query: {message.text}")
    current_conflicts = run_supply_chain_analysis()
    
    ai_system_prompt = f"""
    You are an expert Supply Chain Consultant. 
    CURRENT CONTEXT (The data from the Excel): {json.dumps(current_conflicts)}
    
    LOGISTICS RULES:
    - Critical delay (risk_score > 75) = 7-14 days total delay.
    - If the user asks for rankings or 'the second category', count the ships in the JSON context provided.
    
    INSTRUCTIONS:
    - Respond in the user's language.
    - Use professional Markdown (Bold for titles, no '#').
    - USE THE PROVIDED CONTEXT to answer. Never say you don't have data.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": ai_system_prompt},
                {"role": "user", "content": message.text}
            ]
        )
        bot.reply_to(message, response.choices[0].message.content, parse_mode='Markdown')
    except Exception as e:
        print(f"❌ AI Error: {e}")

if __name__ == "__main__":
    # Ensure no other instances are running: pkill -f python
    try:
        send_initial_report()
        print("📡 Bridge Bot is now INTERACTIVE and ONLINE...")
        bot.infinity_polling()
    except Exception as e:
        print(f"❌ Startup Error: {e}")