import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()
telegram_token = os.getenv("TELEGRAM_TOKEN")
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

    if not vessels or not predictions or not mapping:
        return []

    conflicts = []
    # Filtering for high-risk vessels (Score > 75)
    critical_vessels = [v for v in vessels if float(v.get('risk_score', 0)) > 75]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if assigned_cat:
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            # Match: Significant maritime delay + Predicted 14-day stockout
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                conflicts.append({"ship": v_id, "category": assigned_cat})
    return conflicts

@bot.message_handler(func=lambda message: True)
def handle_interaction(message):
    """Handles incoming Telegram messages by injecting live supply chain data into the AI context."""
    print(f"💬 User Message: {message.text}")
    
    # Refresh data on every request to ensure the AI has the latest context
    current_conflicts = run_supply_chain_analysis()
    
    ai_system_prompt = f"""
    You are an expert Supply Chain Consultant.
    
    CONTEXT DATA:
    Current detected conflicts: {json.dumps(current_conflicts)}
    
    LOGISTICS RULES:
    - A 'Critical' vessel delay (risk_score > 75) implies a 7-14 day gap in the supply chain.
    - This gap accounts for port delays, customs clearance, and inland distribution.
    
    YOUR TASK:
    1. Respond to the user's question using the provided context.
    2. If they ask for a summary or ranking: Rank categories from HIGHEST to LOWEST risk based on the number of ships affected.
    3. Formatting: Use Telegram-compatible Markdown (BOLD for headers, no '#' symbols).
    4. Respond in the same language as the user (Spanish or English).
    5. Always remain professional and executive.
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
        print(f"❌ AI Integration Error: {e}")
        bot.reply_to(message, "Error processing the request. Please check the terminal logs.")

if __name__ == "__main__":
    print("📡 Bridge Bot is INTERACTIVE and ONLINE...")
    bot.infinity_polling()