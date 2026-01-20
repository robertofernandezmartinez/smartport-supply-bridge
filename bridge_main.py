import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot # pip install pyTelegramBotAPI

# 1. Initialization
load_dotenv()

def get_data_from_sheet(sheet_name):
    """Connects to Google Sheets and extracts data from a specific tab."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    
    try:
        if not google_json_str:
            raise ValueError("GOOGLE_CREDENTIALS not found in .env")
            
        creds_dict = json.loads(google_json_str)
        from oauth2client.service_account import ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        
        # Open spreadsheet and specific worksheet
        sheet = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        return sheet.get_all_records()
    except Exception as e:
        print(f"❌ Error reading tab {sheet_name}: {e}")
        return []

def send_telegram_alert(message):
    """Sends the processed alert to Telegram."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        bot = telebot.TeleBot(token)
        bot.send_message(chat_id, message, parse_mode='Markdown')
    else:
        print("❌ Error: Telegram Token or Chat ID not configured.")

def run_bridge_engine():
    """Main logic to bridge maritime risks with inventory predictions."""
    print("✅ Environment variables loaded successfully.")
    print("🔄 Connecting SmartPort Maritime Data with Inventory Predictions...")

    # 1. Load Data from your specific tabs
    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    if not vessels or not predictions or not mapping:
        print("❌ Error: One or more tabs are empty or missing.")
        return

    # 2. Filter Critical Vessel Risks
    critical_vessels = [
        v for v in vessels 
        if str(v.get('risk_level', '')).upper() == 'CRITICAL' 
        or float(v.get('risk_score', 0)) > 0.75
    ]

    if not critical_vessels:
        print("🟢 No critical delays detected at this moment.")
        return

    # 3. Cross-reference Data (Matching)
    for vessel in critical_vessels:
        # Check both ship_name and vessel_id to ensure a match
        vessel_identifier = vessel.get('ship_name') or vessel.get('vessel_id')
        
        # Find which category this ship is carrying
        assigned_cat = next((m['assigned_category'] for m in mapping if m['ship_name_raw'] == vessel_identifier), None)
        
        if assigned_cat:
            # Check if that category has a stockout risk
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            
            # 1.0 indicates a predicted stockout
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                print(f"⚠️ Conflict detected: {vessel_identifier} affecting {assigned_cat}")
                
                # 4. Generate AI Insight
                client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                prompt = f"""
                Analyze this supply chain conflict:
                - Vessel delayed: {vessel_identifier} (Critical Risk)
                - Impacted Category: {assigned_cat}
                - Inventory Status: High Stockout Risk (14 days)
                
                Provide a 3-line executive summary in Spanish including a specific mitigation action.
                """
                
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}]
                )
                
                final_msg = f"🚢 *BRIDGE ALERT: SUPPLY CHAIN AT RISK*\n\n{response.choices[0].message.content}"
                send_telegram_alert(final_msg)
                print("🚀 Telegram alert sent successfully.")

if __name__ == "__main__":
    try:
        run_bridge_engine()
    except Exception as e:
        print(f"❌ Execution Error: {e}")