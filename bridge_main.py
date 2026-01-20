import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()

# Simple persistent memory: a local file to store already alerted IDs
SENT_ALERTS_FILE = "sent_alerts.json"

def load_sent_alerts():
    if os.path.exists(SENT_ALERTS_FILE):
        with open(SENT_ALERTS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_sent_alerts(sent_set):
    with open(SENT_ALERTS_FILE, "w") as f:
        json.dump(list(sent_set), f)

def get_data_from_sheet(sheet_name):
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

def send_telegram_alert(message):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        bot = telebot.TeleBot(token)
        bot.send_message(chat_id, message, parse_mode='Markdown')
    else:
        print("❌ Error: Telegram config missing.")

def run_bridge_engine():
    print("✅ System Online. Checking for NEW supply chain conflicts...")
    
    # Load history to avoid duplicates
    sent_alerts = load_sent_alerts()

    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    if not vessels or not predictions or not mapping:
        return

    critical_vessels = [
        v for v in vessels 
        if str(v.get('risk_level', '')).upper() == 'CRITICAL' 
        or float(v.get('risk_score', 0)) > 75 # Matches your screenshot format (75.72)
    ]

    new_conflicts_found = False

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if assigned_cat:
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            
            # Check if stockout prediction is high AND we haven't alerted this vessel/category combo yet
            alert_key = f"{v_id}_{assigned_cat}"
            
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                if alert_key not in sent_alerts:
                    print(f"🚀 NEW Conflict detected: {v_id} -> {assigned_cat}")
                    
                    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                    prompt = f"Summarize in 3 lines (Spanish) the risk for {v_id} affecting {assigned_cat} inventory."
                    
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": prompt}]
                    )
                    
                    send_telegram_alert(f"🚢 *NEW BRIDGE ALERT*\n\n{response.choices[0].message.content}")
                    sent_alerts.add(alert_key)
                    new_conflicts_found = True
                else:
                    print(f"ℹ️ Alert for {v_id} already sent previously. Skipping...")

    if new_conflicts_found:
        save_sent_alerts(sent_alerts)
    else:
        print("🟢 No new conflicts to report.")

if __name__ == "__main__":
    run_bridge_engine()