import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()

# Persistent memory to avoid duplicate alerts
SENT_ALERTS_FILE = "sent_alerts.json"

def load_sent_alerts():
    if os.path.exists(SENT_ALERTS_FILE):
        with open(SENT_ALERTS_FILE, "r") as f:
            try:
                return set(json.load(f))
            except json.JSONDecodeError:
                return set()
    return set()

def save_sent_alerts(sent_set):
    with open(SENT_ALERTS_FILE, "w") as f:
        json.dump(list(sent_set), f)

def get_data_from_sheet(sheet_name):
    """Accesses Google Sheets and retrieves data from a specific tab."""
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
    """Sends a single consolidated message to Telegram."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        bot = telebot.TeleBot(token)
        bot.send_message(chat_id, message, parse_mode='Markdown')
    else:
        print("❌ Error: Telegram configuration missing.")

def run_bridge_engine():
    """Main logic: Aggregates multiple conflicts into a single executive summary."""
    print("✅ System Online. Checking for NEW supply chain conflicts...")
    
    sent_alerts = load_sent_alerts()
    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    if not vessels or not predictions or not mapping:
        print("❌ Critical data missing from Google Sheets.")
        return

    # 1. Identify new critical conflicts
    new_conflicts = []
    
    # Filtering vessels with high risk (>75)
    critical_vessels = [
        v for v in vessels 
        if str(v.get('risk_level', '')).upper() == 'CRITICAL' 
        or float(v.get('risk_score', 0)) > 75
    ]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        # Cross-reference with mapping tab
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if assigned_cat:
            # Cross-reference with inventory predictions
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            alert_key = f"{v_id}_{assigned_cat}"
            
            # Match detected: High vessel risk + High stockout risk
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                if alert_key not in sent_alerts:
                    new_conflicts.append({"ship": v_id, "category": assigned_cat})
                    sent_alerts.add(alert_key)

    # 2. Final Action: Send ONE summary if new conflicts exist
    if new_conflicts:
        print(f"🚀 Found {len(new_conflicts)} new conflicts. Generating AI Summary...")
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Consolidate all conflicts for the AI prompt
        ai_prompt = f"""
        Analyze these multiple supply chain conflicts:
        {json.dumps(new_conflicts)}
        
        Task: Create a 5-line executive summary (in English by default unless the user writes in Spanish).
        - Mention the total number of ships and impacted categories.
        - Focus on the operational impact.
        - Provide one clear mitigation recommendation.
        
        Always respond in English for technical content and code. 
        Maintain a professional tone focused on Data Analytics and Supply Chain logic.
        """
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": ai_prompt}]
        )
        
        executive_report = f"📦 *SUPPLY CHAIN CONSOLIDATED REPORT*\n\n{response.choices[0].message.content}"
        send_telegram_alert(executive_report)
        
        save_sent_alerts(sent_alerts)
        print("✅ Consolidated alert sent. Memory updated.")
    else:
        print("🟢 No new unique conflicts found to report.")

if __name__ == "__main__":
    try:
        run_bridge_engine()
    except Exception as e:
        print(f"❌ Execution Error: {e}")