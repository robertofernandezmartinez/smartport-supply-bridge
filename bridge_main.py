import os
import json
import gspread
from openai import OpenAI
from dotenv import load_dotenv
import telebot

# 1. Initialization
load_dotenv()

def get_data_from_sheet(sheet_name):
    """Accesses Google Sheets and retrieves data from a specific worksheet tab."""
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
    """Dispatches the final consolidated message to the configured Telegram chat."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        bot = telebot.TeleBot(token)
        bot.send_message(chat_id, message, parse_mode='Markdown')
    else:
        print("❌ Error: Telegram configuration missing in .env")

def run_bridge_engine():
    """Main execution engine: identifies conflicts and generates a ranked AI summary."""
    print("✅ System Online. Processing Supply Chain Analysis for Demo...")
    
    vessels = get_data_from_sheet("risk_alerts")
    predictions = get_data_from_sheet("stockout_predictions")
    mapping = get_data_from_sheet("supply_chain_map")

    if not vessels or not predictions or not mapping:
        print("❌ Critical data missing from Google Sheets. Operation aborted.")
        return

    # In Demo Mode, we process all current conflicts to ensure Telegram alerts are triggered
    new_conflicts = []
    
    # Filter for high-risk vessels (Score > 75 or Status: Critical)
    critical_vessels = [
        v for v in vessels 
        if str(v.get('risk_level', '')).upper() == 'CRITICAL' 
        or float(v.get('risk_score', 0)) > 75
    ]

    for vessel in critical_vessels:
        v_id = str(vessel.get('vessel_id') or vessel.get('ship_name'))
        assigned_cat = next((m['assigned_category'] for m in mapping if str(m['ship_name_raw']) == v_id), None)
        
        if assigned_cat:
            stock_risk = next((p for p in predictions if p['category'] == assigned_cat), None)
            
            # Condition: Delay impacting a category with a predicted stockout (value >= 1)
            if stock_risk and float(stock_risk.get('stockout_14d_pred', 0)) >= 1:
                new_conflicts.append({"ship": v_id, "category": assigned_cat})

    # Final Action: Generate a single AI report if conflicts are found
    if new_conflicts:
        print(f"🚀 Found {len(new_conflicts)} conflicts. Requesting AI Executive Summary...")
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Prompt
        ai_prompt = f"""
        Analyze these supply chain conflicts: {json.dumps(new_conflicts)}
        
        Task: Create an executive report in English.
        
        FORMATTING RULES FOR TELEGRAM:
        - Do NOT use '#' for headers. 
        - Use BOLD text for titles and section headers (e.g., *SECTION TITLE*).
        - Use bullet points for lists.
        - Ensure the structure is clean and easy to read on a mobile screen.

        CONTENT:
        1. Rank the impacted categories from HIGHEST to LOWEST risk based on the number of vessels affected.
        2. For each category, explain that the 7-14 day delay (including customs/trucking) will cause a stockout gap.
        3. Be conversational at the end: Ask the user if they want details on a specific category or ship. Respond in the same language used by the user.
        """      
         
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": ai_prompt}]
        )
        
        report_msg = f"📦 *SUPPLY CHAIN RISK RANKING*\n\n{response.choices[0].message.content}"
        send_telegram_alert(report_msg)
        print("✅ Alert dispatched successfully.")
    else:
        print("🟢 No conflicts identified with current data criteria.")

if __name__ == "__main__":
    try:
        run_bridge_engine()
    except Exception as e:
        print(f"❌ Execution Error: {e}")