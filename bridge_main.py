import os
import pandas as pd
import gspread
import json
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
import telebot

# --- ENVIRONMENT SETUP ---
# Forzamos la búsqueda del archivo .env en el directorio actual
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")
load_dotenv(dotenv_path=env_path)

# --- VALIDATION ---
def check_env():
    required = ["GOOGLE_CREDENTIALS", "OPENAI_API_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "SPREADSHEET_ID"]
    missing = [var for var in required if os.getenv(var) is None]
    if missing:
        raise ValueError(f"❌ Missing variables in .env: {', '.join(missing)}")
    print("✅ Environment variables loaded successfully.")

# --- INITIALIZATION ---
try:
    check_env()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    bot = telebot.TeleBot(os.getenv("TELEGRAM_TOKEN"))
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
except Exception as e:
    print(f"🚨 Initialization Error: {e}")
    exit()

def get_data_from_sheet(sheet_name):
    """Downloads a specific sheet from the spreadsheet."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_raw = os.getenv("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_raw)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return pd.DataFrame(sh.worksheet(sheet_name).get_all_records())

def run_bridge_engine():
    print("🔄 Connecting SmartPort Maritime Data with Inventory Predictions...")
    
    try:
        # 1. Load DataFrames
        df_risk = get_data_from_sheet("risk_alerts")
        df_map = get_data_from_sheet("supply_chain_map")
        df_stock = get_data_from_sheet("predictions_TEST")
        
        # 2. Identify High Risk Vessels (Score > 0.75)
        # Check column names: 'ship_name' and 'risk_score'
        critical_vessels = df_risk[df_risk['risk_score'] > 0.75]
        
        if critical_vessels.empty:
            print("🟢 No critical delays detected at this moment.")
            return

        for _, vessel in critical_vessels.iterrows():
            ship = str(vessel['ship_name']).strip()
            risk_val = vessel['risk_score']
            
            # 3. Find associated categories in our Bridge Map
            # Normalizing both sides to lower case for safety
            mapped_cats = df_map[df_map['ship_name_raw'].str.lower() == ship.lower()]['assigned_category'].unique()
            
            for category in mapped_cats:
                # 4. Filter products in this category with stockout prediction = 1
                stockout_items = df_stock[(df_stock['category'] == category) & (df_stock['stockout_14d_pred'] == 1)]
                
                if not stockout_items.empty:
                    # 5. Generate Professional Insight via OpenAI
                    print(f"⚠️ Conflict found: {ship} delay affects {category} stock.")
                    
                    prompt = f"""
                    LOGISTICS ALERT:
                    Vessel: {ship} (Risk Score: {risk_val}) is critically delayed.
                    This ship carries goods from the '{category}' category.
                    Inventory Impact: Our predictive model shows that {len(stockout_items)} items in '{category}' 
                    will face a STOCKOUT within the next 14 days.
                    
                    Task: Write a concise, executive-level alert for a Supply Chain Manager. 
                    Be professional, urgent, and include a business recommendation.
                    """
                    
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are a Supply Chain Intelligence Expert."},
                            {"role": "user", "content": prompt}
                        ]
                    )
                    
                    final_message = response.choices[0].message.content
                    
                    # 6. Send to Telegram
                    bot.send_message(CHAT_ID, final_message)
                    print(f"🚀 Integrated alert sent for {ship} impacting {category} inventory.")

    except Exception as e:
        print(f"❌ Execution Error: {e}")

if __name__ == "__main__":
    run_bridge_engine()