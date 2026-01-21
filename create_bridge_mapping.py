import os
import json
import gspread
import random
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# 1. Initialization
load_dotenv()

def generate_full_mapping():
    """Reads all unique vessels and assigns them a category from the REAL inventory list."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        
        # 2. Get vessels from risk_alerts
        risk_sheet = sh.worksheet("risk_alerts")
        vessels_data = risk_sheet.get_all_records()
        
        # We use vessel_id as the primary key based on your previous screenshots
        unique_vessels = list(set([str(v.get('vessel_id')) for v in vessels_data if v.get('vessel_id') is not None]))
        
        # 3. REAL CATEGORIES (Matches stockout_predictions exactly)
        # Replacing "Fashion" with "Clothing" and adding "Furniture" and "Groceries"
        real_categories = ["Clothing", "Electronics", "Furniture", "Groceries", "Toys"]
        
        # 4. Prepare mapping data
        mapping_data = [["ship_name_raw", "assigned_category"]] # Headers
        for vessel_id in unique_vessels:
            mapping_data.append([vessel_id, random.choice(real_categories)])
        
        # 5. Write to supply_chain_map
        try:
            map_sheet = sh.worksheet("supply_chain_map")
            map_sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            map_sheet = sh.add_worksheet(title="supply_chain_map", rows=str(len(unique_vessels) + 100), cols="5")
            
        map_sheet.update('A1', mapping_data)
        print(f"✅ Mapping updated! {len(unique_vessels)} vessels linked to REAL categories: {real_categories}")
        
    except Exception as e:
        print(f"❌ Error creating mapping: {e}")

if __name__ == "__main__":
    generate_full_mapping()