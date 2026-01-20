import os
import json
import gspread
import random
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# 1. Initialization
load_dotenv()

def generate_full_mapping():
    """Reads all unique vessels and assigns them a category in the mapping tab."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_json_str = os.getenv("GOOGLE_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    
    try:
        creds_dict = json.loads(google_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        
        # Get all existing vessels from the risk_alerts tab
        risk_sheet = sh.worksheet("risk_alerts")
        vessels_data = risk_sheet.get_all_records()
        
        # Extract unique vessel names
        unique_vessels = list(set([v.get('ship_name') or v.get('vessel_id') for v in vessels_data if v.get('ship_name') or v.get('vessel_id')]))
        
        # Complete list of categories (Add as many as you need)
        categories = ["Electronics", "Fashion", "Home Appliances", "Toys", "Automotive", "Food & Beverage", "Pharma"]
        
        # Prepare data for the mapping tab
        mapping_data = [["ship_name_raw", "assigned_category"]] # Headers
        for ship in unique_vessels:
            mapping_data.append([ship, random.choice(categories)])
        
        # Write to supply_chain_map tab
        # Create the tab if it doesn't exist, or clear it if it does
        try:
            map_sheet = sh.worksheet("supply_chain_map")
            map_sheet.clear()
        except:
            map_sheet = sh.add_worksheet(title="supply_chain_map", rows="100", cols="20")
            
        map_sheet.update('A1', mapping_data)
        print(f"✅ Mapping complete! {len(unique_vessels)} vessels mapped to categories.")
        
    except Exception as e:
        print(f"❌ Error creating mapping: {e}")

if __name__ == "__main__":
    generate_full_mapping()