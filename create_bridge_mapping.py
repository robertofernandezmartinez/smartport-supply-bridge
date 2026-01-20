import os
import json
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

def setup_bridge_dictionary():
    """Links Vessels to Product Categories in Google Sheets."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    try:
        creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
        
        # Create or update the 'supply_chain_map' sheet
        try:
            map_sheet = spreadsheet.add_worksheet(title="supply_chain_map", rows="100", cols="5")
        except:
            map_sheet = spreadsheet.worksheet("supply_chain_map")

        # Define the link: Which ship carries which category?
        mapping_data = [
            ["ship_name_raw", "assigned_category"],
            ["Megastar", "Electronics"],
            ["MEGAStar", "Electronics"],
            ["Megastar", "Toys"],
            ["Star", "Electronics"],
            ["Star", "Toys"],
            ["Finlandia", "Furniture"],
            ["FINLANDIA", "Furniture"],
            ["Finlandia", "Clothing"],
            ["Europa", "Groceries"]
        ]
        
        map_sheet.clear()
        map_sheet.update('A1', mapping_data)
        print("✅ Supply Chain Mapping successful. The bridge is ready.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    setup_bridge_dictionary()