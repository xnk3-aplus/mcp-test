import os
from dotenv import load_dotenv
import requests
import pandas as pd
import json

load_dotenv()

class TableAPIClient:
    """Client for fetching data from Base Table (ID 81)"""
    
    def __init__(self):
        self.url = "https://table.base.vn/extapi/v1/table/records"
        # Using the tokens established in table.py
        self.TABLE_ACCESS_TOKEN = os.getenv('TABLE_ACCESS_TOKEN')
        
    def get_checkin_scores(self) -> dict:
        """
        Fetch all records and return a mapping of:
        {user_id}_{timestamp} -> next_action_score
        """
        payload = {'access_token_v2': self.TABLE_ACCESS_TOKEN, 'table_id': 81}
        headers = {}
        scores_map = {}

        try:
            response = requests.post(self.url, headers=headers, data=payload)
            data = response.json()
            records = data.get('data', [])
            
            if not records:
                print("DEBUG: No records found in Base Table.")
                return {}

            # Extract vals from each record
            rows = [r.get('vals', {}) for r in records]
            df = pd.DataFrame(rows)
            # print(f"DEBUG: DF Columns: {df.columns.tolist()}")
            
            # Helper to safely get value from dataframe row
            # Mappings confirmed from table.py:
            # f2: next_action_score
            # f7: checkin_since
            # f10: checkin_user_id
            
            # Check if required columns exist
            required_cols = ['f2', 'f7', 'f10']
            for col in required_cols:
                if col not in df.columns:
                    # If columns missing, try to see if they are empty or named differently?
                    # valid response usually has f-keys.
                    # If empty df, loop won't run.
                    continue

            for _, row in df.iterrows():
                try:
                    score_val = row.get('f2')
                    timestamp_val = row.get('f7')
                    user_id_val = row.get('f10')
                    
                    if pd.isna(score_val) or pd.isna(timestamp_val) or pd.isna(user_id_val):
                        continue
                        
                    # Clean User ID
                    user_id = str(user_id_val).strip()
                    if not user_id: continue
                    
                    # Clean Timestamp
                    # f7 is datetime string "YYYY-MM-DD HH:MM:SS"
                    try:
                         # dt is naive (representing local time 2026-...)
                         # We verified this is treated as UTC by .timestamp() resulting in +7h offset vs real UTC
                         # So we subtract 7h to get correct UTC timestamp
                         dt = pd.to_datetime(timestamp_val) - pd.Timedelta(hours=7)
                         timestamp = int(dt.timestamp())
                    except:
                        continue
                        
                    # Clean Score
                    try:
                        score = float(score_val)
                    except:
                        score = 0
                    
                    # Create Key matching goal_test.py expectation
                    key = f"{user_id}_{timestamp}"
                    scores_map[key] = score
                    
                except Exception:
                    continue
                    
            print(f"DEBUG: Loaded {len(scores_map)} checkin scores from Base Table.")
            return scores_map
                    
            print(f"DEBUG: Loaded {len(scores_map)} checkin scores from Base Table.")
            return scores_map

        except Exception as e:
            print(f"Error fetching checkin scores from Table: {e}")
            return {}
