from fastmcp import FastMCP, Context

from typing import Dict, List, Optional
import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from dotenv import load_dotenv

load_dotenv()


# Tokens from environment
GOAL_ACCESS_TOKEN = os.getenv('GOAL_ACCESS_TOKEN')
ACCOUNT_ACCESS_TOKEN = os.getenv('ACCOUNT_ACCESS_TOKEN')
TABLE_ACCESS_TOKEN = os.getenv('TABLE_ACCESS_TOKEN', '5654~iP-VCfH0Oh_ZGLthwfMDzTskE_7RVCEuGDAZKg6YmlIrCzS1sKyUCv9YIivHcTV7rEgC473hbTx7Io6_Ho0eItYkxIXp0SXI3qQQHAhj9QTrlFEkggRCz0zQBxR-AUpQHp8wMgEgwJYFeR1BNeBmYg')
# GG_SCRIPT_URL removed as requested

import time

def _make_request(url: str, data: Dict, description: str = "") -> requests.Response:
    """Make HTTP request with error handling and retry logic"""
    max_retries = 3
    backoff_factor = 1  # seconds

    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = backoff_factor * (2 ** attempt)
                print(f"⚠️ Error {description}: {e}. Retrying in {wait_time}s ({attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed {description} after {max_retries + 1} attempts: {e}")
                raise

# Department and Team ID Mappings
DEPT_ID_MAPPING = {
    "450": "BP Thị Trường",
    "451": "BP Cung Ứng",
    "452": "BP Nhân Sự Hành Chính",
    "453": "BP Tài Chính Kế Toán",
    "542": "Khối hiện trường (các vùng miền)",
    "651": "Ban Giám Đốc",
    "652": "BP R&D, Business Line mới"
}

TEAM_ID_MAPPING = {
    "307": "Đội Bán hàng - Chăm sóc khách hàng",
    "547": "Đội Nguồn Nhân Lực",
    "548": "Đội Kế toán - Quản trị",
    "1032": "Team Hoàn Thuế VAT (Nhóm)",
    "1128": "Đội Thanh Hóa (Miền Bắc)",
    "1129": "Đội Quy Nhơn",
    "1133": "Đội Hành chính - Số hóa",
    "1134": "Team Thực tập sinh - Thử nghiệm mới (Nhóm)",
    "1138": "Đội Marketing - AI",
    "1141": "Đội Tài chính - Đầu tư",
    "1148": "Đội Logistic quốc tế - Thị trường",
    "546": "Đội Mua hàng - Out source",
    "1130": "Đội Daknong",
    "1131": "Đội KCS VT-SG",
    "1135": "Đội Chuỗi cung ứng nội địa - Thủ tục XNK",
    "1132": "Đội Văn hóa - Chuyển hóa",
    "1136": "Đội Chất lượng - Sản phẩm",
    "1137": "Team 1 (Nhóm 1)",
    "1139": "Đội Data - Hệ thống - Số hóa",
    "1375": "AGILE _ DỰ ÁN 1"
}

# Create FastMCP server
mcp = FastMCP("OKR Analysis Server")

# Helper functions
def get_cycle_list() -> List[Dict]:
    """Get list of OKR cycles from API"""
    url = "https://goal.base.vn/extapi/v1/cycle/list"
    data = {'access_token_v2': GOAL_ACCESS_TOKEN}
    try:
        response = _make_request(url, data, "fetching cycle list")
        
        cycles_data = response.json()
        quarterly_cycles = []
        
        for cycle in cycles_data.get('cycles', []):
            if cycle.get('metatype') == 'quarterly':
                try:
                    start_time = datetime.fromtimestamp(float(cycle['start_time']))
                    end_time = datetime.fromtimestamp(float(cycle['end_time']))
                    quarterly_cycles.append({
                        'name': cycle['name'],
                        'id': str(cycle.get('id', '')),
                        'path': cycle['path'],
                        'start_time': start_time,
                        'end_time': end_time,
                        'formatted_start_time': start_time.strftime('%d/%m/%Y'),
                        'formatted_end_time': end_time.strftime('%d/%m/%Y')
                    })
                except:
                    continue
        
        return sorted(quarterly_cycles, key=lambda x: x['start_time'], reverse=True)
    except Exception as e:
        print(f"Error fetching cycles: {e}")
        return []


def get_checkins_data(cycle_path: str, ctx: Optional[Context] = None) -> List[Dict]:
    """Get all checkins for a cycle with progress reporting"""
    url = "https://goal.base.vn/extapi/v1/cycle/checkins"
    all_checkins = []
    
    if ctx:
        ctx.info(f"Starting to fetch checkins for cycle: {cycle_path}")
    
    max_pages = 50
    for page in range(1, max_pages + 1):
        data = {"access_token_v2": GOAL_ACCESS_TOKEN, "path": cycle_path, "page": page}
        try:
            response = _make_request(url, data, f"fetching checkins page {page}")
            
            response_data = response.json()
            if isinstance(response_data, list) and len(response_data) > 0:
                response_data = response_data[0]
            
            checkins = response_data.get('checkins', [])
            if not checkins:
                break
            
            all_checkins.extend(checkins)
            
            if ctx:
                ctx.report_progress(page, max_pages)
                ctx.info(f"Fetched page {page}, total checkins: {len(all_checkins)}")
                
            if len(checkins) < 10:
                break
        except Exception as e:
            if ctx: ctx.error(f"Error page {page}: {e}")
            break
            
    return all_checkins


def get_goals_and_krs(cycle_path: str, ctx: Optional[Context] = None) -> tuple:
    """Get goals and KRs data"""
    url = "https://goal.base.vn/extapi/v1/cycle/get.full"
    data = {'access_token_v2': GOAL_ACCESS_TOKEN, 'path': cycle_path}
    try:
        response = _make_request(url, data, "fetching goals")
        
        cycle_data = response.json()
        goals = cycle_data.get('goals', [])
        
        krs_url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        
        if ctx: ctx.info("Fetching KRs...")
        for page in range(1, 20):
            krs_data = {"access_token_v2": GOAL_ACCESS_TOKEN, "path": cycle_path, "page": page}
            res = _make_request(krs_url, krs_data, f"fetching krs page {page}")
            
            kd = res.json()
            if isinstance(kd, list) and kd: kd = kd[0]
            
            krs = kd.get("krs", [])
            if not krs: break
            all_krs.extend(krs)
            if len(krs) < 20: break
            
        return goals, all_krs
    except:
        return [], []


def get_user_names() -> Dict[str, str]:
    """Get user mapping"""
    url = "https://account.base.vn/extapi/v1/users"
    data = {"access_token_v2": ACCOUNT_ACCESS_TOKEN}
    try:
        response = _make_request(url, data, "fetching users")
        ud = response.json()
        if isinstance(ud, list) and ud: ud = ud[0]
        return {str(u['id']): u['name'] for u in ud.get('users', [])}
    except:
        return {}



# MCP Tools


def get_targets_data(cycle_path: str, ctx: Optional[Context] = None) -> List[Dict]:
    """Get targets data for alignment info"""
    # Fetch targets - this is simplified as direct target API might be complex to get all
    # We will try to fetch targets related to KRs if possible, or just skip if too complex for standalone
    # Based on goal.py, it constructs target_df. For standalone, we might skip deep target hierarchy join if not essential,
    # but user requested target columns.
    # We will return empty list for now if strict target structure is needed, or try to implementation basic fetch if possible.
    # Given standalone constraints, we will leave target columns as None/Empty for now unless we implement full target scanning.
    return []

def get_target_sub_goal_ids(target_id: str) -> List[str]:
    """Fetch sub-goal IDs for a specific target"""
    url = "https://goal.base.vn/extapi/v1/target/get"
    data = {'access_token_v2': GOAL_ACCESS_TOKEN, 'id': str(target_id)}
    
    try:
        response = _make_request(url, data, f"fetching sub-goals for {target_id}")
        response_data = response.json()
        if response_data and 'target' in response_data and response_data['target']:
            cached_objs = response_data['target'].get('cached_objs', [])
            if isinstance(cached_objs, list):
                return [str(item.get('id')) for item in cached_objs if 'id' in item]
        return []
    except Exception as e:
        print(f"Error fetching sub-goal {target_id}: {e}")
        return []

def parse_targets_logic(cycle_path: str, ctx: Optional[Context] = None) -> pd.DataFrame:
    """Parse targets data from API to create target mapping with robust logic"""
    url = "https://goal.base.vn/extapi/v1/cycle/get.full"
    data = {'access_token_v2': GOAL_ACCESS_TOKEN, 'path': cycle_path}

    try:
        if ctx: ctx.info("Fetching targets data...")
        response = _make_request(url, data, "fetching targets")
        response_data = response.json()
        
        if not response_data or 'targets' not in response_data:
            return pd.DataFrame()
        
        all_targets = []
        raw_targets = response_data.get('targets', [])
        
        # 1. Map Company Targets (Top Level scope='company')
        company_targets_map = {}
        for t in raw_targets:
            if t.get('scope') == 'company':
                company_targets_map[str(t.get('id', ''))] = {
                    'id': str(t.get('id', '')),
                    'name': t.get('name', '')
                }
        
        # 2. Iterate ALL targets to find relevant ones (including detached Dept/Team targets)
        collected_targets = []
        
        # Helper to extract form data
        def extract_form_data(target_obj):
            # strict columns requested by user
            form_data = {
                "Mức độ đóng góp vào mục tiêu công ty": "",
                "Mức độ ưu tiên mục tiêu của Quý": "",
                "Tính khó/tầm ảnh hưởng đến hệ thống": ""
            }
            if 'form' in target_obj and isinstance(target_obj['form'], list):
                for item in target_obj['form']:
                    key = item.get('name')
                    val = item.get('value')
                    if key:
                        form_data[key] = val
            return form_data

        for t in raw_targets:
            t_id = str(t.get('id', ''))
            scope = t.get('scope', '')
            parent_id = str(t.get('parent_id') or '')
            
            # Case A: Detached Dept/Team Target linked to Company Parent
            if scope in ['dept', 'team'] and parent_id in company_targets_map:
                parent = company_targets_map[parent_id]
                target_data = {
                    'target_id': t_id,
                    'target_company_id': parent['id'],
                    'target_company_name': parent['name'],
                    'target_name': t.get('name', ''),
                    'target_scope': scope,
                    'target_dept_id': None, 'target_dept_name': None,
                    'target_team_id': None, 'target_team_name': None,
                    'team_id': str(t.get('team_id', '')),
                    'dept_id': str(t.get('dept_id', ''))
                }
                # Merge form data
                target_data.update(extract_form_data(t))
                collected_targets.append(target_data)

            # Case B: Company Target (inspect its cached_objs)
            elif scope == 'company':
                if 'cached_objs' in t and isinstance(t['cached_objs'], list):
                    for kr in t['cached_objs']:
                        sub_data = {
                            'target_id': str(kr.get('id', '')),
                            'target_company_id': t_id,
                            'target_company_name': t.get('name', ''),
                            'target_name': kr.get('name', ''),
                            'target_scope': kr.get('scope', ''),
                            'target_dept_id': None, 'target_dept_name': None,
                            'target_team_id': None, 'target_team_name': None,
                            'team_id': str(kr.get('team_id', '')),
                            'dept_id': str(kr.get('dept_id', ''))
                        }
                        # Merge form data from the sub-object (kr)
                        sub_data.update(extract_form_data(kr))
                        collected_targets.append(sub_data)

        # 3. Post-process: Fill columns and fetch sub-goals
        total_targets = len(collected_targets)
        for i, target_data in enumerate(collected_targets):
            if ctx and i % 5 == 0: 
                ctx.info(f"Processing target {i+1}/{total_targets}: {target_data['target_name']}")
            
            # Fill specific columns based on scope
            if target_data['target_scope'] == 'dept':
                target_data['target_dept_id'] = target_data['target_id']
                target_data['target_dept_name'] = target_data['target_name']
            elif target_data['target_scope'] == 'team':
                target_data['target_team_id'] = target_data['target_id']
                target_data['target_team_name'] = target_data['target_name']
            
            # Fetch sub-goal IDs
            target_data['list_goal_id'] = get_target_sub_goal_ids(target_data['target_id'])
            
            all_targets.append(target_data)
        
        return pd.DataFrame(all_targets)
    except Exception as e:
        if ctx: ctx.error(f"Error parsing targets: {e}")
        return pd.DataFrame()

# Helper to resolve cycle path
def _resolve_cycle_path(cycle_arg: str = None, ctx: Context = None) -> str:
    cycles = get_cycle_list()
    if not cycles: return None
    
    if not cycle_arg:
        if ctx: ctx.info(f"No cycle specified, defaulting to latest: {cycles[0]['name']}")
        return cycles[0]['path']
        
    lower_arg = cycle_arg.lower().strip()
    
    # 1. Try date matching (MM/YYYY or YYYY-MM)
    try:
        query_date = None
        # Try MM/YYYY
        if '/' in lower_arg:
            parts = lower_arg.split('/')
            if len(parts) == 2:
                month, year = int(parts[0]), int(parts[1])
                query_date = datetime(year, month, 15) # Pick middle of month
        # Try YYYY-MM
        elif '-' in lower_arg:
             parts = lower_arg.split('-')
             if len(parts) == 2:
                 year, month = int(parts[0]), int(parts[1])
                 query_date = datetime(year, month, 15)
        
        if query_date:
            if ctx: ctx.info(f"Parsed date query: {query_date.strftime('%m/%Y')}")
            # Find cycle covering this date
            for c in cycles:
                if c['start_time'] <= query_date <= c['end_time']:
                    if ctx: ctx.info(f"Found cycle by date: {c['name']}")
                    return c['path']
    except Exception as e:
        if ctx: ctx.info(f"Date parsing failed: {e}, falling back to name search")
        pass

    # 2. Search by name (Fallback)
    for c in cycles:
        if lower_arg in c['name'].lower():
            if ctx: ctx.info(f"Selected cycle by name: {c['name']}")
            return c['path']
            
    # Fallback/Default
    if ctx: ctx.info(f"Cycle '{cycle_arg}' not found, defaulting to latest: {cycles[0]['name']}")
    return cycles[0]['path']

def get_cycle_info(cycle_arg: str = None):
    """Resolve cycle arg to full cycle info (name, path)"""
    cycles = get_cycle_list()
    if not cycles: return None
    
    selected_cycle = None
    if not cycle_arg:
        selected_cycle = cycles[0]
    else:
        lower_arg = cycle_arg.lower().strip()
        # 1. Try date
        try:
             # Reuse date logic logic or just loop since it's short
             query_date = None
             if '/' in lower_arg:
                parts = lower_arg.split('/')
                if len(parts) == 2: query_date = datetime(int(parts[1]), int(parts[0]), 15)
             elif '-' in lower_arg:
                 parts = lower_arg.split('-')
                 if len(parts) == 2: query_date = datetime(int(parts[0]), int(parts[1]), 15)
             
             if query_date:
                for c in cycles:
                    if c['start_time'] <= query_date <= c['end_time']:
                        selected_cycle = c
                        break
        except: pass
        
        # 2. Name search
        if not selected_cycle:
            for c in cycles:
                if lower_arg in c['name'].lower():
                    selected_cycle = c
                    break
    
    # Fallback
    if not selected_cycle: selected_cycle = cycles[0]
    
    return selected_cycle

def _get_full_data_logic(ctx: Optional[Context] = None, cycle_arg: str = None) -> List[Dict]:
    """Core logic to get full detailed data"""
    try:
        if ctx: ctx.info("Starting full data fetch...")
        cycles = get_cycle_list()
        if not cycles: return [{"error": "No OKR cycles found"}]
        
        
        # Resolve cycle
        cycle_path = _resolve_cycle_path(cycle_arg, ctx)
        if not cycle_path: return [{"error": "No OKR cycles found"}]
        
        # 1. Fetch all raw data
        checkins = get_checkins_data(cycle_path, ctx)
        goals, krs = get_goals_and_krs(cycle_path, ctx)
        
        if not goals and not krs:
             return [{"error": "No Goals or KRs found in cycle"}]

        # 1b. Fetch Targets using robust logic
        targets_df = parse_targets_logic(cycle_path, ctx)
        
        # Build maps
        user_map = get_user_names()
        goal_map = {str(g['id']): g for g in goals}
        
        # Convert targets_df to dictionary map for easier lookup
        targets_map = {}
        if not targets_df.empty:
            for _, row in targets_df.iterrows():
                targets_map[str(row['target_id'])] = row.to_dict()

        # 3. Join Data
        full_data = []
        

        # Helper to extract form values
        def extract_form_value(form_array, field_name):
            if not form_array or not isinstance(form_array, list):
                return ""
            for item in form_array:
                if item.get('name') == field_name:
                    return item.get('value', item.get('display', ""))
            return ""

        # Helper for timestamp conversion
        def convert_time(ts):
            if not ts: return ''
            try:
                dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                tz_hcm = pytz.timezone('Asia/Ho_Chi_Minh')
                return dt_utc.astimezone(tz_hcm).strftime('%Y-%m-%d %H:%M:%S')
            except:
                return ''

        # Process all KRs (safe iteration)
        if not krs:
            # Check for goals without KRs if needed, but for now we follow existing logic
            pass

        for kr in krs:
            kr_id = str(kr.get('id', ''))
            # Safety: if no KR ID, skip or generate placeholder? goal.py skips or errors. We skip.
            if not kr_id: continue

            goal_id = str(kr.get('goal_id', ''))
            goal = goal_map.get(goal_id, {})
            
            # Common Goal/KR data
            goal_user_id = str(goal.get('user_id', ''))
            
            # Target Info
            target_id_ref = str(goal.get('target_id', ''))
            t_info = targets_map.get(target_id_ref, {})
            
            # Extract Direct Goal Data (Sync with goal.py)
            g_dept_id = str(goal.get('dept_id', '0'))
            g_team_id = str(goal.get('team_id', '0'))
            
            # Map names
            g_dept_name = "" if (g_dept_id == "0" or g_dept_id == 0 or not g_dept_id) else DEPT_ID_MAPPING.get(g_dept_id, "")
            g_team_name = "" if (g_team_id == "0" or g_team_id == 0 or not g_team_id) else TEAM_ID_MAPPING.get(g_team_id, "")
            
            goal_form = goal.get('form', [])
            
            base_row = {
                'goal_id': goal_id,
                'goal_name': goal.get('name', ''),
                'goal_content': goal.get('content', ''),
                'goal_since': convert_time(goal.get('since')),
                'goal_current_value': goal.get('current_value', 0),
                'goal_user_id': goal_user_id,
                'goal_target_id': target_id_ref,
                'kr_id': kr_id,
                'kr_name': kr.get('name', ''),
                'kr_content': kr.get('content', ''),
                'kr_since': convert_time(kr.get('since')),
                'kr_current_value': kr.get('current_value', 0),
                'goal_user_name': user_map.get(goal_user_id, f"User_{goal_user_id}"),
                'goal_username': '', 
                'list_goal_id': '',
                # Target populated
                'target_id': t_info.get('target_id', ''), 
                'target_company_id': t_info.get('target_company_id', ''), 
                'target_company_name': t_info.get('target_company_name', ''),
                'target_name': t_info.get('target_name', ''), 
                'target_scope': t_info.get('target_scope', ''),
                'target_dept_id': t_info.get('target_dept_id', ''),
                'target_dept_name': t_info.get('target_dept_name', ''),
                'target_team_id': t_info.get('target_team_id', ''),
                'target_team_name': t_info.get('target_team_name', ''),
                'list_goal_id': t_info.get('list_goal_id', []),
                
                # Direct Goal Extractions [NEW]
                'dept_id': g_dept_id,
                'team_id': g_team_id,
                'dept_name': g_dept_name,
                'team_name': g_team_name,
                'Mức độ đóng góp vào mục tiêu công ty': extract_form_value(goal_form, 'Mức độ đóng góp vào mục tiêu công ty'),
                'Mức độ ưu tiên mục tiêu của Quý': extract_form_value(goal_form, 'Mức độ ưu tiên mục tiêu của Quý'),
                'Tính khó/tầm ảnh hưởng đến hệ thống': extract_form_value(goal_form, 'Tính khó/tầm ảnh hưởng đến hệ thống'),
            }
            
            # Add dynamic form fields from target
            # Exclude known standard keys to avoid overwriting base fields (though likely safe)
            standard_keys = [
                'target_id', 'target_company_id', 'target_company_name', 'target_name', 
                'target_scope', 'target_dept_id', 'target_dept_name', 'target_team_id', 
                'target_team_name', 'list_goal_id'
            ]
            for k, v in t_info.items():
                if k not in standard_keys:
                    # Avoid overwriting user-requested goal-level fields if target also has them (duplicates)
                    # We prioritize the Goal-level extraction above for the 3 specific fields
                    if k not in ['Mức độ đóng góp vào mục tiêu công ty', 'Mức độ ưu tiên mục tiêu của Quý', 'Tính khó/tầm ảnh hưởng đến hệ thống']:
                        base_row[k] = v
            
            # Find checkins for this KR
            kr_checkins = [c for c in checkins if str(c.get('obj_export', {}).get('id', '')) == kr_id]
            
            if not kr_checkins:
                # Add row with empty checkin info - Logic matches goal.py "no checkin" row
                row = base_row.copy()
                # Explicitly set checkin fields to empty/default
                row.update({
                    'checkin_id': '', 'checkin_name': '', 'checkin_since': '',
                    'checkin_since_timestamp': '', 'cong_viec_tiep_theo': '',
                    'checkin_target_name': '', 'checkin_kr_current_value': 0, 'checkin_user_id': ''
                })
                # Ensure extract_form_value is used or default empty
                row['cong_viec_tiep_theo'] = ''
                full_data.append(row)
            else:
                for c in kr_checkins:
                    row = base_row.copy()
                    checkin_ts = c.get('since', '')
                    c_form = c.get('form', [])
                    row.update({
                        'checkin_id': str(c.get('id', '')),
                        'checkin_name': c.get('name', ''),
                        'checkin_since': convert_time(checkin_ts),
                        'checkin_since_timestamp': checkin_ts,
                        'cong_viec_tiep_theo':  extract_form_value(c_form, 'Công việc tiếp theo') or extract_form_value(c_form, 'Mô tả tiến độ') or extract_form_value(c_form, 'Những công việc quan trọng, trọng yếu, điểm nhấn thực hiện trong Tuần để đạt được kết quả (không phải công việc giải quyết hàng ngày)') or '', 
                        'checkin_target_name': '', 
                        'checkin_kr_current_value': c.get('current_value', 0),
                        'checkin_user_id': str(c.get('user_id', ''))
                    })
                    full_data.append(row)
                    
        return full_data
        
    except Exception as e:
        if ctx: ctx.error(f"Error generating full data: {e}")
        return [{"error": str(e)}]





def _get_checkins_from_table(ctx: Optional[Context] = None, cycle: str = None) -> List[Dict]:
    """Fetch checkins from Base Table 81 filtered by cycle_id"""
    
    # 1. Resolve Cycle to get ID
    cycle_info = get_cycle_info(cycle)
    if not cycle_info:
        if ctx: ctx.error("Could not find any cycle info")
        return [{"error": "Cycle not found"}]
        
    cycle_id = str(cycle_info.get('id', ''))
    cycle_name = cycle_info.get('name', '')
    
    if ctx: ctx.info(f"Fetching checkins for cycle '{cycle_name}' (ID: {cycle_id}) from Table 81...")
    
    # 2. Fetch User Map
    user_map = get_user_names()
    
    # 3. Fetch Table Records
    url = "https://table.base.vn/extapi/v1/table/records"
    all_records = []
    
    # Pagination
    for page in range(1, 100): # Safety limit
        payload = {
            'access_token_v2': TABLE_ACCESS_TOKEN, 
            'table_id': 81,
        }
        try:
            response = _make_request(url, payload, f"fetching table page {page}")
            data = response.json()
            records = data.get('data', [])
            
            if not records:
                break
                
            all_records.extend(records)
            
            if len(records) < 100:
                break
        except Exception as e:
            if ctx: ctx.error(f"Error fetching table page {page}: {e}")
            break
            
    if not all_records:
        if ctx: ctx.info(f"No records found in Table 81.")
        return []

    # 4. Filter and Map
    simplified_checkins = []
    
    for r in all_records:
        vals = r.get('vals', {})
        
        # 'f1' is cycle_id
        r_cycle_id = str(vals.get('f1', ''))
        
        # Filter by Cycle ID
        if r_cycle_id == cycle_id:
            # Map Fields similar to get_records.py
            # '_name' -> checkin_name
            # 'f1' -> cycle_id
            # 'f2' -> next_action_score
            # 'f4' -> cong_viec_tiep_theo
            # 'f5' -> checkin_id
            # 'f7' -> checkin_since
            # 'f8' -> checkin_target_name
            # 'f9' -> checkin_kr_current_value
            # 'f10' -> checkin_user_id
            # 'f11' -> kr_name
            
            u_id = str(vals.get('f10', ''))
            user_name = user_map.get(u_id, f"User {u_id}") if u_id else ""
            
            item = {
                'checkin_name': r.get('name', ''), # _name
                'checkin_since': vals.get('f7', ''),
                'goal_user_name': user_name,
                'kr_name': vals.get('f11', ''),
                'cong_viec_tiep_theo': vals.get('f4', ''),
                'checkin_kr_current_value': vals.get('f9', 0),
                # Extra fields if needed for debugging or context
                'checkin_id': vals.get('f5', ''),
                'next_action_score': vals.get('f2', '')
            }
            simplified_checkins.append(item)
            
    if ctx: ctx.info(f"Found {len(simplified_checkins)} checkins for cycle {cycle_name}")
    return simplified_checkins


def _get_all_checkins_logic(ctx: Optional[Context] = None, cycle: str = None) -> List[Dict]:
    """Logic for get_all_checkins - switched to Table Source"""
    return _get_checkins_from_table(ctx, cycle)

    # OLD LOGIC DEPRECATED
    # full_data = _get_full_data_logic(ctx, cycle)
    # 
    # # Check for error
    # if isinstance(full_data, list) and len(full_data) == 1 and "error" in full_data[0]:
    #     return full_data

    simplified_checkins = []
    
    for row in full_data:
        # Filter: Must have a checkin_id (meaning it's a real check-in row, not just a goal placeholder)
        if row.get('checkin_id'):
            simplified_checkins.append({
                'checkin_name': row.get('checkin_name', ''),
                'checkin_since': row.get('checkin_since', ''),
                'goal_user_name': row.get('goal_user_name', ''),
                'kr_name': row.get('kr_name', ''),
                'cong_viec_tiep_theo': row.get('cong_viec_tiep_theo', ''),
                'checkin_kr_current_value': row.get('checkin_kr_current_value', 0)
            })
            
    return simplified_checkins

@mcp.tool(
    name="get_all_checkins",
    description="Lấy danh sách tất cả check-in của mọi người dùng với các thông tin chi tiết: tên, thời gian, người dùng, KR, công việc tiếp theo, điểm số, điểm next action.",
    tags={"okr", "checkin", "report"},
    annotations={"readOnlyHint": True}
)
def get_all_checkins(ctx: Context, cycle: str = None) -> List[Dict]:
    """
    Get all check-ins with specific fields:
    - checkin_name
    - checkin_since
    - goal_user_name
    - kr_name
    - cong_viec_tiep_theo
    - checkin_kr_current_value
    - next_action_score
    """
    return _get_all_checkins_logic(ctx, cycle)


def _get_tree_logic(ctx: Optional[Context] = None, cycle_arg: str = None) -> Dict:
    """Build hierarchical OKR tree using robust target parsing"""
    try:
        if ctx: ctx.info("Building OKR tree...")
        
        # Resolve cycle
        cycle_path = _resolve_cycle_path(cycle_arg, ctx)
        if not cycle_path: return {"error": "No OKR cycles found"}
        
        # 1. Fetch Robust Target List
        targets_df = parse_targets_logic(cycle_path, ctx)
        if targets_df.empty:
             return {"error": "No targets found"}

        # 2. Fetch User Goals & KRs
        goals, krs = get_goals_and_krs(cycle_path, ctx)
        
        # 2b. Fetch User Map
        user_map = get_user_names()
        
        # 3. Build lookup maps
        # Map: KR ID (which acts as Dept/Team Target ID in API relation) -> List of User Goals
        goals_by_target = {}
        personal_goals = [] # Goals with no target_id
        
        for g in goals:
            tid = str(g.get('target_id', ''))
            # Check for valid target_id (not None, not empty, not "0")
            if tid and tid != "0":
                if tid not in goals_by_target: goals_by_target[tid] = []
                goals_by_target[tid].append(g)
            else:
                personal_goals.append(g)
                
        krs_by_goal = {}
        for k in krs:
            gid = str(k.get('goal_id', ''))
            if gid:
                if gid not in krs_by_goal: krs_by_goal[gid] = []
                krs_by_goal[gid].append(k)

        # 4. Construct Tree
        # Structure: Company -> Dept/Team -> Goals
        tree = {}
        
        # Group by Company Target
        # targets_df has columns: target_id, target_name, target_scope, target_company_id, target_company_name
        
        # Get unique Company IDs
        company_groups = targets_df.groupby(['target_company_id', 'target_company_name'])
        
        for (co_id, co_name), group in company_groups:
            if not co_name: co_name = "Unknown Company Target"
            
            dept_team_targets = {}
            
            # Iterate through Dept/Team targets in this Company scope
            for _, row in group.iterrows():
                dt_id = str(row['target_id'])
                dt_name = row['target_name']
                dt_scope = row['target_scope'] # dept/team
                
                # Fetch aligned goals
                aligned_goals = goals_by_target.get(dt_id, [])
                
                # Get mapped names from target row if available (parse_targets_logic extracts them)
                t_team_id = str(row.get('team_id', ''))
                t_dept_id = str(row.get('dept_id', ''))
                
                # Resolve mapped name based on scope
                mapped_context_name = ""
                if dt_scope == 'dept' and t_dept_id:
                     mapped_context_name = DEPT_ID_MAPPING.get(t_dept_id, "")
                elif dt_scope == 'team' and t_team_id:
                     mapped_context_name = TEAM_ID_MAPPING.get(t_team_id, "")

                goals_dict = {}
                for g in aligned_goals:
                    g_id = str(g.get('id', ''))
                    g_name = g.get('name', '')
                    
                    # Fetch KRs
                    g_krs = krs_by_goal.get(g_id, [])
                    krs_dict = {}
                    for k in g_krs:
                        k_id = str(k.get('id', ''))
                        u_id = str(k.get('user_id', ''))
                        u_name = user_map.get(u_id, "Unknown")
                        krs_dict[k_id] = {
                            'name': k.get('name', ''),
                            'value': k.get('current_value', 0),
                            'top_value': k.get('goal', 0),
                            'unit': k.get('unit', ''),
                            'owner': u_name
                        }
                    
                    goals_dict[g_id] = {
                        'name': g_name,
                        'krs': krs_dict
                    }
                
                # Add to tree IF it has goals (Active Branch)
                if goals_dict:
                     if dt_scope not in dept_team_targets:
                         dept_team_targets[dt_scope] = {}
                     
                     # Store mapped name with logic to handle uniqueness (use unique key but store display label)
                     # We use dt_name (target name) as key, but add extra info
                     dept_team_targets[dt_scope][dt_name] = {
                         'name': dt_name,
                         'mapped_name': mapped_context_name,
                         'goals': goals_dict
                     }
            
            # Only add Company Node if it has active children
            if dept_team_targets:
                tree[co_name] = {
                    'name': co_name,
                    'type': 'company',
                    'target_dept_or_team': dept_team_targets
                }
        
        # 5. Process Personal Goals (No Target)
        if personal_goals:
            # Group by Team/Dept
            personal_groups = {}
            
            for g in personal_goals:
                # Determine group
                g_team_id = str(g.get('team_id', ''))
                g_dept_id = str(g.get('dept_id', ''))
                
                group_name = "Unknown Group"
                if g_team_id and g_team_id != "0":
                    group_name = TEAM_ID_MAPPING.get(g_team_id, f"Team {g_team_id}")
                elif g_dept_id and g_dept_id != "0":
                    group_name = DEPT_ID_MAPPING.get(g_dept_id, f"Dept {g_dept_id}")
                
                if group_name not in personal_groups:
                    personal_groups[group_name] = {}
                
                g_id = str(g.get('id', ''))
                g_name = g.get('name', '')
                
                # Fetch KRs
                g_krs = krs_by_goal.get(g_id, [])
                krs_dict = {}
                for k in g_krs:
                    k_id = str(k.get('id', ''))
                    u_id = str(k.get('user_id', ''))
                    u_name = user_map.get(u_id, "Unknown")
                    krs_dict[k_id] = {
                        'name': k.get('name', ''),
                        'value': k.get('current_value', 0),
                        'top_value': k.get('goal', 0),
                        'unit': k.get('unit', ''),
                        'owner': u_name
                    }
                
                personal_groups[group_name][g_id] = {
                    'name': g_name,
                    'krs': krs_dict
                }
            
            if personal_groups:
                tree['PERSONAL'] = {
                    'name': 'Mục tiêu cá nhân',
                    'type': 'personal',
                    'groups': personal_groups
                }
                
        return tree

    except Exception as e:
        if ctx: ctx.error(f"Error building tree: {e}")
        return {"error": str(e)}

def _convert_to_visual_nodes(tree_data: Dict) -> Dict:
    """Convert API tree format to a generic list of nodes for easier display"""
    root_children = []
    
    # Sort keys to ensure consistent order, but put PERSONAL last if possible
    keys = list(tree_data.keys())
    keys.sort(key=lambda x: 1 if x == 'PERSONAL' else 0) # PERSONAL at end
    
    for top_key in keys:
        node_data = tree_data[top_key]
        node_type = node_data.get('type', 'company')
        
        # Case 1: Company Target Tree
        if node_type == 'company':
            co_name = node_data.get('name', top_key)
            co_node = {'label': f"🏢 {co_name}", 'children': []}
            
            # Dept/Team types
            dept_team_targets = node_data.get('target_dept_or_team', {})
            
            for dtype, targets in dept_team_targets.items():
                # Iterate targets directly
                for t_name_key, t_data in targets.items(): 
                    t_name = t_data.get('name', '')
                    mapped_name = t_data.get('mapped_name', '')
                    
                    if not mapped_name:
                        continue

                    # Format: [Mapped Name] Target Name
                    label_name = f"[{mapped_name}] {t_name}"
                    
                    target_label = f"🎯 {label_name}"
                    t_node = {'label': target_label, 'children': []}
                    
                    goals = t_data.get('goals', {})
                    for g_id, g_data in goals.items():
                        g_node = {'label': f"📝 {g_data['name']}", 'children': []}
                        
                        krs = g_data.get('krs', {})
                        for k_id, k_data in krs.items():
                            val = k_data.get('value', 0)
                            top = k_data.get('top_value', 0)
                            unit = k_data.get('unit', '')
                            
                            try:
                                v_float = float(val) if val else 0.0
                                t_float = float(top) if top else 0.0
                            except:
                                v_float, t_float = 0.0, 0.0
                            
                            stats = ""
                            if v_float != 0 or t_float != 0:
                                stats = f" ({val}/{top} {unit})"
                                
                            owner = k_data.get('owner', '')
                            owner_str = f" - 👤 {owner}" if owner else ""
                            kr_label = f"🔹 {k_data['name']}{stats}{owner_str}"
                            g_node['children'].append({'label': kr_label})
                        
                        t_node['children'].append(g_node)
                    
                    co_node['children'].append(t_node)
            
            root_children.append(co_node)
            
        # Case 2: Personal Goals Branch
        elif node_type == 'personal':
            # PERSONAL -> [Group Name] -> Goal -> KR
            p_node = {'label': f"👤 {node_data.get('name', 'PERSONAL')}", 'children': []}
            
            groups = node_data.get('groups', {})
            for group_name, goals in groups.items():
                group_node = {'label': f"📂 {group_name}", 'children': []}
                
                for g_id, g_data in goals.items():
                    g_node = {'label': f"📝 {g_data['name']}", 'children': []}
                    
                    krs = g_data.get('krs', {})
                    for k_id, k_data in krs.items():
                        val = k_data.get('value', 0)
                        top = k_data.get('top_value', 0)
                        unit = k_data.get('unit', '')
                        
                        try:
                            v_float = float(val) if val else 0.0
                            t_float = float(top) if top else 0.0
                        except:
                            v_float, t_float = 0.0, 0.0
                        
                        stats = ""
                        if v_float != 0 or t_float != 0:
                            stats = f" ({val}/{top} {unit})"
                            
                        owner = k_data.get('owner', '')
                        owner_str = f" - 👤 {owner}" if owner else ""
                        kr_label = f"🔹 {k_data['name']}{stats}{owner_str}"
                        g_node['children'].append({'label': kr_label})
                    
                    group_node['children'].append(g_node)
                
                p_node['children'].append(group_node)
            
            root_children.append(p_node)
        
    return {'label': 'ROOT', 'children': root_children}

@mcp.tool(
    name="get_okr_tree",
    description="Lấy cây mục tiêu OKR (Company > Dept/Team > Goal). Hỗ trợ hiển thị tên BP/Team đã map.",
    tags={"okr", "tree", "visualization"},
    annotations={"readOnlyHint": True}
)
def get_okr_tree(ctx: Context, cycle: str = None) -> Dict:
    """
    Get the hierarchical OKR tree visualization structure.
    Returns a 'visual node' tree: Root -> Company -> Dept/Team -> User Goal -> KRs.
    
    Args:
        cycle (str, optional): Name of the cycle to fetch (e.g. "Q4 2024"). Defaults to current/latest.
    """
    raw_tree = _get_tree_logic(ctx, cycle)
    if "error" in raw_tree: return raw_tree
    

    return _convert_to_visual_nodes(raw_tree)




if __name__ == "__main__":
    mcp.run(transport="http", port=8000)