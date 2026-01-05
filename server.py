from fastmcp import FastMCP, Context
from fastmcp.utilities.types import File
from typing import Dict, List, Optional
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Tokens from environment
GOAL_ACCESS_TOKEN = os.getenv('GOAL_ACCESS_TOKEN')
ACCOUNT_ACCESS_TOKEN = os.getenv('ACCOUNT_ACCESS_TOKEN')

# Create FastMCP server
mcp = FastMCP("OKR Analysis Server")

# Helper functions
def get_cycle_list() -> List[Dict]:
    """Get list of OKR cycles from API"""
    url = "https://goal.base.vn/extapi/v1/cycle/list"
    data = {'access_token': GOAL_ACCESS_TOKEN}
    try:
        response = requests.post(url, data=data, timeout=30)
        if response.status_code != 200:
            return []
        
        cycles_data = response.json()
        quarterly_cycles = []
        
        for cycle in cycles_data.get('cycles', []):
            if cycle.get('metatype') == 'quarterly':
                try:
                    start_time = datetime.fromtimestamp(float(cycle['start_time']))
                    quarterly_cycles.append({
                        'name': cycle['name'],
                        'path': cycle['path'],
                        'start_time': start_time,
                        'formatted_start_time': start_time.strftime('%d/%m/%Y')
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
        data = {"access_token": GOAL_ACCESS_TOKEN, "path": cycle_path, "page": page}
        try:
            response = requests.post(url, data=data, timeout=30)
            if response.status_code != 200:
                break
            
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
    data = {'access_token': GOAL_ACCESS_TOKEN, 'path': cycle_path}
    try:
        response = requests.post(url, data=data, timeout=30)
        if response.status_code != 200: return [], []
        
        cycle_data = response.json()
        goals = cycle_data.get('goals', [])
        
        krs_url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        
        if ctx: ctx.info("Fetching KRs...")
        for page in range(1, 20):
            krs_data = {"access_token": GOAL_ACCESS_TOKEN, "path": cycle_path, "page": page}
            res = requests.post(krs_url, data=krs_data, timeout=30)
            if res.status_code != 200: break
            
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
    data = {"access_token": ACCOUNT_ACCESS_TOKEN}
    try:
        response = requests.post(url, data=data, timeout=30)
        if response.status_code != 200: return {}
        ud = response.json()
        if isinstance(ud, list) and ud: ud = ud[0]
        return {str(u['id']): u['name'] for u in ud.get('users', [])}
    except:
        return {}


def _calculate_shifts_logic(ctx: Optional[Context] = None) -> dict:
    """Core logic to calculate OKR shifts"""
    try:
        if ctx: ctx.info("Starting calculation...")
        cycles = get_cycle_list()
        if not cycles: return {"error": "No OKR cycles found"}
        
        cycle_path = cycles[0]['path']
        if ctx: ctx.info(f"Cycle: {cycles[0]['name']}")
        
        checkins = get_checkins_data(cycle_path, ctx)
        _, krs = get_goals_and_krs(cycle_path, ctx)
        user_names = get_user_names()
        
        today = datetime.now()
        first_day = datetime(today.year, today.month, 1)
        last_month_end = first_day - timedelta(days=1)
        last_month_end = last_month_end.replace(hour=23, minute=59, second=59)
        
        user_okr_data = {}
        for kr in krs:
            uid = str(kr.get('user_id', ''))
            if not uid: continue
            name = user_names.get(uid, f'User_{uid}')
            
            if name not in user_okr_data:
                user_okr_data[name] = {'current': [], 'last': []}
            
            user_okr_data[name]['current'].append(kr.get('current_value', 0))
            
            kr_id = str(kr.get('id', ''))
            valid_checkins = []
            for c in [x for x in checkins if str(x.get('obj_export', {}).get('id', '')) == kr_id]:
                try:
                    t = datetime.fromtimestamp(int(c.get('since', 0)))
                    if t <= last_month_end:
                        valid_checkins.append((t, c.get('current_value', 0)))
                except: pass
            
            if valid_checkins:
                valid_checkins.sort(key=lambda x: x[0], reverse=True)
                user_okr_data[name]['last'].append(valid_checkins[0][1])
            else:
                user_okr_data[name]['last'].append(0)
        
        results = []
        for name, d in user_okr_data.items():
            if not d['current']: continue
            cur = sum(d['current']) / len(d['current'])
            last = sum(d['last']) / len(d['last']) if d['last'] else 0
            
            results.append({
                'user_name': name,
                'monthly_shift': round(cur - last, 2),
                'current_value': round(cur, 2),
                'last_month_value': round(last, 2),
                'kr_count': len(d['current']),
                'reference_date': last_month_end.strftime('%d/%m/%Y')
            })
            
        results.sort(key=lambda x: x['monthly_shift'], reverse=True)
        return {'cycle': cycles[0]['name'], 'total_users': len(results), 'data': results}
    except Exception as e:
        if ctx: ctx.error(str(e))
        return {"error": str(e)}


# MCP Tools

@mcp.tool(annotations={"readOnlyHint": True})
def get_monthly_okr_shifts(ctx: Context) -> dict:
    """
    Calculate monthly OKR shift values for all users.
    Returns monthly OKR movement comparing current values to last month end.
    """
    return _calculate_shifts_logic(ctx)


@mcp.tool(annotations={"readOnlyHint": True})
def get_goal_data_csv(ctx: Context) -> File:
    """
    Generate and display goal_data.csv containing monthly OKR shifts.
    """
    try:
        data = _calculate_shifts_logic(ctx)
        
        if "error" in data:
            df = pd.DataFrame([{"error": data["error"]}])
        else:
            df = pd.DataFrame(data["data"])
            
        csv_path = "goal_data.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        # Return File resource - FastMCP will handle base64 encoding and embedding
        return File(path=csv_path)
    except Exception as e:
        df = pd.DataFrame([{"error": str(e)}])
        df.to_csv("goal_data.csv", index=False)
        return File(path="goal_data.csv")

if __name__ == "__main__":
    mcp.run(transport="http", port=8000)