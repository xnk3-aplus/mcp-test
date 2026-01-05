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


def get_targets_data(cycle_path: str, ctx: Optional[Context] = None) -> List[Dict]:
    """Get targets data for alignment info"""
    # Fetch targets - this is simplified as direct target API might be complex to get all
    # We will try to fetch targets related to KRs if possible, or just skip if too complex for standalone
    # Based on goal.py, it constructs target_df. For standalone, we might skip deep target hierarchy join if not essential,
    # but user requested target columns.
    # We'll return empty list for now if strict target structure is needed, or try to implementation basic fetch if possible.
    # Given standalone constraints, we will leave target columns as None/Empty for now unless we implement full target scanning.
    return []

def _get_full_data_logic(ctx: Optional[Context] = None) -> List[Dict]:
    """Core logic to get full detailed data"""
    try:
        if ctx: ctx.info("Starting full data fetch...")
        cycles = get_cycle_list()
        if not cycles: return [{"error": "No OKR cycles found"}]
        
        cycle_path = cycles[0]['path']
        if ctx: ctx.info(f"Cycle: {cycles[0]['name']}")
        
        # 1. Fetch all raw data
        checkins = get_checkins_data(cycle_path, ctx)
        goals, krs = get_goals_and_krs(cycle_path, ctx)
        user_names = get_user_names()
        
        if not goals and not krs:
             return [{"error": "No Goals or KRs found in cycle"}]

        # 2. Build Maps
        goal_map = {str(g['id']): g for g in goals}
        user_map = user_names
        
        # 3. Join Data
        full_data = []
        

        # Helper to get safe string
        def safe_get(d, k, default=''):
            return str(d.get(k, default)) if d.get(k) is not None else default

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
            # Handle case with goals but no KRs if necessary, but typically Goals have KRs
            pass

        for kr in krs:
            kr_id = str(kr.get('id', ''))
            # Safety: if no KR ID, skip or generate placeholder? goal.py skips or errors. We skip.
            if not kr_id: continue

            goal_id = str(kr.get('goal_id', ''))
            goal = goal_map.get(goal_id, {})
            
            # Common Goal/KR data
            goal_user_id = str(goal.get('user_id', ''))
            
            base_row = {
                'goal_id': goal_id,
                'goal_name': goal.get('name', ''),
                'goal_content': goal.get('content', ''),
                'goal_since': convert_time(goal.get('since')),
                'goal_current_value': goal.get('current_value', 0),
                'goal_user_id': goal_user_id,
                'goal_target_id': str(goal.get('target_id', '')),
                'kr_id': kr_id,
                'kr_name': kr.get('name', ''),
                'kr_content': kr.get('content', ''),
                'kr_since': convert_time(kr.get('since')),
                'kr_current_value': kr.get('current_value', 0),
                'goal_user_name': user_map.get(goal_user_id, f"User_{goal_user_id}"),
                'goal_username': '', 
                'list_goal_id': '',
                # Target placeholders
                'target_id': '', 'target_company_id': '', 'target_company_name': '',
                'target_name': '', 'target_scope': '', 'target_dept_id': '',
                'target_dept_name': '', 'target_team_id': '', 'target_team_name': ''
            }
            
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
                full_data.append(row)
            else:
                for c in kr_checkins:
                    row = base_row.copy()
                    checkin_ts = c.get('since', '')
                    row.update({
                        'checkin_id': str(c.get('id', '')),
                        'checkin_name': c.get('name', ''),
                        'checkin_since': convert_time(checkin_ts),
                        'checkin_since_timestamp': checkin_ts,
                        'cong_viec_tiep_theo': c.get('form', [{}])[0].get('value', '') if c.get('form') else '', # Better extraction
                        'checkin_target_name': '', 
                        'checkin_kr_current_value': c.get('current_value', 0),
                        'checkin_user_id': str(c.get('user_id', ''))
                    })
                    full_data.append(row)
                    
        return full_data
        
    except Exception as e:
        if ctx: ctx.error(f"Error generating full data: {e}")
        return [{"error": str(e)}]


@mcp.tool(annotations={"readOnlyHint": True})
def get_full_okr_data(ctx: Context) -> List[Dict]:
    """
    Get the full monthly OKR data dataset as detailed JSON.
    Returns a list of records merging Goals, KRs, and Check-ins.
    Fields include: goal_id, goal_name, kr_name, checkin_since, cong_viec_tiep_theo, etc.
    """
    return _get_full_data_logic(ctx)





def _get_tree_logic(ctx: Optional[Context] = None) -> Dict:
    """Build hierarchical OKR tree"""
    try:
        if ctx: ctx.info("Building OKR tree...")
        cycles = get_cycle_list()
        if not cycles: return {"error": "No OKR cycles found"}
        cycle_path = cycles[0]['path']
        
        # 1. Fetch Hierarchy (Targets)
        url = "https://goal.base.vn/extapi/v1/cycle/get.full"
        data = {'access_token': GOAL_ACCESS_TOKEN, 'path': cycle_path}
        res = requests.post(url, data=data, timeout=30)
        if res.status_code != 200: return {"error": "Failed to fetch cycle data"}
        
        cycle_data = res.json()
        raw_company_targets = cycle_data.get('targets', [])
        
        # 2. Fetch User Goals & KRs
        goals, krs = get_goals_and_krs(cycle_path, ctx)
        
        # 3. Build lookup maps
        # Map: KR ID -> List of User Goals (User Goals link to a 'Target ID' which is actually a Dept/Team KR ID)
        goals_by_target = {}
        for g in goals:
            tid = str(g.get('target_id', ''))
            if tid:
                if tid not in goals_by_target: goals_by_target[tid] = []
                goals_by_target[tid].append(g)
                
        krs_by_goal = {}
        for k in krs:
            gid = str(k.get('goal_id', ''))
            if gid:
                if gid not in krs_by_goal: krs_by_goal[gid] = []
                krs_by_goal[gid].append(k)

        # 4. Construct Tree
        tree = {}
        
        for co_target in raw_company_targets:
            c_name = co_target.get('name', 'Unnamed Company Target')
            
            # Sub-targets (Dept/Team targets) are in 'cached_objs'
            dept_team_targets = {}
            
            for dt_target in co_target.get('cached_objs', []):
                dt_id = str(dt_target.get('id', ''))
                dt_name = dt_target.get('name', '')
                dt_type = dt_target.get('scope', 'dept') # dept or team or other
                
                # Fetch aligned User Goals
                aligned_goals = goals_by_target.get(dt_id, [])
                
                goals_dict = {}
                for g in aligned_goals:
                    g_id = str(g.get('id', ''))
                    g_name = g.get('name', '')
                    
                    # Fetch KRs for this goal
                    g_krs = krs_by_goal.get(g_id, [])
                    krs_dict = {}
                    for k in g_krs:
                        k_id = str(k.get('id', ''))
                        krs_dict[k_id] = {
                            'name': k.get('name', ''),
                            'value': k.get('current_value', 0),
                            'top_value': k.get('goal', 0),
                            'unit': k.get('unit', '')
                        }
                        
                    goals_dict[g_id] = {
                        'name': g_name,
                        'krs': krs_dict
                    }
                    
                # Add to dept/team structure
                if dt_type not in dept_team_targets:
                     dept_team_targets[dt_type] = {}
                
                dept_team_targets[dt_type][dt_name] = { # Key by name as requested
                    'name': dt_name,
                    'goals': goals_dict
                }

            tree[c_name] = {
                'name': c_name,
                'target_dept_or_team': dept_team_targets
            }
            
        return tree

    except Exception as e:
        if ctx: ctx.error(f"Error building tree: {e}")
        return {"error": str(e)}


@mcp.tool(annotations={"readOnlyHint": True})
def get_okr_tree(ctx: Context) -> Dict:
    """
    Get the hierarchical OKR tree structure.
    Returns: Company Target -> Dept/Team Target -> User Goal -> KRs.
    """
    return _get_tree_logic(ctx)


if __name__ == "__main__":
    mcp.run(transport="http", port=8000)