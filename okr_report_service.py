import pandas as pd
import numpy as np
import requests
import json
import warnings
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import pytz
import calendar
from excel_generator import OKRSheetGenerator
from table_client import TableAPIClient
import os
from dotenv import load_dotenv

# Re-use config or load from env if needed, server.py likely loads env too
load_dotenv()

# Constants matching goal_new.py
QUARTER_START_MONTHS = [1, 4, 7, 10]
MIN_WEEKLY_CHECKINS = 2
REQUEST_TIMEOUT = 30
MAX_PAGES_KRS = 50
MAX_PAGES_CHECKINS = 100

# Access Tokens (Passed from server or loaded from env)
GOAL_ACCESS_TOKEN = os.getenv('GOAL_ACCESS_TOKEN')
ACCOUNT_ACCESS_TOKEN = os.getenv('ACCOUNT_ACCESS_TOKEN')

hcm_tz = pytz.timezone('Asia/Ho_Chi_Minh')

# Department and Team ID Mappings (Should probably be shared or duplicated)
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

class DateUtils:
    """Utility class for date calculations"""
    
    @staticmethod
    def get_last_friday_date() -> datetime:
        """Get last Friday date - always returns Friday of previous week"""
        today = datetime.now()
        current_weekday = today.weekday()
        days_to_monday_current_week = current_weekday
        monday_current_week = today - timedelta(days=days_to_monday_current_week)
        monday_previous_week = monday_current_week - timedelta(days=7)
        friday_previous_week = monday_previous_week + timedelta(days=4)
        return friday_previous_week.replace(hour=23, minute=59, second=59)

    @staticmethod
    def get_quarter_start_date() -> datetime:
        """Get current quarter start date"""
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    @staticmethod
    def get_last_month_end_date() -> datetime:
        """Get last day of previous month"""
        today = datetime.now()
        first_day_current_month = datetime(today.year, today.month, 1)
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        return last_day_previous_month.replace(hour=23, minute=59, second=59)

    @staticmethod
    def convert_timestamp_to_datetime(timestamp) -> Optional[str]:
        """Convert timestamp to datetime string in Asia/Ho_Chi_Minh timezone"""
        if timestamp is None or timestamp == '' or timestamp == 0:
            return None
        try:
            dt_utc = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            tz_hcm = pytz.timezone('Asia/Ho_Chi_Minh')
            dt_hcm = dt_utc.astimezone(tz_hcm)
            return dt_hcm.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None

    @staticmethod
    def should_calculate_monthly_shift() -> bool:
        return True
    
    @staticmethod
    def is_last_week_of_month() -> bool:
        """Check if current week is the last week of month"""
        now = datetime.now()
        weeks = DateUtils._get_weeks_in_current_month()
        
        if not weeks:
            return False
        
        last_week = weeks[-1]
        return last_week['start_date'] <= now.date() <= last_week['end_date']
    
    @staticmethod
    def is_week_4_or_5_of_quarter_start_month() -> bool:
        """
        Kiểm tra xem hiện tại có phải tuần 4 hoặc 5 của tháng đầu quý không
        """
        now = datetime.now()
        current_month = now.month
        
        # Kiểm tra xem có phải tháng đầu quý không (1, 4, 7, 10)
        if current_month not in QUARTER_START_MONTHS:
            return False
        
        # Lấy danh sách các tuần trong tháng hiện tại
        weeks = DateUtils._get_weeks_in_current_month()
        
        if not weeks:
            return False
        
        # Tìm tuần hiện tại
        current_week_number = None
        for week in weeks:
            if week['start_date'] <= now.date() <= week['end_date']:
                current_week_number = week['week_number']
                break
        
        # Kiểm tra xem có phải tuần 4 hoặc 5 không
        return current_week_number in [4, 5]

    @staticmethod
    def _get_weeks_in_current_month():
        """
        Lấy tất cả các tuần trong tháng hiện tại
        Quy tắc: Nếu ngày đầu/cuối tháng rơi vào thứ 2-6, vẫn tính là tuần của tháng đó
        """
        now = datetime.now()
        year = now.year
        month = now.month
        
        # Ngày đầu và cuối tháng
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, calendar.monthrange(year, month)[1])
        
        weeks = []
        current_date = first_day
        
        while current_date <= last_day:
            week_start = current_date - timedelta(days=current_date.weekday())
            week_start = max(week_start, first_day)
            
            week_end = week_start + timedelta(days=6)
            week_end = min(week_end, last_day) # Không được sau ngày cuối tháng
            
            weeks.append({
                'week_number': len(weeks) + 1,
                'start_date': week_start.date(),
                'end_date': week_end.date(),
                'week_range': f"{week_start.strftime('%d/%m')} - {week_end.strftime('%d/%m')}"
            })
            
            current_date = week_end + timedelta(days=1)
        
        return weeks

class User:
    """User class for OKR tracking"""
    
    def __init__(self, user_id, name, co_OKR=1, checkin=0, dich_chuyen_OKR=0, score=0):
        self.user_id = str(user_id)
        self.name = name
        self.co_OKR = co_OKR
        self.checkin = checkin
        self.dich_chuyen_OKR = dich_chuyen_OKR
        self.score = score
        self.OKR = {month: 0 for month in range(1, 13)}

    def calculate_score(self):
        """Calculate score based on criteria"""
        score = 0.5
        
        if self.checkin == 1:
            score += 0.5
        
        if self.co_OKR == 1:
            score += 1
        
        movement = self.dich_chuyen_OKR
        movement_scores = [
            (10, 0.15), (25, 0.25), (30, 0.5), (50, 0.75),
            (80, 1.25), (99, 1.5), (float('inf'), 2.5)
        ]
        
        for threshold, points in movement_scores:
            if movement < threshold:
                score += points
                break
        
        self.score = round(score, 2)

class UserManager:
    """Manages user data and calculations"""
    
    def __init__(self, account_df, krs_df, checkin_df, cycle_df=None, final_df=None, users_with_okr_names=None, monthly_okr_data=None):
        self.account_df = account_df
        self.krs_df = krs_df
        self.checkin_df = checkin_df
        self.cycle_df = cycle_df
        self.final_df = final_df
        self.users_with_okr_names = users_with_okr_names or set()
        self.monthly_okr_data = monthly_okr_data or []
        
        self.user_name_map = self._create_user_name_map()
        self.users = self._create_users()

    def _create_user_name_map(self) -> Dict[str, str]:
        user_map = {}
        if not self.account_df.empty and 'id' in self.account_df.columns and 'name' in self.account_df.columns:
            for _, row in self.account_df.iterrows():
                user_map[str(row['id'])] = row.get('name', 'Unknown')
        return user_map

    def _create_users(self) -> Dict[str, User]:
        users = {}
        if not self.account_df.empty and 'id' in self.account_df.columns and 'name' in self.account_df.columns:
            for _, row in self.account_df.iterrows():
                user_id = str(row.get('id'))
                name = row.get('name', 'Unknown')
                has_okr = 1 if name in self.users_with_okr_names else 0
                users[user_id] = User(user_id, name, co_OKR=has_okr)
        return users

    def _get_monthly_weekly_criteria_details(self, user_id) -> dict:
        user_name = self.user_name_map.get(str(user_id), '')
        if not user_name:
            return {'meets_criteria': False}
        
        now = datetime.now()
        current_month_weeks = DateUtils._get_weeks_in_current_month()
        current_month_year = f"{now.year}-{now.month:02d}"
        
        if self.final_df is None or self.final_df.empty:
            return {'meets_criteria': False}
        
        user_checkins = self.final_df[
            (self.final_df['goal_user_name'] == user_name) &
            (self.final_df['checkin_since'].notna()) &
            (self.final_df['checkin_since'] != '')
        ].copy()
        
        if user_checkins.empty:
            return {'meets_criteria': False}
        
        user_checkins['checkin_date'] = pd.to_datetime(user_checkins['checkin_since']).dt.date
        user_checkins['checkin_month_year'] = pd.to_datetime(user_checkins['checkin_since']).dt.strftime('%Y-%m')
        
        current_month_checkins = user_checkins[user_checkins['checkin_month_year'] == current_month_year].copy()
        
        if current_month_checkins.empty:
            return {'meets_criteria': False}
        
        def get_week_number(checkin_date):
            for week in current_month_weeks:
                if week['start_date'] <= checkin_date <= week['end_date']:
                    return week['week_number']
            return None
        
        current_month_checkins['week_number'] = current_month_checkins['checkin_date'].apply(get_week_number)
        user_weekly_checkins = current_month_checkins.groupby(['week_number']).size().reset_index(name='checkins_count')
        total_checkins = len(current_month_checkins)
        
        meets_criteria = total_checkins > 3
        return {'meets_criteria': meets_criteria}

    def update_okr_movement(self):
        monthly_shift_map = {}
        if self.monthly_okr_data:
            for data in self.monthly_okr_data:
                monthly_shift_map[data['user_name']] = data['okr_shift_monthly']
        
        for user in self.users.values():
            user_name = user.name
            if user_name in monthly_shift_map:
                user.dich_chuyen_OKR = round(monthly_shift_map[user_name], 2)
            else:
                user.dich_chuyen_OKR = 0

    def calculate_scores(self):
        is_last_week = DateUtils.is_last_week_of_month()
        
        for user in self.users.values():
            if is_last_week:
                criteria_details = self._get_monthly_weekly_criteria_details(user.user_id)
                meets_criteria = criteria_details['meets_criteria']
                user.checkin = 1 if meets_criteria else 0
            else:
                user.checkin = 0
            
            user.calculate_score()
            
    def get_users(self) -> List[User]:
        return list(self.users.values())

class GoalAPIClient:
    """Client for handling API requests"""
    
    def __init__(self, goal_token: str, account_token: str):
        self.goal_token = goal_token
        self.account_token = account_token

    def _make_request(self, url: str, data: Dict, description: str = "") -> requests.Response:
        try:
            response = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Error {description}: {e}")
            raise e

    def get_filtered_members(self) -> pd.DataFrame:
        url = "https://account.base.vn/extapi/v1/group/get"
        data = {'access_token_v2': self.account_token, "path": "nvvanphong"}
        try:
            response = self._make_request(url, data, "fetching account members")
            response_data = response.json()
            members = response_data.get('group', {}).get('members', [])
            return pd.DataFrame([
                {'id': str(m.get('id', '')), 'name': m.get('name', ''), 'username': m.get('username', '')}
                for m in members
            ])
        except:
            return pd.DataFrame()

    def get_cycle_list(self) -> List[Dict]:
        url = "https://goal.base.vn/extapi/v1/cycle/list"
        data = {'access_token_v2': self.goal_token}
        response = self._make_request(url, data, "fetching cycle list")
        data = response.json()
        quarterly_cycles = []
        for cycle in data.get('cycles', []):
            if cycle.get('metatype') == 'quarterly':
                try:
                    start_time = datetime.fromtimestamp(float(cycle['start_time']), tz=timezone.utc)
                    quarterly_cycles.append({
                        'name': cycle['name'],
                        'path': cycle['path'],
                        'start_time': start_time,
                        'formatted_start_time': start_time.strftime('%d/%m/%Y')
                    })
                except:
                    continue
        return sorted(quarterly_cycles, key=lambda x: x['start_time'], reverse=True)
    
    def get_account_users(self) -> pd.DataFrame:
        url = "https://account.base.vn/extapi/v1/users"
        data = {'access_token_v2': self.account_token}
        try:
            response = self._make_request(url, data, "fetching account users")
            json_response = response.json()
            if isinstance(json_response, list) and len(json_response) > 0:
                json_response = json_response[0]
            account_users = json_response.get('users', [])
            return pd.DataFrame([{'id': str(user['id']), 'name': user['name'], 'username': user['username']} for user in account_users])
        except:
             return pd.DataFrame()

    def get_target_sub_goal_ids(self, target_id: str) -> List[str]:
        url = "https://goal.base.vn/extapi/v1/target/get"
        data = {'access_token_v2': self.goal_token, 'id': str(target_id)}
        try:
            response = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                response_data = response.json()
                if response_data and 'target' in response_data:
                    cached_objs = response_data['target'].get('cached_objs', [])
                    if isinstance(cached_objs, list):
                        return [str(item.get('id')) for item in cached_objs if 'id' in item]
            return []
        except:
            return []

    def get_goals_data(self, cycle_path: str) -> pd.DataFrame:
        url = "https://goal.base.vn/extapi/v1/cycle/get.full"
        data = {'access_token_v2': self.goal_token, 'path': cycle_path}
        try:
            response = self._make_request(url, data, "fetching goals data")
            data = response.json()
            
            def extract_form_value(form_array, field_name):
                if not form_array or not isinstance(form_array, list): return ""
                for item in form_array:
                    if item.get('name') == field_name:
                        return item.get('value', item.get('display', ""))
                return ""

            goals_data = []
            for goal in data.get('goals', []):
                form_data = goal.get('form', [])
                dept_id = str(goal.get('dept_id', '0'))
                team_id = str(goal.get('team_id', '0'))
                
                dept_name = "" if (dept_id == "0" or dept_id == 0 or not dept_id) else DEPT_ID_MAPPING.get(dept_id, "")
                team_name = "" if (team_id == "0" or team_id == 0 or not team_id) else TEAM_ID_MAPPING.get(team_id, "")
                
                goals_data.append({
                    'goal_id': str(goal.get('id')),
                    'goal_name': goal.get('name', 'Unknown Goal'),
                    'goal_since': DateUtils.convert_timestamp_to_datetime(goal.get('since')),
                    'goal_current_value': goal.get('current_value', 0),
                    'goal_user_id': str(goal.get('user_id', '')),
                    'goal_target_id': str(goal.get('target_id', '')) if goal.get('target_id') else '',
                    'dept_id': dept_id, 'team_id': team_id,
                    'dept_name': dept_name, 'team_name': team_name,
                    'Mức độ đóng góp vào mục tiêu công ty': extract_form_value(form_data, 'Mức độ đóng góp vào mục tiêu công ty'),
                    'Mức độ ưu tiên mục tiêu của Quý': extract_form_value(form_data, 'Mức độ ưu tiên mục tiêu của Quý'),
                    'Tính khó/tầm ảnh hưởng đến hệ thống': extract_form_value(form_data, 'Tính khó/tầm ảnh hưởng đến hệ thống'),
                })
            return pd.DataFrame(goals_data)
        except:
             return pd.DataFrame()

    def parse_targets_data(self, cycle_path: str) -> pd.DataFrame:
        url = "https://goal.base.vn/extapi/v1/cycle/get.full"
        data = {'access_token_v2': self.goal_token, 'path': cycle_path}
        try:
            response = self._make_request(url, data, "fetching targets data")
            response_data = response.json()
            if not response_data or 'targets' not in response_data:
                return pd.DataFrame()
            
            raw_targets = response_data.get('targets', [])
            company_targets_map = {}
            for t in raw_targets:
                if t.get('scope') == 'company':
                    company_targets_map[str(t.get('id', ''))] = {'id': str(t.get('id', '')), 'name': t.get('name', '')}
            
            def extract_form_data(target_obj):
                form_data = {"Mức độ đóng góp vào mục tiêu công ty": "", "Mức độ ưu tiên mục tiêu của Quý": "", "Tính khó/tầm ảnh hưởng đến hệ thống": ""}
                if 'form' in target_obj and isinstance(target_obj['form'], list):
                    for item in target_obj['form']:
                        key = item.get('name')
                        val = item.get('value')
                        if key: form_data[key] = val
                return form_data

            targets_map = {}
            for t in raw_targets:
                t_id = str(t.get('id', ''))
                scope = t.get('scope', '')
                parent_id = str(t.get('parent_id') or '')
                
                def create_base_data(obj, parent_info=None):
                    data = {
                        'target_id': str(obj.get('id', '')),
                        'target_name': obj.get('name', ''),
                        'target_scope': obj.get('scope', ''),
                        'target_company_id': parent_info['id'] if parent_info else None,
                        'target_company_name': parent_info['name'] if parent_info else None,
                        'target_dept_id': None, 'target_dept_name': None,
                        'target_team_id': None, 'target_team_name': None,
                        'team_id': str(obj.get('team_id', '')), 'dept_id': str(obj.get('dept_id', ''))
                    }
                    data.update(extract_form_data(obj))
                    return data

                if scope in ['dept', 'team'] and parent_id in company_targets_map:
                    parent = company_targets_map[parent_id]
                    targets_map[t_id] = create_base_data(t, parent)
                elif scope == 'company':
                    if t_id not in targets_map:
                        targets_map[t_id] = create_base_data(t, {'id': t_id, 'name': t.get('name', '')})
                    if 'cached_objs' in t and isinstance(t['cached_objs'], list):
                        for kr in t['cached_objs']:
                            targets_map[str(kr.get('id', ''))] = create_base_data(kr, {'id': t_id, 'name': t.get('name', '')})
                elif t_id not in targets_map:
                    parent_info = company_targets_map.get(parent_id)
                    targets_map[t_id] = create_base_data(t, parent_info)

            all_targets = list(targets_map.values())
            for target_data in all_targets:
                if target_data['target_scope'] == 'dept':
                    target_data['target_dept_id'] = target_data['target_id']
                    target_data['target_dept_name'] = target_data['target_name']
                elif target_data['target_scope'] == 'team':
                    target_data['target_team_id'] = target_data['target_id']
                    target_data['target_team_name'] = target_data['target_name']
                target_data['list_goal_id'] = self.get_target_sub_goal_ids(target_data['target_id'])
            
            return pd.DataFrame(all_targets)
        except:
            return pd.DataFrame()

    def get_krs_data(self, cycle_path: str) -> pd.DataFrame:
        url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        for page in range(1, MAX_PAGES_KRS + 1):
             data = {'access_token_v2': self.goal_token, "path": cycle_path, "page": page}
             try:
                response = self._make_request(url, data, f"loading KRs page {page}")
                response_data = response.json()
                if isinstance(response_data, list) and response_data: response_data = response_data[0]
                krs_list = response_data.get("krs", [])
                if not krs_list: break
                for kr in krs_list:
                    all_krs.append({
                        'kr_id': str(kr.get('id', '')),
                        'kr_name': kr.get('name', 'Unknown KR'),
                        'kr_since': DateUtils.convert_timestamp_to_datetime(kr.get('since')),
                        'kr_current_value': kr.get('current_value', 0),
                        'kr_user_id': str(kr.get('user_id', '')),
                        'goal_id': kr.get('goal_id'),
                    })
             except:
                 break
        return pd.DataFrame(all_krs)

    def get_all_checkins(self, cycle_path: str) -> List[Dict]:
        url = "https://goal.base.vn/extapi/v1/cycle/checkins"
        all_checkins = []
        for page in range(1, MAX_PAGES_CHECKINS + 1):
            data = {'access_token_v2': self.goal_token, "path": cycle_path, "page": page}
            try:
                response = self._make_request(url, data, f"loading checkins page {page}")
                response_data = response.json()
                if isinstance(response_data, list) and response_data: response_data = response_data[0]
                checkins = response_data.get('checkins', [])
                if not checkins: break
                all_checkins.extend(checkins)
            except:
                break
        return all_checkins

class OKRReportService:
    def __init__(self):
        self.goal_token = GOAL_ACCESS_TOKEN
        self.account_token = ACCOUNT_ACCESS_TOKEN
        self.api_client = GoalAPIClient(self.goal_token, self.account_token)
        self.okr_calculator = OKRCalculator() 

    def generate_report(self) -> bytes:
        cycles = self.api_client.get_cycle_list()
        if not cycles: raise Exception("No OKR cycles found")
        
        # Default to latest cycle
        cycle = cycles[0]
        cycle_path = cycle['path']
        cycle_name = cycle['name']
        
        print(f"Generating report for cycle: {cycle_name}")
        
        # 1. Fetch Data
        goals_df = self.api_client.get_goals_data(cycle_path)
        krs_df = self.api_client.get_krs_data(cycle_path)
        target_df = self.api_client.parse_targets_data(cycle_path)
        all_checkins = self.api_client.get_all_checkins(cycle_path)
        account_df = self.api_client.get_filtered_members()
        
        # Table Scores
        try:
             table_client = TableAPIClient()
             table_scores_map = table_client.get_checkin_scores()
        except:
             table_scores_map = {}
        
        # Process Checkins
        checkin_df = self._extract_checkin_data(all_checkins, table_scores_map)
        
        # Merge Data
        if goals_df.empty or krs_df.empty: return b""
        
        merged_df = pd.merge(goals_df, krs_df, on='goal_id', how='left', suffixes=('_goal', '_kr'))
        if 'kr_id' not in merged_df.columns: merged_df['kr_id'] = None
        
        all_users = self.api_client.get_account_users()
        if not all_users.empty:
            id_to_name = dict(zip(all_users['id'].astype(str), all_users['name']))
            merged_df['goal_user_name'] = merged_df['goal_user_id'].map(id_to_name)
        else:
            merged_df['goal_user_name'] = 'Unknown'
            
        users_with_okr_names = set(merged_df['goal_user_name'].dropna().unique())
        
        final_df = pd.merge(merged_df, checkin_df, on='kr_id', how='left')
        
        if not target_df.empty:
             final_df = pd.merge(final_df, target_df, left_on='kr_id', right_on='target_id', how='left', suffixes=('', '_target'))
             # Sub-goal merging simplification for MVP
        
        final_df = self._clean_final_data(final_df)
        
        # Calculate Logic
        user_manager = UserManager(account_df, krs_df, checkin_df, final_df=final_df, users_with_okr_names=users_with_okr_names)
        
        # Calculate Monthly Shift
        monthly_okr_data = self._calculate_monthly_shifts(final_df, DateUtils.get_last_month_end_date())
        user_manager.monthly_okr_data = monthly_okr_data
        user_manager.update_okr_movement()
        user_manager.calculate_scores()
        
        # Prepare Excel Data
        final_users = user_manager.get_users()
        users_data = []
        for user in final_users:
            stats = {
                'okr_shift_display': f"{user.dich_chuyen_OKR}%" if user.dich_chuyen_OKR else "0%",
                'has_okrs': 'Yes' if user.co_OKR else 'No',
                'checkin_score_val': user.checkin * 4, # Example mapping based on score logic
                'checkin_score': user.checkin,
                'score': user.score
            }
             # Map ranges for checkmarks
            shift = user.dich_chuyen_OKR
            if shift < 25: stats['shift_lt_25'] = True
            elif shift < 50: stats['shift_25_50'] = True
            elif shift < 75: stats['shift_50_75'] = True
            elif shift <= 100: stats['shift_75_100'] = True
            else: stats['shift_gt_100'] = True
            
            # Additional heuristic mappings for Excel checkmarks would go here
            # For MVP, we pass what we have
            
            users_data.append({'name': user.name, 'stats': stats})
            
        # Generate Excel
        generator = OKRSheetGenerator()
        excel_buffer = generator.generate_excel(users_data, cycle_name)
        
        return excel_buffer.getvalue()

    def _extract_checkin_data(self, all_checkins, scores_map):
         # Simplified version of DataProcessor.extract_checkin_data
         checkin_list = []
         for checkin in all_checkins:
            try:
                user_id = str(checkin.get('user_id', ''))
                since_timestamp = checkin.get('since', '')
                score_key = f"{user_id}_{since_timestamp}"
                checkin_list.append({
                    'checkin_id': checkin.get('id'),
                    'checkin_name': checkin.get('name'),
                    'checkin_since': DateUtils.convert_timestamp_to_datetime(since_timestamp),
                    'checkin_target_name': checkin.get('obj_export', {}).get('name', ''),
                    'kr_id': str(checkin.get('obj_export', {}).get('id', '')),
                    'checkin_kr_current_value': checkin.get('current_value', 0),
                    'goal_user_name': '', # Filled later
                    'next_action_score': scores_map.get(score_key, 0)
                })
            except: continue
         return pd.DataFrame(checkin_list)

    def _clean_final_data(self, df):
        if 'kr_current_value' in df.columns:
            df['kr_current_value'] = pd.to_numeric(df['kr_current_value'], errors='coerce').fillna(0)
        if 'checkin_kr_current_value' in df.columns:
            df['checkin_kr_current_value'] = pd.to_numeric(df['checkin_kr_current_value'], errors='coerce').fillna(0)
        return df

    def _calculate_monthly_shifts(self, final_df, reference_date):
        # Simplified shift calc
        users = final_df['goal_user_name'].dropna().unique()
        data = []
        for user in users:
            user_df = final_df[final_df['goal_user_name'] == user]
            current_val = self.okr_calculator.calculate_current_value(user_df)
            ref_val, _ = self.okr_calculator.calculate_reference_value(reference_date, user_df)
            shift = current_val - ref_val
            data.append({'user_name': user, 'okr_shift_monthly': shift})
        return data

class OKRCalculator:
    def calculate_current_value(self, df):
        try:
            unique_goals = df.groupby('goal_name')['goal_current_value'].first().reset_index()
            unique_goals['goal_current_value'] = pd.to_numeric(unique_goals['goal_current_value'], errors='coerce').fillna(0)
            return unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
        except: return 0

    def calculate_reference_value(self, reference_date, df):
        # Simplified reference value
        try:
             df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')
             # Logic to find latest checkin before date...
             # For MVP, returning random or 0 if complex
             # Implementing basic logic:
             unique_krs = df['kr_id'].unique()
             vals = []
             for kr in unique_krs:
                 kr_df = df[df['kr_id'] == kr]
                 valid = kr_df[kr_df['checkin_since_dt'] <= reference_date]
                 if not valid.empty:
                     valid = valid.sort_values('checkin_since_dt')
                     vals.append(float(valid.iloc[-1]['checkin_kr_current_value']))
                 else:
                     vals.append(0)
             return np.mean(vals) if vals else 0, []
        except: return 0, []
