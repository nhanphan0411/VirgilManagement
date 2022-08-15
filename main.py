import datetime
import bson
from termcolor import colored

import gspread
from gspread_dataframe import set_with_dataframe
from google.auth import default
import pandas as pd
import numpy as np
import re

import os
from datetime import date
from pathlib import Path
import glob

import zipfile
import io

import pytz
NOW = datetime.datetime.now(pytz.timezone('Asia/Bangkok'))

# -----------------------------------------------------------
#  Configuration
# -----------------------------------------------------------

LEARNER_SHEETS = {
    'master': {
        'url': 'https://docs.google.com/spreadsheets/d/1vmZZ6m0UI_sxnxpJ8wuc-M6ccu6qE5nd1kLj1Z64VUY/edit#gid=525468601',
        'worksheet_name': 'All students',
        'columns_row': 0},

    'learnworld': {
        'url': "/content/drive/MyDrive/CoderSchool/PlatformEngineer/VirgilManagement/Data/LWReports",
        'worksheet_name': None,
        'columns_row': None}
        }

MENTOR_SHEETS = None 

SESSION_SHEETS = {
    
    'raw_schedule': {
        'url': 'https://docs.google.com/spreadsheets/d/1vmZZ6m0UI_sxnxpJ8wuc-M6ccu6qE5nd1kLj1Z64VUY/edit#gid=525468601',
        'worksheet_name': 'Mentor_sessions_All',
        'columns_row': 0
    },
    
    'raw_recaps': {
        'url': 'https://docs.google.com/spreadsheets/d/1Uf9yzsy3mA_QeF0h3TI6ZlwXFL_SP6ec9NN80ZlyhJE/edit#gid=1362073476',
        'worksheet_name': 'Mentor Claim Your Session Response',
        'columns_row': 0},

    'processed_recaps': {
        'url': 'https://docs.google.com/spreadsheets/d/1leOO3tvIoyF5uoFr0VZwel1CbQWFKI7GudW6Hir4VwM/edit#gid=574324779',
        'worksheet_name': 'Recap',
        'columns_row': 0
    },

    'processed_schedule': {
        'url': 'https://docs.google.com/spreadsheets/d/1leOO3tvIoyF5uoFr0VZwel1CbQWFKI7GudW6Hir4VwM/edit#gid=574324779',
        'worksheet_name': 'Schedule',
        'columns_row': 0},
    
    'unfit_recaps': {
        'url': 'https://docs.google.com/spreadsheets/d/1leOO3tvIoyF5uoFr0VZwel1CbQWFKI7GudW6Hir4VwM/edit#gid=574324779',
        'worksheet_name': 'Wrong Input',
        'columns_row': 0}
}


COURSE_INFO = {"Web Modules": ['M1', 'M2', 'M3'],
               "Web Minicourses": ['M1.1', 'M1.2', 'M1.3', 'M1.4', 'M2.1', 'M2.2', 'M2.3', 'M3.1'],
               "Web Estimation": {'M1': 10, 'M2': 16, 'M3': 22},
               "DS Modules": ['M1', 'M2', 'M3', 'M4'],
               "DS Minicourses": ['M1.1', 'M1.2', 'M2.1', 'M2.2', 'M3.1', 'M3.2', 'M4.1'],
               "DS Estimation": {'M1': 6, 'M2': 10, 'M3': 14, 'M4': 18}
               }

STAFF_EMAILS = ['hieu.n.pham1210@gmail.com', 'lehoangchauanh@gmail.com']

# -----------------------------------------------------------
#  Credentials
# -----------------------------------------------------------

credentials = {
  "type": "service_account",
  "project_id": "virgil-mentor-recap",
  "private_key_id": "bd99c9cafec7a42fb79f5358d8cfe7ac6ae7bc55",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDedoR5hQegpglb\nl82OzAC2A4mh1SsVKVcYGQkmGpows0Oe9jKGafoAkv1+KKju/OstEncrZCSAirpG\nGIAsHtuuwo3p3yl3Esg9JHaWCaKRelmvzKSd/6AUiRMG4gSsnfyCEY8nTCj6L4aL\nDZQSgK1etO9ul/RkfqJc8G0Y6JBd4IOLkTZlyXAQrcoIGFSSeVsxBg4QfW9LlAlM\nKvNsH3FerBpH8mDWGrIljOwZzC6hFHtpjENc4diTnhF5DTuqMprzlm2nEcbK8Pvt\nLLWJor3+AcY3cfbx6leNOBtx5yXO4TNBPp+M81314tetIRRFi9MHDzjY7fNfxuYd\ngznq7wUFAgMBAAECggEAEUvmteoNc0Slkp0FavZakcxJna8Mx/rsXh/IMdG8rk+p\n0Mi0mfXQbc3/7pirtdUtVdZLNLD3E5V1sTCDeqq1iutujEIBH9RyhCp7G+RI4wW/\nxmbjX7N5107nD1Bwa5lnAhfqaPRHdVhPni0il6Owlx37Rb6DznJ4HkDFQrwGSX4f\nrq6dFF543chvCGdIi0m7YyVqhl69z0wSlta8uUfYKEpsQRumNKb9v6+qRkDHdIXN\nluwh32sBQFCyZiZFrWa8Tpmadp9UVnkId23I0dVnH/lsrkqcHX9bghGvD6NEzMts\nK2UhzUuoQn8DUfLTXie5+YwggZ1fKoZOOxBXkkUjCwKBgQDy3l05Ztu9Lo8wT3g/\n3+6TonggS+1XgSYOGEjePwJEUsNWoYfWjrWdLaWhSIyezcis/s+aUlUeCjryzRhz\nfyhG6QZtoinpED3+r+iqYTa3qucuTOrY/rxPko/KeeLEXbfT7Z/+xKszD6NgykV0\nSRkY+dTbNZilmH98MIdutbrVwwKBgQDqfbYFvQ+MiXr6Ckzip4aM7rMF0Y/OvT9W\nuFNYuTslwT96GLfLTMkqb+6hHADZ8J+gdfRcsskvOqNQ+FaO9fDLsnIA3KYAwb1E\nNK/sIOZcuBwXxGF3bDXX4Kxt8P3z9wqGzLWArq+8QydiI4j9G5E9xKz8j7hwbKWN\nLQlTp7dllwKBgQDjyD4zCg9pnXNaYg5sCgBeovB7ifquKUihy5D8MKhVoDHo3FLk\nnrAjHJ7NZ5fLkUF3Tv0yxaljJUEoW8dk+4uxBfirdn5MQpmCd0WGFusU08tDF4zj\nu/atws3tnudaOHOi5qk6bwYKO86FhvAhIjXRwO8sMVZbaW2X8oPzB6qVtQKBgG+M\nLbK9JVBtdhrMG5cWqmMQigkSj56BWEggl0V1mDV1oISsjYIgnPGbHaF2eNE0Ljte\n4UfXpOzasCYsfMC1Lj84PBM845sojqiLh8SEQhGJY5PKz7LHvzVao6wJIEWWF2PY\nrd7xOpgMOgftb5U4L/d6P/BFTi/s/zQRYIZzGkMxAoGAIl80vToQc0urCh28KsyE\nF6YY/C+sfFuHqKH/a1xivjBlbZ7w9zz+HX/mdnj3stGlqqCSKf/wog1zITci2vFA\nGwBy3p4PiQMDQVALeEzTDiX6fpqXaBg2veyWLSfZ8+k5ivKHx7eb7zLB2YAHkcDD\nhFn24tFEcrkleNR34ujt7cI=\n-----END PRIVATE KEY-----\n",
  "client_email": "virgil-data-bot@virgil-mentor-recap.iam.gserviceaccount.com",
  "client_id": "110539757483865292718",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/virgil-data-bot%40virgil-mentor-recap.iam.gserviceaccount.com"
}

# gspread_client = gspread.service_account(filename='/content/virgil-mentor-recap-bd99c9cafec7.json')
gc = gspread.service_account_from_dict(credentials)
# list all available spreadsheets
spreadsheets = gc.openall()
if spreadsheets:
    print("Available spreadsheets:")
    for spreadsheet in spreadsheets:
        print("Title:", spreadsheet.title, "URL:", spreadsheet.url)
else:
    print("No spreadsheets available")
    print("Please share the spreadsheet with Service Account email")
    print(gc.auth.signer_email)

# -----------------------------------------------------------
#  Learners
# -----------------------------------------------------------

class Learners(object):
    def __init__(self, sheet_dicts):
        """Each data frame input is defined by a dictionary of url, worksheet name,
           and columns_row"""
        self.master_data_dict = sheet_dicts['master']
        self.lw_data_dict = sheet_dicts['learnworld']
        self.lw_map_dir = {
            # Web Virgil
            'ftw-virgil': 'WebVirgil',
            'web-virgil-m11-basic-html-cs': 'WebVirgil/M1.1',
            'programming-with-javascript': 'WebVirgil/M1.2',
            'web-virgil-javascript-for-web': 'WebVirgil/M1.3',
            'wv-web-developing-environment': 'WebVirgil/M1.4',
            'wv-react-fundamental': 'WebVirgil/M2.1',
            'wv-react-ecosystem': 'WebVirgil/M2.2',
            'wv-redux': 'WebVirgil/M2.3',
            'web-virgil-intro-to-nodejs': 'WebVirgil/M3.1',
            # DS Virgil
            'da-virgil': 'DSVirgil',
            'dv-m11-basic-python': 'DSVirgil/M1.1',
            'dv-m12-python-practice': 'DSVirgil/M1.2',
            'dv-m21-db-sql-intro': 'DSVirgil/M2.1',
            'dv-bigquery-advanced-sql': 'DSVirgil/M2.2',
            'dv-m31-pandas-101': 'DSVirgil/M3.1',
            'dv-m32-prepare-your-data': 'DSVirgil/M3.2',
            'dv-m41-analysis-and-visualization': 'DSVirgil/M4.1',
        }
        self.reports_dir = {'Web': 'WebVirgil',
                            'DS': 'DSVirgil'}


    
    # ----- MASTER STUDENT DATA -----
    def preprocess_master_data(self, df):
        df = df[df['#'] != ''].copy(deep=True)
        df.drop(columns=['#', 'DS Check', 'Duplicate check', 'ID', ''], inplace=True)
        df.rename(columns={'Enrollment (start) date': 'Enrollment Date'}, inplace=True)

        # Time related columns
        # Enrollment Date
        df.loc[:, 'Enrollment Date'] = pd.to_datetime(df.loc[:, 'Enrollment Date'], format='%m/%d/%Y')
        df['Enrollment Month'] = df['Enrollment Date'].dt.to_period('M')
        df['Enrollment Week Year'] = df['Enrollment Date'].dt.year.astype('str') + '-W' + df['Enrollment Date'].dt.isocalendar().week.astype('str').str.zfill(2)
        df['Week'] = df['Enrollment Date'].apply(lambda x: ((pd.Timestamp.today() - x) // pd.to_timedelta(7, 'D')) + 1)
        
        # Dropped and Postponed Date
        df.rename(columns={'Postponed/Canceled date': 'Dropout Date'}, inplace=True)
        df.loc[:, 'Dropout Date'] = pd.to_datetime(df.loc[:, 'Dropout Date'], format='%m/%d/%Y', errors='coerce')
        df['Dropout Month'] = df['Dropout Date'].dt.to_period('M')
        df['Dropout Week Year'] = None
        df.loc[df['Dropout Date'].notna(), 'Dropout Week Year'] = df.loc[df['Dropout Date'].notna(), 'Dropout Date'].dt.year.astype('str') + '-W' + df.loc[df['Dropout Date'].notna(), 'Dropout Date'].dt.isocalendar().week.astype('str').str.zfill(2)
        df.loc[df['Dropout Week Year']=='2022-W52', 'Dropout Week Year'] = '2021-W52'
        df['Duration to Drop'] = (df['Dropout Date'] - df['Enrollment Date']).dt.days.astype('float')

        # Text columns
        df.loc[:, 'Status'] = df['Status'].str.lower()
        df['Student email'] = df['Student email'].str.lower().str.strip()

        # Batch data
        def get_batch_in_num(x):
            if x.day <= 15:
                return int(f"{x.strftime('%y%m')}01")  
            else:
                return int(f"{x.strftime('%y%m')}02")

        def get_batch_in_text(x):
            if x.day <= 15:
                return f"{x.strftime('%y-%b')}-Early"
            else:
                return f"{x.strftime('%y-%b')}-Late" 
        
        df['Batch Code'] = df['Enrollment Date'].apply(get_batch_in_num)
        df['Batch'] = df['Enrollment Date'].apply(get_batch_in_text)

        return df
    
    def load_and_preprocess_master_data(self):
        df = Utils.load_gspread(*self.master_data_dict.values())
        # df = Utils.load_gspread(self.master_url, self.master_worksheet_name, self.master_columns)
        df = self.preprocess_master_data(df)
        return df
    

    # ----- LEARNWORLD DATA -----
    def get_zip_files_by_date(self, date=date.today().strftime("%d %b %Y")):
        print(f'Get zip files by {date}')
        zip_files = [
            os.path.join(self.lw_data_dict['url'], f) 
            for f in os.listdir(self.lw_data_dict['url']) 
                if os.path.isfile(os.path.join(self.lw_data_dict['url'], f))
        ]
        result = []
        for filename in zip_files:
            if filename.find(date) > 0:
                result.append(filename)
                print(f'    Zip file found: {filename}')
        return result
    
    def unzip_one_report(self, zipFilePath):
        Logger.info(f'Unzip file {zipFilePath}')
        if (not zipFilePath):
            Logger.error("ERROR: Path to zip file is undefined")
            return []
            
        zf = zipfile.ZipFile(zipFilePath)
        result = []
        
        for name in zf.namelist():
            found = ""
            for index, key in enumerate(self.lw_map_dir):
                if name.find(key) > 0:
                    found = key
            if (found != ""):
                output_filename = name[9:] if name[:9] == "detailed/" else name
                # print(name, '--',output_filename,'--', self.lw_map_dir[found])
                dirpath = os.path.join(LEARNER_SHEETS['learnworld']['url'], self.lw_map_dir[found])
                if (not os.path.exists(dirpath)):
                    Path(dirpath).mkdir(parents=True, exist_ok=True)
                filepath = os.path.join(dirpath, output_filename)
                if (not os.path.exists(filepath)):
                    with open(filepath, 'wb') as out:
                        out.write(zf.read(name))
                        result.append(name)
        print(f'    Unzipped {len(result)} files.')
        return result
    
    def unzip_reports_by_date(self, date):
        """ date in format of day month year. For example:
            8 Aug 2022"""
        for filepath in self.get_zip_files_by_date(date):
            self.unzip_one_report(filepath)
    
    def load_reports_by_date(self, date=date.today().strftime("%Y-%m-%d"), course_name='Web'):
        """Return a JSON file with keys as Mini-Courses"""
        Logger.info(f'Get reports of {course_name} Virgil by {date}')
        filepaths = [
            os.path.join(LEARNER_SHEETS['learnworld']['url'], f)
            for f in glob.glob(os.path.join(LEARNER_SHEETS['learnworld']['url'], self.reports_dir[course_name], '**', '*.xlsx'), recursive=True)
                if os.path.isfile(os.path.join(LEARNER_SHEETS['learnworld']['url'], f)) and (f.find(date) > 0)
        ]
        if (len(filepaths) == 0):
            Logger.error('No reports found.')
            return []
        else:
            print(f'{len(filepaths)} reports found.')
        reports = {item[1].split('/')[1]: None for item in self.lw_map_dir.items() if f'{course_name}Virgil/' in item[1]}
        for module in reports.keys():
            for filepath in filepaths:
                if filepath.find(module) > 0:
                    raw_report = pd.read_excel(filepath, sheet_name='Summary')
                    raw_report['MiniCourse'] = module
                    reports[module] = raw_report
        return reports

    def load_learning_pace_report(self, reports):
        """Report of how many days it takes for a learner to complete a module"""
        pace_report = pd.concat(reports.values())[['Email', 'User Name', 'Tags', 'Course Start Date', 'MiniCourse']]
        return pace_report 

    def preprocess_learning_pace_report(self, pace_report, course):
        pace_report['Email'] = pace_report['Email'].str.strip()
        pace_report = pace_report[~pace_report['Email'].isin(STAFF_EMAILS)]  
        modules = pace_report[pace_report['MiniCourse'].str.endswith(".1")]['MiniCourse'].unique()
        
        # Convert to datetime
        pace_report['Course Start Date'] = pd.to_datetime(pace_report['Course Start Date'])
        
        # Reformat the table
        pace_report = pace_report.pivot_table(index=['Email', 'User Name', 'Tags'],
                                              columns='MiniCourse',
                                              values='Course Start Date').reset_index()
        
        
        # Calculate consumed time (in weeks) for a learner to finish a module
        for i in range(len(modules) - 1):
            time_to_finish = (pace_report[modules[i+1]] - pace_report[modules[i]])
            time_to_finish = ((time_to_finish / pd.to_timedelta(7, 'D')) + 1).apply(np.ceil).astype('float')
            pace_report[f'M{i+1} Finished In'] = time_to_finish

        pace_report['Weeks in Course'] = pd.to_datetime(date.today()) - pace_report['M1.1']
        pace_report['Weeks in Course'] = (pace_report['Weeks in Course'] / pd.to_timedelta(7, 'D') + 1).apply(np.ceil)

        
        def get_module_at(row, number_of_modules):
            module_to_finish = row.isna().sum()
            module_at = number_of_modules - module_to_finish
            return module_at
        
        def get_minicourse_at(row, course):
                return COURSE_INFO[f"{course} Minicourses"][row.notna().sum()-1]

        def get_expected_module_at(weeks, course): 
            expected_module_at = (weeks > np.array(list(COURSE_INFO[f"{course} Estimation"].values()))).sum() + 1
            return expected_module_at
        
        pace_report['Module At'] = pace_report[modules].apply(lambda x: get_module_at(x, len(COURSE_INFO[f"{course} Modules"])), axis=1)
        pace_report['Mini-Course At'] = pace_report[COURSE_INFO[f"{course} Minicourses"]].apply(lambda x: get_minicourse_at(x, course), axis='columns')
        pace_report['Expected Module At'] = pace_report['Weeks in Course'].apply(lambda x: get_expected_module_at(x, course))
        pace_report['On Track'] = pace_report['Module At'] >= pace_report['Expected Module At']

        return pace_report

    # ----- COMBINE LW AND MASTER -----
    def update_progress_report(self, student_master_df, learning_pace_report, course, save=False):
        course_code = {'Web': 'FTW', 'DS': 'DS'}
        students_by_course_df = student_master_df[student_master_df['Class']==course_code[course]]
        progress_df = pd.merge(left=learning_pace_report, 
                            right=students_by_course_df[['Student email', 'Status', 'Student name', 
                                                         'Batch Code', 'Batch', 'Duration to Drop']],
                            left_on='Email',
                            how='right',
                            right_on='Student email').drop(columns=['Email', 'User Name'])[['Student email', 'Student name', 'Tags', 
                                                                                            'Status', 'Batch Code', 'Batch', 
                                                                                            'Duration to Drop'] + list(learning_pace_report.columns[3:])].rename(columns={'Student email': 'Email'})
        
        # Student progress would not take into consideration of To Be Enrolled students
        progress_df = progress_df[progress_df['Status']!='to be enrolled']

        # Correct graduated learners
        progress_df.loc[(progress_df['Expected Module At'] > len(COURSE_INFO[f'{course} Modules'])) & (progress_df['Status'] == 'graduated'), 'On Track'] = True 

        # Update timestamp 
        progress_df['Updated At'] = NOW
        
        # Students by batch and status
        by_status = pd.pivot_table(data=progress_df,
                    columns='Status',
                    index=['Batch Code', 'Batch'],
                    values='Email',
                    aggfunc=len,
                    margins=True).rename(columns={'All': 'total in batch'})

        # Among active student where are they at?
        by_active_module = pd.pivot_table(data=progress_df[progress_df['Status']=='active'],
                    columns='Module At',
                    index=['Batch Code', 'Batch'],
                    values='Email',
                    aggfunc=len,
                    margins=True).drop(columns=['All'])

        if course == 'Web':
            by_active_module.columns = ['active at M1', 'active at M2', 'active at M3']
        elif course == 'DS':
            by_active_module.columns = ['active at M1', 'active at M2', 'active at M3', 'active at M4']

        by_batch = pd.concat([by_status, by_active_module], axis=1).fillna(0).astype(int)
        by_batch = by_batch.drop(index=['All']).reset_index()

        if save:
            # Save
            Utils.save_gspread(progress_df,
                            'https://docs.google.com/spreadsheets/d/1cZQsAuLvKJTCR0JGdC2qDST2tJVIqBfYf819fBFGL0Y/edit#gid=0',
                            f'{course}_LW_Master')
            Logger.success(f'Successfully wrote {course} student progress data')
            
            Utils.save_gspread(by_batch,
                            'https://docs.google.com/spreadsheets/d/1cZQsAuLvKJTCR0JGdC2qDST2tJVIqBfYf819fBFGL0Y/edit#gid=0',
                            f'{course}_LW_Pivot')
            Logger.success(f'Successfully wrote {course} student progress pivot data')

        return progress_df, by_batch

# -----------------------------------------------------------
#  Mentor Sessions
# -----------------------------------------------------------

class MentorSessions():
    def __init__(self, sheets_dict):
        """Each data frame input is defined by a dictionary of url, worksheet name,
           and columns_row"""
        self.raw_schedule_dict = sheets_dict['raw_schedule']
        self.raw_recaps_dict = sheets_dict['raw_recaps']
        self.processed_recaps_dict = sheets_dict['processed_recaps']
        self.processed_schedule_dict = sheets_dict['processed_schedule']
        self.unfit_recaps_dict = sheets_dict['unfit_recaps']

    def preprocess_raw_schedule_data(self, df):
        df['Student email'] = df['Student email'].str.lower().str.strip()
        df['Mentor email'] = df['Mentor email'].str.lower().str.strip()
        return df
    
    def load_and_preprocess_raw_schedule_data(self):
        df = Utils.load_gspread(*self.raw_schedule_dict.values())
        df = self.preprocess_raw_schedule_data(df)
        return df
    
    def preprocess_raw_recaps_data(self, df):
        df.columns = ['Recapped Timestamp', 'Mentor email', 'Class', 
                    'Mentee email', 'On-time', 'Session Timestamp', 'Session Timestamp Absent',
                    'Mini-course At', 'Understanding Level', 'Topics', 'Complete the goal', 
                    'Next goal', 'Message', 'Mentor satisfactation', 
                    'Red flag', 'Note on mentee', 'Action to Red Flag', 'Action Date Log']
        
        # Time Series
        df['Recapped Timestamp'] = pd.to_datetime(df['Recapped Timestamp'])
        df.loc[84, 'Session Timestamp'] = '22/07/2022'
        
        df['Session Timestamp'] = pd.to_datetime(df['Session Timestamp'])
        df.loc[df['Session Timestamp'].isna(), 'Session Timestamp'] = df.loc[df['Session Timestamp'].isna(), 'Recapped Timestamp']
        df['Session Week'] = df['Session Timestamp'].dt.isocalendar().week
        df['Session Week'] = df['Session Week'].astype('int')
        
        # Text Columns
        df['Mentor email'] = df['Mentor email'].str.lower().str.strip()
        df['Mentee email'] = df['Mentee email'].str.lower().str.strip()
        return df

    def load_and_preprocess_raw_recaps_data(self):
        df = Utils.load_gspread(*self.raw_recaps_dict.values())
        df = self.preprocess_raw_recaps_data(df)
        return df

    def load_processed_recaps(self):
        df = Utils.load_gspread(*self.processed_recaps_dict.values())
        return df
    
    def load_processed_schedule(self):
        df = Utils.load_gspread(*self.processed_schedule_dict.values())
        df['Report Week'] = df['Report Week'].astype('int')
        return df

    def load_unfit_recaps(self):
        df = Utils.load_gspread(*self.unfit_recaps_dict.values())
        return df

# -----------------------------------------------------------
#  Utils 
# -----------------------------------------------------------

class Logger:
    @staticmethod
    def error(msg):
        print(colored('ERROR: ' + msg, color='red', attrs=['bold']))

    @staticmethod
    def success(msg):
        print(colored(msg, color='green', attrs=['bold']))

    @staticmethod
    def info(msg):
        print(colored('INFO: ' + msg))

class Utils:
    # GOOGLE SHEET 
    def load_gspread(sheet_url, worksheet_name, columns_row=0):
        global gc
        try:
            sheet = gc.open_by_url(sheet_url)
            worksheet = sheet.worksheet(worksheet_name)
            if (not worksheet):
                Logger.error(f'Worksheet {worksheet_name} not found in {sheet_url}')
                return None
            else:
                rows = worksheet.get_all_values()
                df = pd.DataFrame.from_records(rows[columns_row+1:], columns=rows[columns_row])
                return df
        except Exception as e:
            Logger.error(e)
            return None

    def save_gspread(df, sheet_url, worksheet_name):
        global gc
        try:
            worksheet = gc.open_by_url(sheet_url).worksheet(worksheet_name)
            if (not worksheet):
                Logger.error(f'Worksheet {worksheet_name} not found in {sheet_url}')
                return False
            else:
                set_with_dataframe(worksheet, df)
                Logger.success('Data saved')
                return True
        except Exception as e:
            Logger.error(e)
            return False