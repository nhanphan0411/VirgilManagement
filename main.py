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
NOW = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).strftime('%y-%m-%d %H:%M:%S')


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
            'ftw-virgil': 'WebVirgil',
            'web-virgil-m11-basic-html-cs': 'WebVirgil/M1.1',
            'programming-with-javascript': 'WebVirgil/M1.2',
            'web-virgil-javascript-for-web': 'WebVirgil/M1.3',
            'wv-web-developing-environment': 'WebVirgil/M1.4',
            'wv-react-fundamental': 'WebVirgil/M2.1',
            'wv-react-ecosystem': 'WebVirgil/M2.2',
            'wv-redux': 'WebVirgil/M2.3',
            'web-virgil-intro-to-nodejs': 'WebVirgil/M3.1',
            'web-virgil-expressjs-with-mongodb': 'WebVirgil/M3.2',
            'wv-restful-backend': 'WebVirgil/M3.3',
            'da-virgil': 'DSVirgil',
            'dv-m11-basic-python': 'DSVirgil/M1.1',
            'dv-m12-python-practice': 'DSVirgil/M1.2',
            'dv-m21-db-sql-intro': 'DSVirgil/M2.1',
            'dv-bigquery-advanced-sql': 'DSVirgil/M2.2',
            'dv-m31-pandas-101': 'DSVirgil/M3.1',
            'dv-m32-prepare-your-data': 'DSVirgil/M3.2',
            'dv-m41-analysis-and-visualization': 'DSVirgil/M4.1',
            'dv-m42-report': 'DSVirgil/M4.2',
            'dv-ml-fundamentals': 'DSVirgil/M5.1'
        }
        self.reports_dir = {'Web': 'WebVirgil',
                            'DS': 'DSVirgil'}


    
    # ----- MASTER STUDENT DATA -----
    def preprocess_master_data(self, df):
        df = df[df['x'] != ''].copy(deep=True)
        df.drop(columns=['x', 'ID'], inplace=True)
        df.rename(columns={'Enrollment (start) date': 'Enrollment Date'}, inplace=True)

        # Text columns
        df.loc[:, 'Status'] = df['Status'].str.lower()
        df['Student email'] = df['Student email'].str.lower().str.strip()
        df['Student name'] = df['Student name'].str.title().str.strip()

        # Time related columns
        # Enrollment Date
        df.loc[:, 'Enrollment Date'] = pd.to_datetime(df.loc[:, 'Enrollment Date'])
        df['Enrollment Month'] = df['Enrollment Date'].dt.to_period('M')
        df['Enrollment Week Year'] = df['Enrollment Date'].dt.year.astype('str') + '-W' + df['Enrollment Date'].dt.isocalendar().week.astype('str').str.zfill(2)
        df['Week'] = df['Enrollment Date'].apply(lambda x: ((pd.Timestamp.today() - x) // pd.to_timedelta(7, 'D'))).apply(np.ceil)
        
        # Dropped and Postponed Date
        df.rename(columns={'Postponed/Canceled date': 'Dropout Date'}, inplace=True)
        df.loc[:, 'Dropout Date'] = pd.to_datetime(df.loc[:, 'Dropout Date'], errors='coerce')
        df['Dropout Month'] = df['Dropout Date'].dt.to_period('M')
        df['Dropout Week Year'] = None
        df.loc[df['Dropout Date'].notna(), 'Dropout Week Year'] = df.loc[df['Dropout Date'].notna(), 'Dropout Date'].dt.year.astype('str') + '-W' + df.loc[df['Dropout Date'].notna(), 'Dropout Date'].dt.isocalendar().week.astype('str').str.zfill(2)
        df.loc[df['Dropout Week Year']=='2022-W52', 'Dropout Week Year'] = '2021-W52'
        df.loc[:, 'Duration to Drop'] = None
        df.loc[(df['Status'].isin(['dropped', 'postponed'])), 'Duration to Drop'] = (df.loc[(df['Status'].isin(['dropped', 'postponed'])), 'Dropout Date'] - df.loc[(df['Status'].isin(['dropped', 'postponed'])), 'Enrollment Date']).dt.days
        df.loc[:, 'Duration to Drop'] = df.loc[:, 'Duration to Drop'].astype('float')
        
        # Return date and graduated date
        df.loc[:, 'Expected return date'] = pd.to_datetime(df.loc[:, 'Expected return date'])
        df.loc[:, 'Return Date'] = pd.to_datetime(df.loc[:, 'Return Date'])
        df.loc[:, 'Graduated Date'] = pd.to_datetime(df.loc[:, 'Graduated Date'])

        # Batch data
        def get_batch_in_num(x):
            if x.day < 13:
                return int(f"{x.strftime('%y%m')}01")  
            else:
                return int(f"{x.strftime('%y%m')}02")

        def get_batch_in_text(x):
            if x.day < 13:
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
    # ----- Unzip and load raw reports -----
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
    
    def load_summary_reports_by_date(self, date=date.today().strftime("%Y-%m-%d"), course_name='Web'):
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

    def load_time_spent_reports_by_date(self, date=date.today().strftime("%Y-%m-%d"), course_name='Web'):
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
                    raw_report = pd.read_excel(filepath, sheet_name='Time Spent')
                    raw_report['MiniCourse'] = module
                    reports[module] = raw_report
        return reports

    def load_progress_reports_by_date(self, date=date.today().strftime("%Y-%m-%d"), course_name='Web'):
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
                    raw_report = pd.read_excel(filepath, sheet_name='Progress Status')
                    raw_report['MiniCourse'] = module
                    reports[module] = raw_report
        return reports

    # ----- Preprocess raw reports: Users Data -----

    def load_learning_pace_report(self, reports):
        """Report of how many days it takes for a learner to complete a module"""
        pace_report = pd.concat(reports.values())[['Email', 'User Name', 'Tags', 'Course Start Date', 'Date of certificate', 'MiniCourse']]
        pace_report.rename(columns={'Course Start Date': 'Start', 
                                    'Date of certificate': 'Finish'}, inplace=True)
        return pace_report 

    def preprocess_learning_pace_report(self, pace_report, course):
        pace_report['Email'] = pace_report['Email'].str.strip()
        pace_report = pace_report[~pace_report['Email'].isin(STAFF_EMAILS)]  
        modules = pace_report[pace_report['MiniCourse'].str.endswith(".1")]['MiniCourse'].unique()
        modules = COURSE_INFO[f"{course} Modules"]
        minicourses = COURSE_INFO[f"{course} Minicourses"]
        
        # Convert to datetime
        pace_report.loc[:, 'Start'] = pd.to_datetime(pace_report.loc[:, 'Start'])
        pace_report.loc[pace_report['Finish'] == '-', 'Finish'] = None
        pace_report.loc[:, 'Finish'] = pd.to_datetime(pace_report.loc[:, 'Finish'])
        
        # Reformat the table
        pace_report = pace_report.pivot_table(index=['Email', 'User Name', 'Tags'],
                                              columns='MiniCourse',
                                              values=['Start', 'Finish']).reset_index()
        
        pace_report.columns = list(map(lambda x: x.strip("_"), pace_report.columns.get_level_values(0) + '_' +  pace_report.columns.get_level_values(1)))
        
        # Calculate consumed time (in weeks) for a learner to finish a mini-course
        # Duration = start of latter minicourse - start of previous minicourse 
        # Last minicourse = date of certificate - start of the last minicourse  
        for i in range(len(minicourses)-1):
            time_to_finish = (pace_report[f"Start_{minicourses[i+1]}"] - pace_report[f"Start_{minicourses[i]}"])
            time_to_finish = ((time_to_finish / pd.to_timedelta(7, 'D')) + 1).apply(np.ceil).astype('float')
            pace_report[f'{minicourses[i]} Finished In'] = time_to_finish
        time_to_finish_last_minicourse = pace_report[f"Finish_{minicourses[-1]}"] - pace_report[f"Start_{minicourses[-1]}"]
        time_to_finish_last_minicourse = ((time_to_finish_last_minicourse / pd.to_timedelta(7, 'D')) + 1).apply(np.ceil).astype('float')
        pace_report[f'{minicourses[-1]} Finished In'] = time_to_finish_last_minicourse

        # Calculate consumed time (in weeks) for a learner to finish a module
        # Duration = start of latter module - start of previous module 
        # Last module = date of certificate - start of the last module
        first_minicourses = list(map(lambda x: x+'.1', modules))
        for i in range(len(modules) - 1):
            time_to_finish = (pace_report[f"Start_{first_minicourses[i+1]}"] - pace_report[f"Start_{first_minicourses[i]}"])
            time_to_finish = ((time_to_finish / pd.to_timedelta(7, 'D'))).apply(np.ceil).astype('float')
            pace_report[f'Module {i+1} Finished In'] = time_to_finish
        time_to_finish_last_module = pace_report[f"Finish_{minicourses[-1]}"] - pace_report[f"Start_{first_minicourses[-1]}"]
        time_to_finish_last_module = ((time_to_finish_last_module / pd.to_timedelta(7, 'D'))).apply(np.ceil).astype('float')
        pace_report[f'Module {len(modules)} Finished In'] = time_to_finish_last_module

        # Get Weeks in Course 
        pace_report['Weeks in Course'] = pd.to_datetime(date.today()) - pace_report['Start_M1.1']
        pace_report['Weeks in Course'] = (pace_report['Weeks in Course'] / pd.to_timedelta(7, 'D')).apply(np.ceil)
        
        # Get checkpoint where learners at 
        def get_minicourse_at(row):
            return minicourses[row.notna().sum()-1]
        
        def get_expected_module_at(weeks, course): 
            expected_module_at = (weeks > np.array(list(COURSE_INFO[f"{course} Estimation"].values()))).sum() + 1
            return expected_module_at
        
        pace_report['Mini-Course At'] = pace_report[list(map(lambda x: 'Start_'+x, minicourses))].apply(get_minicourse_at, axis='columns')
        pace_report['Module At'] = pace_report['Mini-Course At'].apply(lambda x: int(x[1]))
        pace_report['Expected Module At'] = pace_report['Weeks in Course'].apply(lambda x: get_expected_module_at(x, course))
        pace_report['On Track'] = pace_report['Module At'] >= pace_report['Expected Module At']

        return pace_report

    # ----- Combine Users Report with Master Data -----
    def update_progress_report(self, student_master_df, learning_pace_report, course, save=False):
        course_code = {'Web': 'FTW', 'DS': 'DS'}
        student_master_df = student_master_df[student_master_df['Status']!='to be enrolled']
        students_by_course_df = student_master_df[student_master_df['Class']==course_code[course]]
        progress_df = pd.merge(left=learning_pace_report, 
                            right=students_by_course_df[['Student email', 'Status', 'Student name', 
                                                         'Batch Code', 'Batch', 'Duration to Drop', 'Learning type']],
                            left_on='Email',
                            how='right',
                            right_on='Student email').drop(columns=['Email', 'User Name'])[['Student email', 'Student name', 'Tags', 'Learning type',
                                                                                            'Status', 'Batch Code', 'Batch', 
                                                                                            'Duration to Drop'] + list(learning_pace_report.columns[3:])].rename(columns={'Student email': 'Email'})
        
        # Correct graduated learners
        progress_df.loc[(progress_df['Status'] == 'graduated'), 'On Track'] = True 

        # Update timestamp 
        progress_df['Updated At'] = NOW
        progress_df = progress_df.sort_values(by=['Learning type', 'Batch Code', 'Email'], ascending=True)
        
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
            by_active_module.columns = ['active at M1', 'active at M2', 'active at M3', 'active at M4', 'active at M5']

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
    
    # ------- Preprocess LW Progress Report in Detail --------
    def get_check_report(self, reports, pace_report):
        """Preprocess one raw progress report and merge with LW summary report
           to get the data of the minicourse the learner is at only.
        """

        df = reports.drop(columns='Started/Completed')
        df = df.reset_index().rename(columns={'index': 'Activity ID'})
        df.dropna(subset=['Type'], inplace=True)
        df = df.melt(id_vars=['Activity ID', 'Learning Activity', 'Type', 'MiniCourse'],
                value_vars=df.columns[2:-1],
                var_name = 'Email',
                value_name = 'Check')
        df['Email'] = df['Email'].apply(lambda x: x.split("-")[1]).str.strip()

        active_df = pace_report[pace_report['Status'] == 'active']
        active_learners = (active_df['Email']+active_df['Mini-Course At']).unique()
        df['key'] = df['Email']+df['MiniCourse']
        df = df[df['key'].isin(active_learners)]
        df = df.drop(columns='key')
        return df

    def get_processed_check_report(self, reports_dict, pace_report):
        """Loop through and preprocess all the reports"""
        df = pd.DataFrame(columns=["Activity ID",
                                "Learning Activity",
                                "Type",
                                "MiniCourse",
                                "Student Email",
                                "Check"])
        for report in reports_dict.values():
            processed_report = self.get_check_report(report, pace_report)
            df = pd.concat([df, processed_report])
        
        return df

    def get_time_report(self, reports, pace_report): 
        """One LW report includes one check sheet and one time sheet.
           Function to preprocess one time sheet to time series format"""
        df = reports.drop(columns='Average Time Spent (For filtered users)')
        df = df.reset_index().rename(columns={'index': 'Activity ID',
                                              'Average Time Spent (For all users)': 'Average Time Spent'})
        df.dropna(subset=['Type'], inplace=True)
        df = df.melt(id_vars=['Activity ID', 'Learning Activity', 'Type', 'Estimated Duration', 'Average Time Spent', 'MiniCourse'],
                value_vars=df.columns[4:-1],
                var_name = 'Email',
                value_name = 'Time Spent')
        df['Email'] = df['Email'].apply(lambda x: x.split("-")[1]).str.strip()
        df.loc[df['Estimated Duration'] == '-', 'Estimated Duration'] = None
        df.loc[df['Average Time Spent'] == '-', 'Average Time Spent'] = None
        df.loc[df['Time Spent'] == '-', 'Time Spent'] = None

        # Convert to minutes
        def to_minutes(x):
            if x in ['nan', 'None']:
                return None
            else:
                hours = int(x[:2])
                minutes = int(x[3:5])
                return hours*60+minutes

        df['Estimated Duration'] = df['Estimated Duration'].astype(str).apply(to_minutes)
        df['Average Time Spent'] = df['Average Time Spent'].astype(str).apply(to_minutes)
        df['Time Spent'] = df['Time Spent'].astype(str).apply(to_minutes)

        df.loc[df['Type'] != 'Video', 'Estimated Duration'] = None
        df.loc[df['Type'] != 'Video', 'Time Spent'] = None

        active_df = pace_report[pace_report['Status'] == 'active']
        active_learners = (active_df['Email']+active_df['Mini-Course At']).unique()
        df['key'] = df['Email']+df['MiniCourse']
        df = df[df['key'].isin(active_learners)]
        df = df.drop(columns='key')

        return df

    def get_processed_time_report(self, reports_dict, pace_report):
        """Function to preprocess all the time reports from LW"""
        df = pd.DataFrame(columns=["Activity ID",
                                "Learning Activity",
                                "Type",
                                "Estimated Duration",
                                "Average Time Spent",
                                "MiniCourse",
                                "Email",
                                "Time Spent"])
        for report in reports_dict.values():
            processed_report = self.get_time_report(report, pace_report)
            df = pd.concat([df, processed_report])
        
        return df

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
        df['Session Timestamp Absent'] = pd.to_datetime(df['Session Timestamp Absent'])
        df.loc[df['Session Timestamp Absent'].notna(), 'Session Timestamp'] = df.loc[df['Session Timestamp Absent'].notna(), 'Session Timestamp Absent']
        df.loc[df['Session Timestamp'].isna(), 'Session Timestamp'] = df.loc[df['Session Timestamp'].isna(), 'Recapped Timestamp']
        df.drop(columns='Session Timestamp Absent', inplace=True)
        df.loc[df['Session Timestamp'].dt.year < 2022, 'Session Timestamp'] = df.loc[df['Session Timestamp'].dt.year < 2022, 'Recapped Timestamp']
        df['Session Week'] = df['Session Timestamp'].dt.isocalendar().week
        df['Session Week'] = df['Session Week'].astype('int')
        df['Session Year Month'] = df['Session Timestamp'].dt.to_period('M')
                
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

    def save_gspread(df, sheet_url, worksheet_name, clear_sheet=False):
        global gc
        try:
            worksheet = gc.open_by_url(sheet_url).worksheet(worksheet_name)
            if (not worksheet):
                Logger.error(f'Worksheet {worksheet_name} not found in {sheet_url}')
                return False
            else:
                if clear_sheet==True:
                    worksheet.clear()
                set_with_dataframe(worksheet, df)
                Logger.success('Data saved')
                return True
        except Exception as e:
            Logger.error(e)
            return False