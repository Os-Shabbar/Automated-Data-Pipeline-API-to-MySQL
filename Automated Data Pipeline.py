import requests
import pandas as pd
import pymysql
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings('ignore')

# MySQL connection
MYSQL_HOST = 'host'
MYSQL_USER = 'username'
MYSQL_PASSWORD = 'password'  
MYSQL_DB = 'database'

# API Configuration
API_TOKEN = "API_Token"
BASE_URL = "API_URL"
PROJECT_UID_1 = "Project_UID_1"
PROJECT_UID_2 = "Project_UID_2"
PROJECT_UID_3 = "Project_UID_3"


def fetch_kobo_data(asset_uid):
    """Fetch data from Kobo API"""
    headers = {"Authorization": f"Token {API_TOKEN}"}
    url = f"{BASE_URL}{asset_uid}/data.json"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data for {asset_uid}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception fetching {asset_uid}: {e}")
        return None


def merge_columns_vectorized(df, columns_list, new_column_name):
    """Vectorized column merging"""
    available_columns = [col for col in columns_list if col in df.columns]
    if available_columns:
        df[new_column_name] = df[available_columns].fillna('').astype(str).agg(' '.join, axis=1)
        df[new_column_name] = df[new_column_name].str.strip()
    else:
        df[new_column_name] = ''
    return df


def merge_municipality_village(df, column_list, new_col):
    """Merge municipality/village columns efficiently"""
    available = [col for col in column_list if col in df.columns]
    if available:
        df[new_col] = df[available].apply(
            lambda row: '/'.join([str(val) for val in row if pd.notnull(val) and str(val).strip() != '']), 
            axis=1
        )
    else:
        df[new_col] = ''
    return df


def process_unhcr_columns(df):
    """Process UNHCR columns efficiently"""
    unhcr_mappings = [
        (["Scan_UNHCR_1", "UNHCR_number", "Jordanian_ID1"], "UNHCR_1"),
        (["Scan_UNHCR2", "UNHCR_2", "Jordanian_ID2"], "UNHCR_2"),
        (["Scan_UNHCR3", "UNHCR_3", "Jordanian_ID3"], "UNHCR_3"),
        (["Scan_UNHCR4", "UNHCR_4", "Jordanian_ID4"], "UNHCR_4"),
        (["Scan_UNHCR5", "UNHCR_5", "Jordanian_ID5"], "UNHCR_5")
    ]
    
    for columns, target in unhcr_mappings:
        df = merge_columns_vectorized(df, columns, target)
    
    return df


def clean_and_process_data():
    """Main data processing function with parallel API calls"""
    print("Fetching data from Kobo API...")
    
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(fetch_kobo_data, PROJECT_UID_1),
            executor.submit(fetch_kobo_data, PROJECT_UID_2),
            executor.submit(fetch_kobo_data, PROJECT_UID_3)
        ]
        project_data_1, project_data_2, project_data_3 = [f.result() for f in futures]
    
    
    if not project_data_1 or 'results' not in project_data_1:
        print("No data found for project 1")
        return tuple([pd.DataFrame()] * 6)
    
    if not project_data_2 or 'results' not in project_data_2:
        print("No data found for project 2")
        return tuple([pd.DataFrame()] * 6)
    
    if not project_data_3 or 'results' not in project_data_3:
        print("No data found for project 3")
        return tuple([pd.DataFrame()] * 6)

    print("Processing data...")
    

    df1 = pd.DataFrame(project_data_1['results'])
    df2 = pd.DataFrame(project_data_2['results'])
    df3 = pd.DataFrame(project_data_3['results'])
    
    
    df1.columns = [col.split("/")[-1] for col in df1.columns]
    df2.columns = [col.split("/")[-1] for col in df2.columns]
    df3.columns = [col.split("/")[-1] for col in df3.columns]
    
    
    municipality_columns = ["In_Irbid", "In_Ajloun", "In_Jerash", "In_Mafraq", "In_Amman"]
    village_columns = [
        "IQ_North_Irbid", "IQ_East_Irbid", "IQ_West_Irbid", "IQ_City_Centre",
        "In_Bani_Obaid", "In_Kora", "In_Wastiyya", "In_Mazar", "In_Ramtha",
        "In_Taibeh", "In_Bani_Kinana", "In_Aghwar_Shamaliyah", "In_Koforanjah",
        "In_Ajloun_Qasabeh", "In_Sakra", "In_Arjan", "In_Jerash_Qasabeh", "In_Qada_Borma",
        "In_Qada_Al_Mastaba", "In_Mafraq_Qasebeh", "In_Mansheh", "In_Irhab", "In_Balama_a",
        "In_Hosha", "In_Sama_Al_Sarhan", "In_Um_AL_jimal", "In_Sabah", "In_Um_AL_Quttein",
        "In_North_west_badiah", "Badiah_Shamaliyah", "Dier_Alkahf", "In_Jizah", "In_marka",
        "In_Jamaa", "In_Quaismeh", "In_Muaqar", "In_qasabetAmman", "In_Wadisyr", "In_sahab", "In_naour"
    ]
    
    
    df1 = merge_municipality_village(df1, municipality_columns, 'Municipality')
    df1 = merge_municipality_village(df1, village_columns, 'Village')
    df2 = merge_municipality_village(df2, municipality_columns, 'Municipality')
    df2 = merge_municipality_village(df2, village_columns, 'Village')
    
    
    df1 = df1.rename(columns={'Benef_Code': 'REG_USE'})
    df2 = df2.rename(columns={'Benef_Code': 'REG_USE'})
    df3 = df3.rename(columns={'Reg_number': 'REG_USE'})
    
    
    df3['family_Size'] = (df3['men'].fillna('').astype(str) + ' ' + 
                          df3['women'].fillna('').astype(str) + ' ' + 
                          df3['girls'].fillna('').astype(str) + ' ' + 
                          df3['boys'].fillna('').astype(str)).str.strip()
    
    
    df1['Contract_Code'] = 'CFR-' + df1['REG_USE'].astype(str)
    df2['Contract_Code'] = 'FLEX-' + df2['REG_USE'].astype(str)
    
    
    df1 = process_unhcr_columns(df1)
    df2 = process_unhcr_columns(df2)
    
    
    if '_geolocation' in df1.columns:
        df1[['longitude', 'latitude']] = pd.DataFrame(df1['_geolocation'].tolist(), index=df1.index)
    else:
        df1[['longitude', 'latitude']] = ['', '']
        
    if '_geolocation' in df2.columns:
        df2[['longitude', 'latitude']] = pd.DataFrame(df2['_geolocation'].tolist(), index=df2.index)
    else:
        df2[['longitude', 'latitude']] = ['', '']
    
    
    social_columns = [
        'REG_USE', 'today', 'Staff_1', 'HoH_Full_Name_Arabic', 'HoH_Full_Name_English', 
        'Phone_1', 'Phone_2', 'UNHCR_1', 'UNHCR_2', 'UNHCR_3', 'UNHCR_4', 'UNHCR_5',
        'Activity', 'nationality', 'Governorate', 'Municipality', 'Village', 'Address', 
        '_submission_time', 'Contract_Code', 'longitude', 'latitude'
    ]
    
    social_df1 = df1.reindex(columns=social_columns, fill_value='')
    social_df2 = df2.reindex(columns=social_columns, fill_value='')
    combined_social = pd.concat([social_df1, social_df2], ignore_index=True)
    
    combined_social = combined_social[
        (combined_social['REG_USE'].notna()) & 
        (combined_social['REG_USE'] != '') &
        (combined_social['Activity'] == 'case_val')
    ].copy()
    
    combined_social['_submission_time'] = pd.to_datetime(combined_social['_submission_time'], errors='coerce')
    combined_social = (combined_social
                       .sort_values(by='_submission_time', ascending=False)
                       .drop_duplicates(subset='REG_USE', keep='first')
                       .fillna(''))
    
    
    landlord_columns = [
        'REG_USE', 'Name_Owner_ENG', 'Name_Owner_AR', 'Phone_Owner', 'Phone_Owner2',
        'ID_Owner', 'PoA_auth', 'POA_Eng', 'POA_Ar', 'PoA_phone', 'PoA_phone2', 
        'POA_ID', '_submission_time', 'Activity', 'Contract_Code'
    ]
    
    landlord_df1 = df1.reindex(columns=landlord_columns, fill_value='')
    landlord_df2 = df2.reindex(columns=landlord_columns, fill_value='')
    combined_landlord = pd.concat([landlord_df1, landlord_df2], ignore_index=True)
    
    combined_landlord = combined_landlord[
        (combined_landlord['REG_USE'].notna()) & 
        (combined_landlord['REG_USE'] != '') &
        (combined_landlord['Activity'].isin(['case_val', 'landlord_data'])) &
        (combined_landlord['Name_Owner_ENG'].notna()) &
        (combined_landlord['Name_Owner_ENG'] != '')
    ].copy()
    
    combined_landlord['_submission_time'] = pd.to_datetime(combined_landlord['_submission_time'], errors='coerce')
    combined_landlord = (combined_landlord
                         .sort_values(by='_submission_time', ascending=False)
                         .drop_duplicates(subset='REG_USE', keep='first')
                         .fillna(''))
    
    
    agreement_columns_CFR = [
        'REG_USE', 'Signed_Rent', 'total_people', 'people', 'Month_support',
        'Month_Covered', 'Monthly_amount_covered_by_NRC', 'total_fund',
        '_submission_time', 'Activity', 'Contract_Code'
    ]
    
    agreement_columns_FLEX = [
        'REG_USE', 'rent_prop', 'total_people_agreement', 'total_people', 'Vulnerability_Score',
        'total_fund_cal', 'total_boq_cost', 'total_EE_cost', 'rent_support_remaining_val',
        'rent_agreed', 'months_nrc_paid_rent_val', 'Repairs_months', 'Activity', 
        '_submission_time', 'Contract_Code'
    ]
    
    agreement_df1 = df1.reindex(columns=agreement_columns_CFR, fill_value='')
    agreement_df2 = df2.reindex(columns=agreement_columns_FLEX, fill_value='')
    agreement_df2 = agreement_df2.dropna(subset=['total_fund_cal'])
    
    agreement_df2 = agreement_df2.rename(columns={
        "rent_agreed": "Month_support",
        "total_fund_cal": "total_fund"
    })
    
    agreement_df1['family_Size'] = (agreement_df1['total_people'].fillna('').astype(str) + ' ' + 
                                     agreement_df1['people'].fillna('').astype(str)).str.strip()
    agreement_df2['family_Size'] = (agreement_df2['total_people'].fillna('').astype(str) + ' ' + 
                                     agreement_df2['total_people_agreement'].fillna('').astype(str)).str.strip()
    
    combined_agreement = pd.concat([agreement_df1, agreement_df2], ignore_index=True)
    
    combined_agreement = combined_agreement[
        (combined_agreement['REG_USE'].notna()) & 
        (combined_agreement['REG_USE'] != '') &
        (combined_agreement['Activity'].isin(['case_val', 'agreement']))
    ].copy()
    
    combined_agreement['_submission_time'] = pd.to_datetime(combined_agreement['_submission_time'], errors='coerce')
    combined_agreement = (combined_agreement
                          .sort_values(by='_submission_time', ascending=False)
                          .drop_duplicates(subset='REG_USE', keep='first')
                          .fillna(''))
    
    
    numeric_cols = ['Month_Covered', 'months_nrc_paid_rent_val', 'Repairs_months', 
                    'total_fund', 'total_boq_cost']
    for col in numeric_cols:
        if col in combined_agreement.columns:
            combined_agreement[col] = pd.to_numeric(combined_agreement[col], errors='coerce')
    
    
    def format_months(x):
        if pd.notnull(x) and x > 0 and np.isfinite(x):
            return f"{int(x)} months and {int((x - int(x)) * 30)} days"
        return None
    
    combined_agreement['Month_Coverd_Rent_CFR'] = combined_agreement['Month_Covered'].apply(format_months)
    combined_agreement['Month_Coverd_Rent_FLEX'] = combined_agreement['months_nrc_paid_rent_val'].apply(format_months)
    combined_agreement['Months_Coverd_Period_FLEX'] = (
        combined_agreement['months_nrc_paid_rent_val'].fillna(0) + 
        combined_agreement['Repairs_months'].fillna(0)
    ).apply(format_months)
    
    
    combined_agreement['Percent_70'] = np.where(
        combined_agreement['rent_prop'] == 'no',
        combined_agreement['total_boq_cost'] * 0.7,
        combined_agreement['total_fund'] * 0.7
    )
    
    combined_agreement['Percent_30'] = np.where(
        combined_agreement['rent_prop'] == 'no',
        combined_agreement['total_boq_cost'] * 0.3,
        combined_agreement['total_fund'] * 0.3
    )
    
    
    combined_agreement['Month_Covered'] = combined_agreement['Month_Covered'].round(2)
    combined_agreement['months_nrc_paid_rent_val'] = combined_agreement['months_nrc_paid_rent_val'].round(2)
    combined_agreement['Month_support'] = combined_agreement['Month_support'].replace('', None)
    combined_agreement = combined_agreement.replace({np.nan: None, np.inf: None, -np.inf: None})
    
    
    update_columns = [
        'REG_USE', 'HoH_Full_Name_English', 'HoH_Full_Name_Arabic', 'Phone', 'Phone_2',
        'family_Size', 'UNHCR_number', 'UNHCR_2', 'UNHCR_3', 'UNHCR_4', 'UNHCR_5',
        'Activity', '_submission_time'
    ]
    
    update_df = df3.reindex(columns=update_columns, fill_value='')
    update_df = update_df[
        (update_df['REG_USE'].notna()) &
        (update_df['Activity'] == 'Beneficiary_Update')
    ].copy()
    
    update_df['_submission_time'] = pd.to_datetime(update_df['_submission_time'], errors='coerce')
    update_df = (update_df
                 .sort_values(by='_submission_time', ascending=False)
                 .drop_duplicates(subset='REG_USE', keep='first')
                 .fillna(''))
    
    
    sign_CFR_columns = [
        'REG_USE', 'Contract_Code', 'Activity', 'Staff_1', 'Lease_Start_Date', 'Sign_Contract',
        'Payment_Method', 'PoA_Sign_Session', 'POA_Eng', 'POA_Ar', 'PoA_phone',
        'PoA_phone2', 'POA_ID', '_submission_time'
    ]
    
    sign_FLEX_columns = [
        'REG_USE', 'Contract_Code', 'Activity', 'Staff_1', 'renting', 'Lease_Start_Date', 
        'Sign_Flex_Contract', 'Payment_Method', 'PoA_Sign_Session', 'POA_Eng', 'POA_Ar', 
        'PoA_phone', 'PoA_phone2', 'POA_ID', '_submission_time'
    ]
    
    sign_CFR_df = df1.reindex(columns=sign_CFR_columns, fill_value='')
    sign_FLEX_df = df2.reindex(columns=sign_FLEX_columns, fill_value='')
    
    sign_CFR_df = sign_CFR_df[sign_CFR_df['Activity'] == 'ctrct_sign'].copy()
    sign_FLEX_df = sign_FLEX_df[sign_FLEX_df['Activity'] == 'flex_sign'].copy()
    sign_FLEX_df = sign_FLEX_df.rename(columns={'Sign_Flex_Contract': 'Sign_Contract'})
    
    combined_sign = pd.concat([sign_CFR_df, sign_FLEX_df], ignore_index=True)
    combined_sign = combined_sign[
        (combined_sign['REG_USE'].notna()) & 
        (combined_sign['REG_USE'] != '')
    ].copy()
    
    combined_sign['_submission_time'] = pd.to_datetime(combined_sign['_submission_time'], errors='coerce')
    combined_sign['Lease_Start_Date'] = pd.to_datetime(combined_sign['Lease_Start_Date'], errors='coerce')
    combined_sign = (combined_sign
                     .sort_values(by='_submission_time', ascending=False)
                     .drop_duplicates(subset='REG_USE', keep='first')
                     .replace({np.nan: None}))
    
    
    cancel_columns = [
        'REG_USE', 'Contract_Code', 'Activity', 'Staff_1', 'Why_Cancel', 'Cancel_Note',
        'Notes', '_id', '_submission_time'
    ]
    
    cancel_df1 = df1.reindex(columns=cancel_columns, fill_value='')
    cancel_df2 = df2.reindex(columns=cancel_columns, fill_value='')
    combined_cancel = pd.concat([cancel_df1, cancel_df2], ignore_index=True)
    
    combined_cancel = combined_cancel[
        (combined_cancel['REG_USE'].notna()) &
        (combined_cancel['Activity'] == 'Cancel')
    ].copy()
    
    combined_cancel['_submission_time'] = pd.to_datetime(combined_cancel['_submission_time'], errors='coerce')
    combined_cancel = (combined_cancel
                       .sort_values(by='_submission_time', ascending=False)
                       .drop_duplicates(subset='REG_USE', keep='first')
                       .fillna(''))
    
    print(f"Processed: {len(combined_social)} social, {len(combined_landlord)} landlord, "
          f"{len(combined_agreement)} agreement, {len(update_df)} updates, "
          f"{len(combined_sign)} sign, {len(combined_cancel)} cancel records")
    
    return (combined_social, combined_landlord, combined_agreement, 
            update_df, combined_sign, combined_cancel)


def insert_into_mysql(combined_social, combined_landlord, combined_agreement, 
                      update_df, combined_sign, combined_cancel):
    """Insert data into MySQL using batch operations"""
    try:
        print("Connecting to MySQL...")
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cursor = connection.cursor()
        
        
        insert_query_social = """
            INSERT INTO social (REG_USE, Full_Name_Arabic, Full_Name_English, Phone_1, Phone_2, 
                                UNHCR_1, UNHCR_2, UNHCR_3, UNHCR_4, UNHCR_5, Activity, Nationality, 
                                Governorate, Municipality, Village, Submission_date, Staff_1, Address, 
                                Contract_Code, longitude, latitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Full_Name_Arabic = VALUES(Full_Name_Arabic),
            Full_Name_English = VALUES(Full_Name_English),
            Phone_1 = VALUES(Phone_1),
            Phone_2 = VALUES(Phone_2),
            UNHCR_1 = VALUES(UNHCR_1),
            UNHCR_2 = VALUES(UNHCR_2),
            UNHCR_3 = VALUES(UNHCR_3),
            UNHCR_4 = VALUES(UNHCR_4),
            UNHCR_5 = VALUES(UNHCR_5),
            Activity = VALUES(Activity),
            Nationality = VALUES(Nationality),
            Governorate = VALUES(Governorate),
            Municipality = VALUES(Municipality),
            Village = VALUES(Village),
            Submission_date = VALUES(Submission_date),
            Staff_1 = VALUES(Staff_1),
            Address = VALUES(Address),
            Contract_Code = VALUES(Contract_Code),
            longitude = VALUES(longitude),
            latitude = VALUES(latitude)
        """
        
        insert_landlord_query = """
            INSERT INTO landlord_data (REG_USE, Name_Owner_ENG, Name_Owner_AR, Phone_Owner, 
                                       Phone_Owner2, ID_Owner, PoA_auth, POA_Eng, POA_Ar, 
                                       PoA_phone, PoA_phone2, POA_ID, _submission_time, 
                                       Activity, Contract_Code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Name_Owner_ENG = VALUES(Name_Owner_ENG),
            Name_Owner_AR = VALUES(Name_Owner_AR),
            Phone_Owner = VALUES(Phone_Owner),
            Phone_Owner2 = VALUES(Phone_Owner2),
            ID_Owner = VALUES(ID_Owner),
            PoA_auth = VALUES(PoA_auth),
            POA_Eng = VALUES(POA_Eng),
            POA_Ar = VALUES(POA_Ar),
            PoA_phone = VALUES(PoA_phone),
            PoA_phone2 = VALUES(PoA_phone2),
            POA_ID = VALUES(POA_ID),
            _submission_time = VALUES(_submission_time),
            Activity = VALUES(Activity),
            Contract_Code = VALUES(Contract_Code)
        """
        
        insert_agreement_query = """
            INSERT INTO agreement (REG_USE, Month_Covered, Signed_Rent, Monthly_amount_covered_by_NRC,
                                   months_nrc_paid_rent_val, rent_prop, total_boq_cost, Activity,
                                   rent_support_remaining_val, total_fund, Repairs_months, total_EE_cost,
                                   Month_support, Vulnerability_Score, family_Size, Month_Coverd_Rent_CFR,
                                   Month_Coverd_Rent_FLEX, Months_Coverd_Period_FLEX, Percent_70,
                                   Percent_30, _submission_time, Contract_Code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Month_Covered = VALUES(Month_Covered),
            Signed_Rent = VALUES(Signed_Rent),
            Monthly_amount_covered_by_NRC = VALUES(Monthly_amount_covered_by_NRC),
            months_nrc_paid_rent_val = VALUES(months_nrc_paid_rent_val),
            rent_prop = VALUES(rent_prop),
            total_boq_cost = VALUES(total_boq_cost),
            Activity = VALUES(Activity),
            rent_support_remaining_val = VALUES(rent_support_remaining_val),
            total_fund = VALUES(total_fund),
            Repairs_months = VALUES(Repairs_months),
            total_EE_cost = VALUES(total_EE_cost),
            Month_support = VALUES(Month_support),
            Vulnerability_Score = VALUES(Vulnerability_Score),
            family_Size = VALUES(family_Size),
            Month_Coverd_Rent_CFR = VALUES(Month_Coverd_Rent_CFR),
            Month_Coverd_Rent_FLEX = VALUES(Month_Coverd_Rent_FLEX),
            Months_Coverd_Period_FLEX = VALUES(Months_Coverd_Period_FLEX),
            Percent_70 = VALUES(Percent_70),
            Percent_30 = VALUES(Percent_30),
            _submission_time = VALUES(_submission_time),
            Contract_Code = VALUES(Contract_Code)
        """
        
        insert_sign_query = """
            INSERT INTO sign (REG_USE, Contract_Code, Activity, Staff_1, Lease_Start_Date,
                              Sign_Contract, renting, Payment_Method, PoA_Sign_Session, POA_Eng,
                              POA_Ar, PoA_phone, PoA_phone2, POA_ID, _submission_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Contract_Code = VALUES(Contract_Code),
            Activity = VALUES(Activity),
            Staff_1 = VALUES(Staff_1),
            Lease_Start_Date = VALUES(Lease_Start_Date),
            Sign_Contract = VALUES(Sign_Contract),
            renting = VALUES(renting),
            Payment_Method = VALUES(Payment_Method),
            PoA_Sign_Session = VALUES(PoA_Sign_Session),
            POA_Eng = VALUES(POA_Eng),
            POA_Ar = VALUES(POA_Ar),
            PoA_phone = VALUES(PoA_phone),
            PoA_phone2 = VALUES(PoA_phone2),
            POA_ID = VALUES(POA_ID),
            _submission_time = VALUES(_submission_time)
        """
        
        insert_cancel_query = """
            INSERT INTO cancel (REG_USE, Contract_Code, Activity, Staff_1, Why_Cancel,
                                Cancel_Note, Notes, _submission_time, _id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Contract_Code = VALUES(Contract_Code),
            Activity = VALUES(Activity),
            Staff_1 = VALUES(Staff_1),
            Why_Cancel = VALUES(Why_Cancel),
            Cancel_Note = VALUES(Cancel_Note),
            Notes = VALUES(Notes),
            _submission_time = VALUES(_submission_time),
            _id = VALUES(_id)
        """
        
        update_query = """
            UPDATE social
            SET Full_Name_Arabic = %s, Full_Name_English = %s, Phone_1 = %s, Phone_2 = %s,
                UNHCR_1 = %s, UNHCR_2 = %s, UNHCR_3 = %s, UNHCR_4 = %s, UNHCR_5 = %s,
                Activity = %s, Submission_date = %s
            WHERE REG_USE = %s
        """
        
        
        if not combined_social.empty:
            
            social_values = [
                (row['REG_USE'], row['HoH_Full_Name_Arabic'], row['HoH_Full_Name_English'],
                 row['Phone_1'], row['Phone_2'], row['UNHCR_1'], row['UNHCR_2'],
                 row['UNHCR_3'], row['UNHCR_4'], row['UNHCR_5'], row['Activity'],
                 row['nationality'], row['Governorate'], row['Municipality'], row['Village'],
                 row['_submission_time'], row['Staff_1'], row['Address'], row['Contract_Code'],
                 row['longitude'], row['latitude'])
                for _, row in combined_social.iterrows() if str(row['REG_USE']).strip()
            ]
            if social_values:
                cursor.executemany(insert_query_social, social_values)
                
        
        
        if not combined_landlord.empty:
            
            landlord_values = [
                (row['REG_USE'], row['Name_Owner_ENG'], row['Name_Owner_AR'],
                 row['Phone_Owner'], row['Phone_Owner2'], row['ID_Owner'],
                 row['PoA_auth'], row['POA_Eng'], row['POA_Ar'], row['PoA_phone'],
                 row['PoA_phone2'], row['POA_ID'], row['_submission_time'],
                 row['Activity'], row['Contract_Code'])
                for _, row in combined_landlord.iterrows() if str(row['REG_USE']).strip()
            ]
            if landlord_values:
                cursor.executemany(insert_landlord_query, landlord_values)
                
        
        
        if not combined_agreement.empty:
            
            agreement_values = [
                (row['REG_USE'], row['Month_Covered'], row['Signed_Rent'],
                 row['Monthly_amount_covered_by_NRC'], row['months_nrc_paid_rent_val'],
                 row['rent_prop'], row['total_boq_cost'], row['Activity'],
                 row['rent_support_remaining_val'], row['total_fund'], row['Repairs_months'],
                 row['total_EE_cost'], row['Month_support'], row['Vulnerability_Score'],
                 row['family_Size'], row['Month_Coverd_Rent_CFR'], row['Month_Coverd_Rent_FLEX'],
                 row['Months_Coverd_Period_FLEX'], row['Percent_70'], row['Percent_30'],
                 row['_submission_time'], row['Contract_Code'])
                for _, row in combined_agreement.iterrows() if str(row['REG_USE']).strip()
            ]
            if agreement_values:
                cursor.executemany(insert_agreement_query, agreement_values)
                
        
        
        if not combined_sign.empty:
            
            sign_values = [
                (row['REG_USE'], row['Contract_Code'], row['Activity'], row['Staff_1'],
                 row['Lease_Start_Date'], row['Sign_Contract'], row.get('renting', ''),
                 row['Payment_Method'], row['PoA_Sign_Session'], row['POA_Eng'],
                 row['POA_Ar'], row['PoA_phone'], row['PoA_phone2'], row['POA_ID'],
                 row['_submission_time'])
                for _, row in combined_sign.iterrows() if str(row['REG_USE']).strip()
            ]
            if sign_values:
                cursor.executemany(insert_sign_query, sign_values)
               
        
        
        if not combined_cancel.empty:
            
            cancel_values = [
                (row['REG_USE'], row['Contract_Code'], row['Activity'], row['Staff_1'],
                 row['Why_Cancel'], row['Cancel_Note'], row['Notes'],
                 row['_submission_time'], row['_id'])
                for _, row in combined_cancel.iterrows() if str(row['REG_USE']).strip()
            ]
            if cancel_values:
                cursor.executemany(insert_cancel_query, cancel_values)
                
        
        
        if not update_df.empty:
            print(f"Updating {len(update_df)} beneficiary records...")
            update_values = [
                (row['HoH_Full_Name_Arabic'], row['HoH_Full_Name_English'],
                 row['Phone'], row['Phone_2'], row['UNHCR_number'], row['UNHCR_2'],
                 row['UNHCR_3'], row['UNHCR_4'], row['UNHCR_5'], row['Activity'],
                 row['_submission_time'], row['REG_USE'])
                for _, row in update_df.iterrows()
            ]
            if update_values:
                cursor.executemany(update_query, update_values)
                print(f"✓ Updated {len(update_values)} beneficiary records")
        
        connection.commit()
        print("\n✅ All data inserted/updated successfully!")
        
    except pymysql.MySQLError as e:
        print(f"\n❌ MySQL Error: {e}")
        if connection:
            connection.rollback()
            print("Transaction rolled back")
        
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        if connection:
            connection.rollback()
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed")


if __name__ == "__main__":
    print("=" * 60)
    print("KOBO TO MYSQL DATA SYNC - OPTIMIZED VERSION")
    print("=" * 60)
    
    
    result = clean_and_process_data()
    combined_social, combined_landlord, combined_agreement, update_df, combined_sign, combined_cancel = result
    
    
    has_data = any([
        not combined_social.empty,
        not combined_landlord.empty,
        not combined_agreement.empty,
        not update_df.empty,
        not combined_sign.empty,
        not combined_cancel.empty
    ])
    
    if has_data:
        insert_into_mysql(combined_social, combined_landlord, combined_agreement, 
                         update_df, combined_sign, combined_cancel)
    else:
        print("\n⚠️ No data to insert.")
    
    print("=" * 60)
    print("SYNC COMPLETE")
    print("=" * 60)