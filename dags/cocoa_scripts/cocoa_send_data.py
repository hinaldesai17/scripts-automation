def get_cocoa_data_and_send_to_gsheet():
    def get_data_from_questdb(req):
        import requests
        import pandas as pd
        from datetime import datetime, timedelta

        date_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        # Get data from QuestDB
        resp = requests.get(
            f'http://192.168.0.172:8080/api/v1/data/cocoa/{req}',
            params={'from': f'{date_str}', 'to': f'{date_str}'}
        ).json()


        df = pd.DataFrame(resp)
        return df
    
    
    def send_data_gsheet(df, sheet_no):    
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        # Google Sheets authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/dags/cocoa_scripts/cocoadata-efb5408251b1.json', scopes=scopes)
        file = gspread.authorize(creds)
        workbook = file.open("cocoa_data")
        sheet = workbook.get_worksheet(sheet_no)

        # Get the last row index
        rows = sheet.get_all_values()
        last_row_index = len(rows)

        # Specify the range for updating (start from the next row)
        update_range = f'A{last_row_index + 1}:H{last_row_index + df.shape[0]}'

        # Convert DataFrame to a list of lists
        values = df.values.tolist()

        print("sending data...")
        # Updating sheet data
        sheet.update(values, update_range)

        print("Data successfully sent to google sheet!")
    
    req_dict = {0: 'certified_bags', 1:'certified_lots', 2:'grading_bags', 3:'grading_lots', 4:'total_bags'}

    for k, v in req_dict.items():
        try:
            df = get_data_from_questdb(v)
            print("DF", df)
            send_data_gsheet(df, k)
        except Exception as e:
            print("Exception: ", e)
            continue