def get_cocoa_data_and_save_to_qdb():
    import pandas as pd
    
    #from qdb import send_data_to_questdb
    import os
    from datetime import datetime, timedelta
    date_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
    print("Date str:", date_str)

    print("Getting file for date: ", date_str)
    from cocoa_scripts.dataset import download_excel_from_ice, get_worksheet,prepare_df_from_excel, get_date_from_sheet, prepare_cocoa_cert_stk_df, prepare_cocoa_grading_results_df, prepare_cocoa_total_bags_df
    from cocoa_scripts.qdb import send_data_to_questdb

    filename = download_excel_from_ice(date_str)
    ws = get_worksheet(filename, date_str)
    date = get_date_from_sheet(ws)
    df = prepare_df_from_excel(ws)
    cocoa_cert_stk_df_bags, cocoa_cert_stk_df_lots = prepare_cocoa_cert_stk_df(df, date)
    cocoa_grading_results_df_bags, cocoa_grading_results_df_lots = prepare_cocoa_grading_results_df(df, date)
    cocoa_total_bags_df = prepare_cocoa_total_bags_df(df, date)

    # send to questdb
    send_data_to_questdb(cocoa_cert_stk_df_bags, "cocoa_cert_stk_bags")
    send_data_to_questdb(cocoa_cert_stk_df_lots, "cocoa_cert_stk_lots")
    if cocoa_grading_results_df_bags is not None:
        send_data_to_questdb(cocoa_grading_results_df_bags, "cocoa_grad_results_bags")
        send_data_to_questdb(cocoa_grading_results_df_lots, "cocoa_grad_results_lots")
    send_data_to_questdb(cocoa_total_bags_df, "cocoa_total_bags")

    print("COCOA CERT STOCKS DF BAGS::\n", cocoa_cert_stk_df_bags)
    print("COCOA CERT STOCKS DF LOTS:\n", cocoa_cert_stk_df_lots)
    print("COCOA GRADING RESULTS DF BAGS::\n", cocoa_grading_results_df_bags)
    print("COCOA GRADING RESULTS DF LOTS::\n", cocoa_grading_results_df_lots)
    print("COCOA TOTAL BAGS DF::\n", cocoa_total_bags_df)

    ##### Delete the files at the end

    parts = filename.split("_")
    date_and_extension = parts[-1]

    # Replace ".xls" with ".xlsx"
    new_filename = date_and_extension.replace(".xls", ".xlsx")

    if os.path.exists(filename):
        os.remove(filename)
        os.remove(new_filename)
        print(f"{filename, new_filename} deleted successfully")
    else:
        print("file not found")