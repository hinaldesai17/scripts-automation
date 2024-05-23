def send_data_to_questdb(df, tablename):
    from questdb.ingress import Sender
    conf = f'http::addr=192.168.0.172:9000;'

    df['date'] = df['date'].astype('datetime64[ns]')
    with Sender.from_conf(conf) as sender:
            sender.dataframe(
                df,
                symbols=['SYM'],
                table_name=tablename,
                at='date')
    
    print("Sent to questdb with tablename: ", tablename)