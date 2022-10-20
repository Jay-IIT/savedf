from datetime import datetime
import dateparser
import sys
import os
import pandas as pd
import pymysql as sql
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText
from email import encoders
import numpy as np
from attrib_progress import Progress 
USER = os.environ.get('USER', None)
def get_date():
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d")
    return date_time

def get_stats(dfs):
    map_stats = {"Total":0,"Covered":0,"Reviewed Test":0,"Reviewed Attribute":0, "Obsolete":0,"Partial":0,"Not Covered":0,"Bug Opened":0, "Eye Checked":0, 
            "P + BO":0, "C + P + BO":0}
    tot_summary = dfs[0].groupby("coverage").size()
    

    for k,v in tot_summary.items():
        map_stats["Total"] += int(v)
        if k in {"Covered", "Partial",  "Bug Opened"}:
            map_stats["C + P + BO"] += int(v)
        if k in {"Partial", "Bug Opened"}:
            map_stats["P + BO"] += int(v)
        if(map_stats.get(k,None) == 0):
            map_stats[k] = int(v)
    print(map_stats)
    tot_summary = pd.DataFrame(map_stats.items(), columns=["Coverage", "#Count"])
    df_stats = dfs[0].groupby(["module","owner","coverage"]).size()
    stats_df=pd.DataFrame(columns=["module", "owner", "Stats"])
    purned_blocks_df=pd.DataFrame(columns=["module", "owner", "Stats"])
    
    for i, row in df_stats.items():
        status = i[2].split()
        con = "".join([x[0] for x in status])
        status = str(row) + "".join([x[0] for x in status])
        r={"module":i[0], "owner": i[1], "Stats": status} 
        match=stats_df[(stats_df["owner"]==i[1]) & (stats_df["module"]==i[0])].index.tolist()
        if con in {"C","P","BO","EC"}:
            #print(con)
            match_con=purned_blocks_df[(purned_blocks_df["owner"]==i[1]) & (purned_blocks_df["module"]==i[0])].index.tolist()
            if match_con:
                purned_blocks_df.at[match_con[0],"Stats"] = purned_blocks_df.iloc[match_con[0]]["Stats"] + "/" + r["Stats"]
            else:
                purned_blocks_df = purned_blocks_df.append(r,ignore_index=True) 
        if match:
            stats_df.at[match[0],"Stats"] = stats_df.iloc[match[0]]["Stats"] + "/" + r["Stats"]
        else:
            stats_df = stats_df.append(r,ignore_index=True)
    progress_df = Progress(purned_blocks_df).get()
    
    
 
    return purned_blocks_df, stats_df, tot_summary,progress_df

def send_email(dfs):
    sendFrom = 'kmalempa@cisco.com'
    sendTo = f'{USER}@cisco.com'
    # Create the root message and fill in the from, to, and subject headers
    msg = MIMEMultipart()

    filename = "report.xlsx"
    ExcelWriter= pd.ExcelWriter(filename,engine="xlsxwriter")
    purned_blocks_df, summary_df, stats_df,progresss_df=get_stats(dfs)
    summary_df.to_excel(ExcelWriter, sheet_name="Blocks_Summary", index=False)
    purned_blocks_df.to_excel(ExcelWriter, sheet_name="Pruned_Blocks_Summary", index=False)
    stats_df.to_excel(ExcelWriter, sheet_name="Total_Summary", index=False)
    progresss_df.to_excel(ExcelWriter, sheet_name="Progress_Summary", index=False)
    
    ExcelWriter.save()
    ExcelWriter.close()

    attachment = open(filename,"rb")
    x = MIMEBase("application","octet-stream")
    x.set_payload((attachment).read())
    encoders.encode_base64(x)
    x.add_header("Content-Disposition", "attachment; filename=%s" %filename)


    text = "Hi, \n\n\nPlease find the attachement\n\n\nThanks,\nKotesh"
    part1 = MIMEText(text, 'plain')
    msg['From'] = sendFrom
    msg['To'] = sendTo
    msg['Cc'] = 'kmalempa@cisco.com'
    msg['Subject'] = "Attrib tool report"
    msg.attach(part1)
    msg.attach(x)
    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(sendFrom, sendTo, msg.as_string())
    smtp.quit()
    print("Mail Sent Successfully")


def run_query(queries):
    try: 
        tunnel = SSHTunnelForwarder(("asic-web-sjc01",22), remote_bind_address=("127.0.0.1", 3306))
        tunnel.start()
        df = []
        dbconl = sql.connect(host="localhost", user="webattrib_admin",password= "justabouteverything",database= "webattrib2", port=tunnel.local_bind_port)
        for idx,query in enumerate(queries,1):
            print("Query=" +query)
            df.append(pd.read_sql_query(query, dbconl))
        #print(df.head())
        dbconl.close()
        tunnel.close
        return True,df
    except Exception as e:
        return False,e

if _name__ == 'main_':
    try:
        try:
            if(len(sys.argv) > 2 and "-s" in sys.argv):
                start_date_index=sys.argv.index("-s")
                if start_date_index+1 < len(sys.argv):
                    start_date=dateparser.parse(sys.argv[start_date_index+1],date_formats=["%Y-%B-%d"], settings={"TIMEZONE":"UTC"})
                    start_date=start_date.strftime("%Y-%m-%d")
                else:
                    start_date= get_date()
            else:
                start_date= get_date()
        except Exception as e:
            print("Dateparsing ", e)
            sys.exit()

        try:
            if(len(sys.argv) > 2 and "-e" in sys.argv):
                end_date_index=sys.argv.index("-e")
                if end_date_index+1 < len(sys.argv):
                    end_date=dateparser.parse(sys.argv[end_date_index+1],date_formats=["%Y-%B-%d"], settings={"TIMEZONE":"UTC"})
                    end_date=end_date.strftime("%Y-%m-%d")
                else:
                    end_date= get_date()
            else:
                end_date= get_date()
        except Exception as e:
            print("Dateparsing ", e)
            sys.exit()

        print("Given start date [yyyy-mm-dd]:"+ start_date)
        print("Given end date [yyyy-mm-dd]:"+ end_date)
        queries = [f"select * from attributes;"]
        #queries = [f"select * from attributes where module = \"moc_ig\" and entry_date between '{start_date}' and '{end_date}' limit 10", "select module,owner, coverage, count(coverage) from attributes group by module, owner, coverage"]
        
        status,df = run_query(queries)
        if status:
            send_email(df)
        else:
            print("exception ", df)

    except KeyboardInterrupt:
        pass

    except Exception as e:
        print(e)