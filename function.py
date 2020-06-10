# import libraries
import pandas as pd
import numpy as np
import sys
import snowflake.connector # Entering Credentials for Connection
from datetime import date

username = str(sys.argv[1]) 
password = str(sys.argv[2]) 
Start_date = str(sys.argv[3]) 
warehouse_name = 'DS_WH'
account = 'indigoproduction.us-east-1'

connection = snowflake.connector.connect(
        user=str(username),
		account=str(account),
		authenticator='externalbrowser') ##Note - a browser window will launch but you may have to look for it.

# Connecting with a username password
connection = snowflake.connector.connect(
        user=str(username),
        password=str(password),
        account=str(account))

#connect to the current ware house
cursor = connection.cursor()
cursor.execute(f'use warehouse {warehouse_name}')

def pre_processing():
    query= 'SELECT fe.name AS form_element_name ,f.name AS form_name,fe.dca_type AS element_type,fe.updated_at AS element_updated_at,fe.created_at AS element_created_at FROM "PARTHENON"."DCA_DB"."DCA_FORM_ELEMENTS" fe INNER JOIN "PARTHENON"."DCA_DB"."DCA_FORM_FORM_ELEMENTS" ffe ON fe.id = ffe.dca_form_element_id INNER JOIN "PARTHENON"."DCA_DB"."DCA_FORMS" f ON ffe.dca_form_id = f.id INNER JOIN "PARTHENON"."DCA_DB"."DCA_FORM_GROUP_FORMS" fgf ON ffe.dca_form_group_form_id = fgf.id  AND f.id = fgf.dca_form_id INNER JOIN  "PARTHENON"."DCA_DB"."DCA_FORM_GROUPS" fg ON fgf.DCA_FORM_GROUP_ID = fg.id INNER JOIN "PARTHENON"."DCA_DB"."DCA_FORM_TEMPLATE_FORM_GROUPS" ftfg ON fgf.dca_form_template_form_group_id = ftfg.id AND fg.id = ftfg.dca_form_group_id INNER JOIN  "PARTHENON"."DCA_DB"."DCA_FORM_TEMPLATES" ft ON ffe.dca_form_template_id = ft.id AND fgf.dca_form_template_id = ft.id  AND  ftfg.dca_form_template_id = ft.id INNER JOIN "PARTHENON"."DCA_DB"."DCA_PROGRAMS" p ON ft.dca_program_id = p.id INNER JOIN  "PARTHENON"."DCA_DB"."DCA_SEASONS" s ON  ft.dca_season_id = s.id;'
    cursor.execute(query)
    result = cursor.fetch_pandas_all
    print(result)
    #converting into Dataframe
    df = pd.DataFrame.from_records(iter(cursor), columns=[x[0] for x in cursor.description])
    #Dropping all the duplicate entries
    df=df.drop_duplicates(subset=None, keep='first', inplace=False)
    #replacing data type
    df["ELEMENT_TYPE"].replace({"SELECT": "STRING", "MESSAGE": "STRING", "QUANTITY_SELECT" : "STRING",
                             "GEOMETRY_POINT_SELECT" : "STRING", "LINEAR_SCALE" : "STRING",
                             "NUMBER":"BOOLEAN","TEXT":"STRING" ,"PARAGRAPH":"STRING",
                            "TOGGLE":"BOOLEAN","FILE_UPLOAD":"STRING","TANK_MIX":"STRING","GEOMETRY":"STRING",
                            }, inplace=True)
    df['COMBINED']=df['FORM_NAME'].astype(str)+':'+df['ELEMENT_TYPE']
    return df
df=pre_processing()

def to_dic(df):
    dic=df.groupby('FORM_ELEMENT_NAME')['COMBINED'].apply(tuple).to_dict()
    return dic
dic=to_dic(df)

def my_function(dic):
    cursor.execute('Select * from "PARTHENON"."AGRONOMY"."IDV_GENERIC_EVENT"')
    print("Select ")
    print(','.join([col[0] for col in cursor.description]))

    for k,item in dic.items():
        if isinstance(item, tuple):
            num = len(item)
        else:
            num = 1
        if num==1:
            for i in item:
                form_name=i.split(':')[:1]
                for element in form_name:
                    print(",parse_json(EVENT_DATA_JSON):", element, ':' ,k,"::",i.split(':')[1], " as ",k,sep="")
        elif num>1:
            print(',case')
            for i in item:
                form_name=i.split(':')[:1]
                for element in form_name:
                    print('    when parse_json(EVENT_DATA_JSON):'+element+':' ,k+'::',i.split(':')[1]," is not null then parse_json(EVENT_DATA_JSON):"+ i+':' +k+"::STRING",sep="")
            print('    end as '+k )

    print ('from  "PARTHENON"."AGRONOMY"."IDV_GENERIC_EVENT";;')


    for k,item in dic.items():
        if isinstance(item, tuple):
            num = len(item)
        else:
            num = 1
        if num==1:
            for i in item:
                print('dimension: '+k +'{')
                print('view_label: "'+i.capitalize()+'"')
                print('description: " "')
                print( 'type:')
                print('sql: ${TABLE}."'+k.upper()+'" ;;')
                print('}')
                print( )
        elif num>1:
            print('dimension: '+k +'{')
            print('view_label: "'+k.capitalize()+'"')
            print('description: " "')
            print( 'type: string')
            print('sql: ${TABLE}."'+k.upper()+'" ;;')
            print('}')
            print( )
#fetching today's date
today = date.today()
end_date=str(today)

#open file to write o/p
sys.stdout=open("output.txt","w")
if not Start_date:
    print("your_variable is empty")
    my_function(dic)
else:
    start_date = Start_date
    #end_date = '06-01-2020'
    mask = (df['ELEMENT_CREATED_AT'] > start_date) & (df['ELEMENT_CREATED_AT'] <= end_date)
    temp = df.loc[mask]
    pd.DataFrame(temp)
    dic1=to_dic(temp)
    my_function(dic1)
sys.stdout.close()  #closing the file