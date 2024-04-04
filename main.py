from google.cloud import bigquery
import requests
import json
from constants import LOCATIONS_QUERY, OATH, DEST_TABLE, PROJECT_ID


def read_data():
    result_set=[]
    location=''
    state=''
    country=''

    # Perform a query.
    QUERY = (LOCATIONS_QUERY)
    query_job = client.query(QUERY)
    rows = query_job.result()
    count=0

    for row in rows:
        count+=1
        location = row[0],',',row[1],',',row[2]
        state= row[2]
        check = True
        if country == 840:
            country= 'US'
        if row[2] == 'MX':
            country = 'MX'
            check = False
        if row[2] == 'PR':
            country = 'PR'

        try:
            end=correct_addresses(row[0],location, state, country, check)
            result_set.append([row[0],end])
            if count==25:

                count=0
                print('write 25 batch')
                write_to_db(result_set)
                result_set=[]
        except Exception as e:
            print(e)
            write_to_db(result_set)




def correct_addresses(original,location, state, country, check):

    url = 'https://addressvalidation.googleapis.com/v1:validateAddress'

    payload = {
                'address': {
                    "regionCode": country,
                    "administrativeArea": state,
                    "postalCode": "94043",
                    "addressLines": [location]
                },
                "enableUspsCass": check
            }
    # gcloud auth
    # application - default
    # print - access - token
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer '+OATH,
               'X-Goog-User-Project': PROJECT_ID
               }

    r = requests.post(url, data=json.dumps(payload), headers=headers)

    # Handle the response
    result=r.json()
    try:
        formattedAddress=(result.get('result').get('address').get('formattedAddress'))
    except Exception as e:
        print(result)
        print(e)
    print('address formatted')
    return formattedAddress

def create_table():
    table_id = bigquery.Table.from_string(DEST_TABLE)
    schema = [
        bigquery.SchemaField("original_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transformed_address", "STRING", mode="REQUIRED")
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    return table_id

def write_to_db(result_set):
    rows_to_insert = []
    for i in result_set:
        rows_to_insert.append({u"original_address": i[0], u"transformed_address": i[1]})

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == '__main__':
    client = bigquery.Client(project=PROJECT_ID)
    table_id = bigquery.Table.from_string(DEST_TABLE)
    # table_id=create_table()
    read_data()




