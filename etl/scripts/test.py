import json
import psycopg2

def insert_data():
    with psycopg2.connect('') as conn:
        with conn.cursor as cur:
            with open('job-etl/data/json/1231621951.json') as json_file:
                data = file.read()
                query_sql = """
                insert into json_table 
                select * from json_populate_recordset(NULL::json_table, %s);"""
                cur.execute(query_sql, (json.dumps(data)))

# Main function called inside the execute.py script
def main():
    print("[Load] Start")
    print("[Load] Inserting new rows")
    insert_data()
    print("[Load] End")