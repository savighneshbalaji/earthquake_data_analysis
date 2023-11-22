import csv
import mysql.connector


mysql_host = "localhost"
mysql_user = "root"
mysql_password = ""
mysql_db = "mysql"
mysql_table = "neic_earthquakes"

csv_file_path = "/Users/vanand/Documents/spark_project/data/database.csv"
try:
    db = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        passwd=mysql_password,
        database=mysql_db
    )
    cursor = db.cursor()

    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        row_headers = next(csv_reader)

    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  
        csv_rows = list(csv_reader)
    create_table_query = f"CREATE TABLE {mysql_table} ("

    for header_index, header in enumerate(row_headers):
        data_type = "VARCHAR(255)"  
        if all(row[header_index].isdigit() for row in csv_rows):
            data_type = "INT"
        elif all(row[header_index].replace('.', '', 1).isdigit() for row in csv_rows):
            data_type = "FLOAT"
        elif all(row[header_index].lower() in ['true', 'false'] for row in csv_rows):
            data_type = "BOOLEAN"

        create_table_query += f"`{header}` {data_type}, "

    create_table_query = create_table_query.rstrip(", ") + ");"

    print(f"Generated SQL Query for Table Creation: {create_table_query}")
    cursor.execute(create_table_query)

    print(f"Table '{mysql_table}' created successfully!")

    insert_data_query = f"INSERT INTO {mysql_table} ("
    insert_data_query += ', '.join([f"`{header}`" for header in row_headers])
    insert_data_query += ") VALUES ("
    insert_data_query += ', '.join(['%s'] * len(row_headers))
    insert_data_query += ");"

    cursor.executemany(insert_data_query, csv_rows)

    db.commit()

    print(f"Data inserted into the table '{mysql_table}' successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if 'db' in locals() and db.is_connected():
        db.close()
        print("Connection closed.")
