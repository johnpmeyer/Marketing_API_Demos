import pandas as pd
import logging

def create_update_query(table, dtf, constraint):
    """This function creates an upsert query which 
       replaces existing data based on primary key conflicts
       
       table - string name of the database table to update
       dtf - pandas dataframe containing the data to upsert
       constraint - string name of the data table constraint in Postgres
       
       returns - SQL query to do the upsert
    """
    DATABASE_COLUMNS = list(dtf)
    columns = ', '.join([f'{col}' for col in DATABASE_COLUMNS])
    placeholder = ', '.join([f'%({col})s' for col in DATABASE_COLUMNS])
    updates = ', '.join([f'{col} = EXCLUDED.{col}' 
                         for col in DATABASE_COLUMNS])
    query = f"""INSERT INTO {table} ({columns}) 
                VALUES ({placeholder}) 
                ON CONFLICT ON CONSTRAINT {constraint} 
                DO UPDATE SET {updates};"""
    query.split()
    query = ' '.join(query.split())
    return query


def load_updates(df, table, engine, constraint):
    """This function loads a dataframe into the table using an upsert.
       
       df - pandas dataframe containing the data to upsert
       table - string name of the database table to update
       constraint - string name of the data table constraint in Postgres
    """
    conn = engine.raw_connection()
    cursor = conn.cursor()
    df1 = df.where((pd.notnull(df)), None)
    insert_values = df1.to_dict(orient='records')
    for row in insert_values:
        cursor.execute(create_update_query(table, df, constraint), row)
        conn.commit()
    row_count = len(insert_values)
    logging.info(f'Inserted {row_count} rows.')
    cursor.close()
    del cursor
    conn.close()