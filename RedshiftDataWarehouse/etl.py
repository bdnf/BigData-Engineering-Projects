import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, all_tables_count_rows


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables (staging_events, staging_songs)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data: extracting from staging tables into the dimensional tables
                    using INSERT queries declared in sql_queries
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def count_rows(cur, conn):
    """
    Test whether all tables were populated with data successfully
    """
    for query in all_tables_count_rows:
        cur.execute(query)
        res = cur.fetchone()
        for rows in res:
            print("Query %s returned %s " % (query, rows))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    print("Issuing copy commands (it may take up to 15 minutes time to load the data from s3). Please wait ...")
    load_staging_tables(cur, conn)

    print("Inserting tables")
    insert_tables(cur, conn)

    print("Checking number of rows in tables")
    count_rows(cur, conn)

    conn.close()
    print("Finished successfully")


if __name__ == "__main__":
    main()
