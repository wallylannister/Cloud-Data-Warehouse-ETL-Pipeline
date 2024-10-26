import configparser
import psycopg2
from sql_queries import analytical_queries


def select_queries(cur, conn):
    for query in analytical_queries:
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print(row)
        print("--End of query--\n")
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    select_queries(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()