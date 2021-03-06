from data_transfer import DataTransfer
import csv
import psycopg2


def table_list(connect_par):
    tbls = []
    with psycopg2.connect(connect_par) as con:
        with con.cursor() as cur:
            # Получаем список таблиц из исходной БД :
            cur.execute("""SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public'""")
            for row in cur.fetchall():
                tbls.append(row[0])
    return tbls



class DataTransferPostgres(DataTransfer):
    def __init__(
        self, config,
        source_pg_conn_str,
        pg_conn_str,
        pg_meta_conn_str,
         query, *args, **kwargs
    ):
        super(DataTransferPostgres, self).__init__(
            config=config,
           # source_pg_conn_str=source_pg_conn_str,
            pg_conn_str=pg_conn_str,
            pg_meta_conn_str=pg_meta_conn_str,
            query=query, *args, **kwargs
        )
        self.source_pg_conn_str = source_pg_conn_str
        self.query = query

    def provide_data(self, csv_file, context):
        pg_conn = psycopg2.connect(self.source_pg_conn_str)
        pg_cursor = pg_conn.cursor()
        query_to_execute = self.query
        self.log.info("Executing query: {}".format(query_to_execute))
        pg_cursor.execute(query_to_execute)
        csvwriter = csv.writer(
            csv_file,
            delimiter="\t",
            quoting=csv.QUOTE_NONE,
            lineterminator="\n",
            escapechar='\\'
        )

        while True:
            rows = pg_cursor.fetchmany(size=1000)
            if rows:
                for row in rows:
                    _row = list(row)
                    csvwriter.writerow(_row)
            else:
                break
        pg_conn.close()
