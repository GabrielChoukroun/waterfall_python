import pandas as pd
import psycopg2
import psycopg2.extras
import time
import os
import boto3

conn_bo = None
conn_ver = None
conn_inst = None


class SimpleConnectionBO():
    def __init__(self):
        self._host = os.environ['PG_HOST_l']
        self._user = os.environ['PG_USER_l']
        self._token = "core-production-backend-aurora-cluster.cluster-ro-c9us761nawfe.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=iam_ro&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAUXPIFN5HKXXDCQOF%2F20220523%2Fus-east-1%2Frds-db%2Faws4_request&X-Amz-Date=20220523T075538Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEHAaCXVzLWVhc3QtMSJHMEUCIDEdvoHYHyObgqeA139K88y4ljC9%2B3x2CskG8jXzFontAiEA89Fk53Srg7Byf5ImYwR1hNbqWWScypYAg09u21VkWGEq7wEIWRACGgwzMjUyOTM3OTcxOTgiDPrV9b8IdyqvI96mQCrMAUmP1wyz%2FwFDMTea2dmFfXgXYx%2FIZOF%2BLOdhkr493gecYUc3%2BCMhHCRI3ZxoPrOix3g3YN9GS%2ByxZCiWOILOy8O2VoOjNu%2FDomqQJH68Ovw62QGD2TIyOAHQPuWmlHwyTLRc3tOcS15K1AUQj3%2Be%2Fq0%2Fj%2BhAE9kW%2Fdam1xhoJ3smn7Y9luG4kPVxnd4MaYp89D0M7dlm%2FZqr2NwevDm967DF99e2tbyn55%2B8aUOvaRo%2Foc9EWNXmsfV9zUkxCzAxBfj9NQg5e%2B1QqAlsezDx%2B6yUBjqYAUlGjqViHA2EadlbTd6MATgSVPVLNismTKEEjS5XUyAsonLwbQLUL6kPsRn7QfXlKJX0UEU9EV2oIqIp5F9R%2BffemLWlOuO2n2dc01GB8s9jxG59oPRNSDwwb2PHOpf7wnLYjKMBHGgxnVMI%2BSQ%2FhAqorDL3z9fdu1sjLo0I5jVJ%2Fsu5lN2fye7kwq7tpbRYSWjL0zt1Mq63&X-Amz-Signature=fdbe8f7b46e9b4e1f84428a2896bb4cb496e844ccb84a52f18619b6a77bfa19d"
        self._db = os.environ['PG_DB_l']
        print('NEW ---------')

    def _generate_db_token(self):
        session = boto3.Session(profile_name='mfa')
        client = session.client('rds')
        return client.generate_db_auth_token(DBHostname=self._host, Port=2345, DBUsername=self._user)
        
        
    def create_conn(self):
        if conn_bo is None:
            
            return psycopg2.connect(dbname=self._db, user=self._user,
                                    password=self._token, host=self._host,
                                    port=2345, sslmode='require')
        else:
            return conn_bo

    def create_pandas_table(self, sql_query):
        conn = self.create_conn()
        table = pd.read_sql_query(sql_query, conn)
        return table


class SimpleConnectionDW():
    def __init__(self):
        self.retries = 0

    def create_conn(self):
        # https: // stackoverflow.com / questions / 56332906 / where - to - put - ssl - certificates - when - trying - to - connect - to - a - remote - database - using
        if conn_ver is None:
            return psycopg2.connect(dbname=os.environ['VER_DB'], user=os.environ['VER_USER'],
                                    password=os.environ['VER_PASS'], host=os.environ['VER_HOST'],
                                    port=3233, sslmode='require')
        else:
            return conn_ver

    def create_pandas_table(self, sql_query):
        conn = self.create_conn()
        table = pd.read_sql_query(sql_query, conn)
        return table

    def insert_rows(self, table_name, df):
        total_rows = len(df)
        print(total_rows)

        # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
        # df is the dataframe
        if total_rows > 0:
            df_columns = list(df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # table name
            table = table_name

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

            conn = self.create_conn()
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            cur.close()

            print("{} rows have been appended".format(total_rows))

            subject = f'{table} - rows were appended'
            body = f'{table} - {total_rows} rows were appended'
            # print(SendEmail.sending(self, subject, body))

        # 120993
        # 129609
        # 136753
        return total_rows

    def insert_rows_on_conflict(self, table_name, df, conflict_col):
        total_rows = len(df)
        print(total_rows)

        # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
        # df is the dataframe
        if total_rows > 0:
            df_columns = list(df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # create upsert ON CONFLICT (id) DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2...
            con_columns = []
            for x in df_columns:
                if x != conflict_col:
                    this_val = "{}=EXCLUDED.{}".format(x, x)
                    con_columns.append(this_val)
            upsert = ",".join(con_columns)

            # table name
            table = table_name

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}" \
                          "ON CONFLICT ({}) DO UPDATE SET {}" \
                          "".format(table, columns, values, conflict_col, upsert)
            # print(insert_stmt)

            conn = self.create_conn()
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            cur.close()

            print("{} rows have been appended".format(total_rows))

            subject = f'{table} - rows were appended'
            body = f'{table} - {total_rows} rows were appended'
            # print(SendEmail.sending(self, subject, body))

        # 120993
        # 129609
        # 136753
        return total_rows

    def insert_rows_multiple_conflict_nothing(self, table_name:str, df, conflict_col:str):
        total_rows = len(df)
        print(total_rows)

        # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
        # df is the dataframe
        if total_rows > 0:
            df_columns = list(df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # table name
            table = table_name

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {} ON CONFLICT {} DO NOTHING".format(table, columns, values, conflict_col)

            conn = self.create_conn()
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            cur.close()

            print("{} rows have been appended".format(total_rows))

            subject = f'{table} - rows were appended'
            body = f'{table} - {total_rows} rows were appended'
            
        return total_rows

    def delete_rows(self, delete_statement):
        # https: // www.postgresqltutorial.com / postgresql - python / delete /

        try:
            # connect to the PostgreSQL database
            conn = self.create_conn()

            # create a new cursor
            cur = conn.cursor()
            # execute the DELETE statement
            cur.execute(delete_statement)

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Something went wrong, please check logs.")
            LOGGER.error(error)
        finally:
            if conn is not None:
                conn.close()

        return 1

    def refresh_mv(self, sql_refresh):
        conn = self.create_conn()
        cur = conn.cursor()
        # psycopg2.extras.execute_batch(cur, sql_refresh)
        cur.execute(sql_refresh)
        conn.commit()
        cur.close()


class SimpleConnectionInstilend():

    def __init__(self):
        self._host = os.environ['INST_HOST']
        self._user = os.environ['INST_USER']
        self._db = os.environ['INST_DB']
        self._token = self._generate_db_token()

    def _generate_db_token(self):
        session = boto3.Session(profile_name='mfa')
        client = session.client('rds')

        return client.generate_db_auth_token(DBHostname=self._host, Port=5432, DBUsername=self._user)

    def create_conn(self):
        if conn_inst is None:
            return psycopg2.connect(host=self._host, port=5432, database=self._db, user=self._user, password=self._token, sslmode='require')
        elif conn_inst.closed==0:
            conn_inst.close()
            return psycopg2.connect(host=self._host, port=5432, database=self._db, user=self._user, password=self._token, sslmode='require')
        else:
            return conn_inst
    
    def create_pandas_table(self, sql_query):
        conn = self.create_conn()
        table = pd.read_sql_query(sql_query,conn)
        return table

# These global objects represents the database connections
back_office = SimpleConnectionBO()
veritas = SimpleConnectionDW()
inst = SimpleConnectionInstilend()
