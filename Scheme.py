import json
import sys
import mariadb

DB_URL = ""
DB_USERNAME = ""
DB_PASSWORD = ""
DB_PORT = 3306


class Scheme:
    def __init__(self, host: str, user: str, password: str, port: int, scheme: str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.scheme = scheme
        self.conn = mariadb.connect(host=host, user=user, password=password, port=port)
        self.described_table = []

    def get_databases(self):
        cur = self.conn.cursor()
        cur.execute(f"show databases")
        return cur.fetchall()

    def get_tables(self):
        cur = self.conn.cursor()
        cur.execute(f"show tables from {self.scheme}")
        result = cur.fetchall()
        return [''.join(map(str, t)) for t in result]

    def get_table(self, table_name: str) -> list:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"use {self.scheme}")
        cur.execute(f"describe {table_name}")
        return cur.fetchall()

    def get_checks(self, table_name: str) -> list:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT "
                    f"tc.CONSTRAINT_NAME, "
                    f"cc.CHECK_CLAUSE "
                    f"FROM "
                    f"INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc "
                    f"JOIN "
                    f"INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc "
                    f"ON "
                    f"tc.CONSTRAINT_CATALOG = cc.CONSTRAINT_CATALOG "
                    f"AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA "
                    f"AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME "
                    f"WHERE "
                    f"tc.CONSTRAINT_TYPE = 'CHECK' "
                    f"AND tc.TABLE_NAME = '{table_name}' "
                    f"AND tc.TABLE_SCHEMA = '{self.scheme}'")
        return cur.fetchall()

    def get_indexes(self, table_name: str) -> list:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"use {self.scheme}")
        cur.execute(f"show index from {table_name}")
        return cur.fetchall()

    def get_procedures(self) -> list:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '{self.scheme}' AND ROUTINE_TYPE = 'PROCEDURE'")
        return cur.fetchall()

    def get_funcitions(self) -> list:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '{self.scheme}' AND ROUTINE_TYPE = 'FUNCTION'")
        return cur.fetchall()

    def get_events(self):
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT EVENT_NAME, "
                    f"LAST_EXECUTED, "
                    f"EVENT_TYPE, "
                    f"EXECUTE_AT, "
                    f"INTERVAL_VALUE, "
                    f"INTERVAL_FIELD, "
                    f"STATUS FROM INFORMATION_SCHEMA.EVENTS "
                    f"WHERE EVENT_SCHEMA = '{self.scheme}'")
        result = cur.fetchall()
        for i in result:
            i['LAST_EXECUTED'] = str(i['LAST_EXECUTED'])
            i['EXECUTE_AT'] = str(i['EXECUTE_AT'])
        return result

    def get_triggers(self):
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT TRIGGER_NAME, "
                    f"EVENT_OBJECT_TABLE, "
                    f"EVENT_MANIPULATION, "
                    f"ACTION_TIMING, "
                    f"ACTION_STATEMENT "
                    f"FROM INFORMATION_SCHEMA.TRIGGERS "
                    f"WHERE TRIGGER_SCHEMA = '{self.scheme}'")
        return cur.fetchall()

    def get_foreign_keys(self, table_name: str):
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT "
                    f"TABLE_NAME, "
                    f"COLUMN_NAME, "
                    f"CONSTRAINT_NAME,"
                    f"REFERENCED_TABLE_NAME,"
                    f"REFERENCED_COLUMN_NAME "
                    f"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                    f"WHERE TABLE_SCHEMA = '{self.scheme}' "
                    f"AND TABLE_NAME = '{table_name}' "
                    f"AND REFERENCED_TABLE_NAME IS NOT NULL;")
        return cur.fetchall()

    def get_scheme(self):
        tables = self.get_tables()
        if not tables:
            sys.exit("No tables found")

        discribed_tables = {}
        discribed_indexes = {}
        discribed_foreign_keys = {}
        discribed_checks = {}
        for table in tables:
            discribed_tables[table] = self.get_table(table)
            discribed_indexes[table] = self.get_indexes(table)
            discribed_foreign_keys[table] = self.get_foreign_keys(table)
            discribed_checks[table] = self.get_checks(table)

        scheme = {'Host': self.host,
                  self.scheme:
                      {'Tables': discribed_tables, 'indices': discribed_indexes,
                       'Events': self.get_events(),
                       'Triggers': self.get_triggers(),
                       'Procedures': self.get_procedures(),
                       'Functions': self.get_funcitions(),
                       'Foreign_Keys': discribed_foreign_keys,
                       'Checks': discribed_checks
                       }
                  }
        return scheme


if __name__ == "__main__":
    sch1 = Scheme(DB_URL, DB_USERNAME, DB_PASSWORD, DB_PORT, "test1")
    print(json.dumps(sch1.get_scheme()))
