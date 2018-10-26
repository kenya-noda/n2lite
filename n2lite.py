#!/usr/bin/env python3

import sqlite3

__version__ = "0.0.1"


class N2lite(object):
    
    def __init__(self):
        pass

    def open_connection(self, dbpath):
        """
        example:
            dbpath = "/home/amigos/data/logger/sample.db"
        """
        connection = sqlite3.connect(dbpath)
        return connection

    def make_cursor(self, connection):
        """
        example:
            connection = open_connection(dbpath)
        """
        cursor = connection.cursor()
        return cursor

    def commit_data(self, connection):
        connection.commit()
        return

    def close_connection(self, connection):
        connection.close()
        return

    def make_table(self, cursor, table_name, param):
        """
        example:
            cursor = make_cursor(connection)
            table_name = "SIS_VOLTAGE"
            param = ('2l' float, '2r' float, ... )
        """
        cursor.execute("CREATE table if not exists {} {}".format(table_name, param))
        return

    def write(self, cursor, table_name, param, values):
        """
        example:
            cursor = make_cursor(connection)
            table_name = "SIS_VOLTAGE"
            param = "('2l', '2r')" or '' (all param write)
            values = (1.0, 2.0) 
        """
        cursor.execute("INSERT into {0} {1} values {2}".format(table_name, param, str(values)))
        return

    def read(self, cursor, table_name, param):
        """
        example:
            cursor = make_cursor(connection)
            table_name = "SIS_VOLTAGE"
            param = '2l' or '*' (all param read)  # TODO: multi param reading
        """
        row = cursor.execute("SELECT {0} from {1}".format(param, table_name))
        data = row.fetchall()
        return data

    def check_table(self, cursor):
        """
        get information about all table
        example:
            type = table
            name = SIS_VOLTAGE
            tbl_name = SIS_VOLTAGE
            rootpage = 3
            sql = CREATE TABLE SIS_VOLTAGE ('2l', '2r')
        """
        row = cursor.execute("SELECT * from sqlite_master")
        info = row.fetchall()
        return info
