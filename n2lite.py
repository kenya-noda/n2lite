#!/usr/bin/env python3

import sqlite3

__version__ = "0.2.0"


class N2lite(object):
    
    def __init__(self, dbpath):
        """
        example:
            dbpath = "/home/amigos/data/logger/sample.db"
        """
        self.dbpath = dbpath
        self.con = sqlite3.connect(self.dbpath, check_same_thread=False)
        pass
    
    def __del__(self):
        self.con.close()
        return
    
    def open(self):
        """
        for multithread, because sqlite cannnot connect beyond thread.
        """
        self.con = sqlite3.connect(self.dbpath, check_same_thread=False)

    def close(self):
        self.con.close()
        return

    def commit_data(self):
        self.con.commit()
        return

    def make_table(self, table_name, param):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "('2l' float, '2r' float, time float, ... )"
        """
        self.con.execute("CREATE table if not exists {} {}".format(table_name, param))
        return

    def write(self, table_name, param, values, auto_commit = True):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "('2l', '2r')" or '' (all param write)
            values = "(1.0, 2.0)"

            if autocommit = False, you must call commit_data function 
                after calling write function.
        """
        print(values)
        if auto_commit:
            with self.con:
                print("INSERT into {0} {1} values {2}".format(table_name, param, values))
                self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        else:
            self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, tuple(values)))
        return

    def read(self, table_name, param="*"):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "'2l', '2r', time" or "*" (all param read)
        return:
            [[1.2, 1.3, .....], [...]]
        """
        row = self.con.execute("SELECT {0} from {1}".format(param, table_name)).fetchall()
        if not row == []:
            data = [
                [row[i][j] for i in range(len(row))] 
                    for j in range(len(row[0]))
                    ]
        else : data = []
        return data

    def check_table(self):
        """
        get information about all table
        example:
            type = table
            name = SIS_VOLTAGE
            tbl_name = SIS_VOLTAGE
            rootpage = 3
            sql = CREATE TABLE SIS_VOLTAGE ('2l', '2r')
        """
        row = self.con.execute("SELECT * from sqlite_master")
        info = row.fetchall()
        return info
