# -*- coding: utf-8 -*-

class Database(object):

    def __init__(self, connection):
        self.connection = connection
        print('Database initialized')

    def exec(self, query=None):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        print('Query executed')
        return result

    def close(self):
        self.connection.close()
        print('Database closed')