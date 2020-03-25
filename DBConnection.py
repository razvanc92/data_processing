import psycopg2

host = 'fl-cs-chenjuan1.srv.aau.dk'
user = 'razvan'
password = '12345678'
port = 5432
db_name = 'california_traffic'


class DB(object):
    def __init__(self, host, user, password, db_name):
        try:
            connection = psycopg2.connect(user=user,
                                          password=password,
                                          host=host,
                                          port=port,
                                          database=db_name)
            cursor = connection.cursor()
            self._connection = connection
            self._cursor = cursor
            print('Connection successfully establish with PostgreSQL')
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def execute_command(self, command, success_message=None):
        self._cursor.execute(command)
        self._connection.commit()
        if success_message is not None:
            print(success_message)

    def execute_query(self, query):
        self._cursor.execute(query)
        data = self._cursor.fetchall()
        return data


db = DB(host, user, password, db_name)
