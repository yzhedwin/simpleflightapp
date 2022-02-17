# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
from configparser import ConfigParser
import psycopg2

CSV_FILE_PATH = "data/flightsdb.csv"  # let user select file location


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


class Ui:
    def __init__(self):
        self.command_list = 'Commands Available:\n' + \
                            'exit: Exits the program\n' + \
                            'set destination: Set where you want to travel to. Must be a name of an airport\n' + \
                            'set source: Set where you want to travel from. Must be a name of an airport\n' + \
                            'set departure: Sets the range of date that you wish to depart\n' + \
                            'direct: Query the database for direct flights to destination during desired departure ' \
                            'date\n' + \
                            'indirect: Query the database for indirect flights to destination during desired ' \
                            'departure date\n' + 'drop table: Removes a table from database'

    def start_message(self):
        print("Welcome to a simple flight AI, my name is DeeBee")
        print("Version 1.0")
        print(self.command_list)

    @staticmethod
    def exit_message():
        print("See you next time, hope I have been helpful")

    def help_message(self):
        print(self.command_list)


class Commands:
    def __init__(self):
        self.isExit = False
        self.depart_end = '10/08/2022'
        self.depart_start = '14/08/2022'
        self.destination = "'Incheon'"
        self.source = "'Singapore'"

    def parse(self, database, u, userinput):
        if userinput == 'exit':
            Ui.exit_message()
            self.isExit = True
        elif userinput == 'direct':
            filter_direct(database, self)
        # elif userinput == 'indirect':
        elif userinput == 'help':
            Ui.help_message(u)
        elif userinput == 'set destination':
            sql = "SELECT departure from flights UNION SELECT arrival from flights"
            database.cur.execute(sql)
            print("List of Airports available:")
            df = pd.DataFrame(database.cur.fetchall())
            print(df.rename(columns={df.columns[0]: "Airports"}))
            self.destination = set_destination()
            print("Source set to:" + self.destination)
            database.drop_table('direct_flights')
        elif userinput == 'set source':
            sql = "SELECT departure from flights UNION SELECT arrival from flights"
            database.cur.execute(sql)
            print("List of Airports available:")
            df = pd.DataFrame(database.cur.fetchall())
            print(df.rename(columns={df.columns[0]: "Airports"}))
            self.source = set_source()
            print("Source set to:" + self.source)
            database.drop_table('direct_flights')
        elif userinput == 'set departure':
            self.depart_end, self.depart_start = set_departure()
        elif userinput == 'drop table':
            print("Specify which table to drop")
            database.drop_table(input())
            print("dropped")


class Database:

    def __init__(self):
        self.data = pd.read_csv(CSV_FILE_PATH)
        self.cols = "Flight text," \
                    "Departure text," \
                    "Arrival text," \
                    "DepartureTime time," \
                    "ArrivalTime time," \
                    "Schedule text," \
                    "Price integer," \
                    "Days integer"
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        self.conn = psycopg2.connect(**params)
        # create a cursor
        self.cur = self.conn.cursor()
        # execute a statement
        print('PostgreSQL database version:')
        self.cur.execute('SELECT version()')
        # display the PostgreSQL database server version
        db_version = self.cur.fetchone()
        print(db_version)
        # executing SQL command
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        # when all operation is done then commit to the database to reflect changes
        list_tables = self.cur.fetchall()
        print("Input name of database")
        self.table = input()
        if (self.table,) in list_tables:
            print("'{}' Database already exist".format(self.table))
        else:
            print("'{}' Database not exist.".format(self.table))
            self.create_table(self.table)
            self.csv_to_psql()
            print("Creating new database...")
            self.conn.commit()

    def show_table_list(self):
        # executing SQL command
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        # when all operation is done then commit to the database to reflect changes
        tab = self.cur.fetchall()
        print('List of table in database: ')
        count = 1
        for i in tab:
            print(count, i[0])
            count += 1

    def query_table(self, sql):
        self.cur.execute(sql)
        return pd.DataFrame(self.cur.fetchall())

    def create_table(self, table_name):
        sql = "CREATE TABLE IF NOT EXISTS " + table_name + " " + '(' + self.cols + ')'
        self.cur.execute(sql)

    def csv_to_psql(self):
        sql = "COPY " + self.table + " FROM STDIN DELIMITER ',' CSV HEADER"
        self.cur.copy_expert(sql, open(CSV_FILE_PATH, "r"))

    def drop_table(self, table_name):
        self.cur.execute("DROP TABLE IF EXISTS {};".format(table_name))

    def close_connection(self):
        self.conn.commit()
        self.conn.close()


def runApp():
    D1 = Database()
    c = Commands()  # list of cmd query direct, query indirect, exit, newDeparture, newArrival
    u = Ui()
    rename_col(D1)
    u.start_message()
    while not c.isExit:
        # D1.show_table_list()
        print("Please enter a command")
        c.parse(D1, u, input())
    D1.close_connection()


def rename_col(D1):
    query = "Select * from flights"
    df = D1.query_table(query)
    df.rename(
        columns={df.columns[0]: "Flights", df.columns[1]: "Departure", df.columns[2]: "Arrival",
                 df.columns[3]: "DepartureTime",
                 df.columns[4]: "ArrivalTime", df.columns[5]: "Schedule", df.columns[6]: "Price",
                 df.columns[7]: "Days"}, inplace=True)


def filter_direct(database, c):
    print("Showing DIRECT flights from " + c.source + " to " + c.destination + " Sorted by PRICE")
    query_schedule = range_day(c.depart_start, c.depart_end)
    database.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    list_db = database.cur.fetchall()
    if ('direct_flights',) in list_db:
        print("'{}' Database already exist".format('direct_flights'))
        database.drop_table('direct_flights')
    database.create_table('direct_flights')
    query = "Insert INTO direct_flights Select * from flights where (Departure = " + c.source + " AND Arrival " + "= " + c.destination + ") AND " + query_schedule
    try:
        database.cur.execute(query)
        print("'{}' Database created".format('direct_flights'))
        query = "SELECT flight,departure,arrival, departuretime, arrivaltime,schedule,price FROM direct_flights ORDER BY price"
        df = database.query_table(query)
        df.rename(
            columns={df.columns[0]: "Flights", df.columns[1]: "Departure", df.columns[2]: "Arrival",
                     df.columns[3]: "DepartureTime",
                     df.columns[4]: "ArrivalTime", df.columns[5]: "Schedule", df.columns[6]: "Price"}, inplace=True)
        print(df)
    except IndexError:
        print("No flights found")
    except psycopg2.ProgrammingError:
        print("No results to fetch")


def set_destination():
    print("Input desired destination by Airport Name")
    destination = "'" + input() + "'"
    return destination


def set_source():
    print("Input current Airport")
    source = "'" + input() + "'"
    return source


def set_departure():
    print("Input desired departure date range in dd/mm/yyyy")
    print("Input earliest departure date")
    depart_start = input()
    print("Input latest departure date")
    depart_end = input()
    return depart_end, depart_start


def range_day(depart_end, depart_start):
    dict_day = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }
    depart_start_datetime = pd.to_datetime(depart_start, infer_datetime_format=True)
    depart_end_datetime = pd.to_datetime(depart_end, infer_datetime_format=True)
    if int(depart_end_datetime.day) - int(depart_start_datetime.day) >= 7:
        return "(schedule LIKE '%1%' OR" \
               "schedule LIKE '%2%' OR" \
               "schedule LIKE '%3%' OR" \
               "schedule LIKE '%4%' OR" \
               "schedule LIKE '%5%' OR " \
               "schedule LIKE '%6%' OR" \
               "schedule LIKE '%7%')"
    else:
        cond = "(schedule LIKE '%" + str(dict_day.get(str(depart_start_datetime.day_name()))) + "%'"
        for i in range(int(depart_start_datetime.day) + 1, int(depart_end_datetime.day) + 1):
            new_date = str(i) + '/' + str(depart_start_datetime.month) + '/' + str(depart_start_datetime.year)
            day_name = (pd.to_datetime(new_date, infer_datetime_format=True).day_name())
            cond = cond + " OR schedule LIKE '%" + str(dict_day.get(str(day_name))) + "%'"
        cond = cond + ")"
        return cond


if __name__ == '__main__':
    runApp()
