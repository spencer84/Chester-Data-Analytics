import sqlite3


def create_db(db_name='cda.db'):
    """
    Create a db file - If a database doesn't already exists, the connect() function will create one in local dir
    :param db_name: Filename of the database, default name is 'cda.db'
    :return:
    """
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    def create_epc(cursor):
        """
        Create a table for EPC records
        :param cursor: sqlite3 cursor object from db connection
        :return:
        """
        cursor.execute('''CREATE TABLE IF NOT EXISTS epc 
                    (address text, address1 text ,uprn text,postcode text, current_energy_rating text, 
                    total_floor_area real, lodgement_datetime text, query_date text
                    )''')
        return
    def create_landreg(cursor):
        """
        Create a table for Land Registry Price Paid records
        :param cursor: sqlite3 cursor object from db connection
        :return:
        """
        cursor.execute("""CREATE TABLE IF NOT EXISTS landreg (postcode_district text, postcode text, 
         PAON text, street_name text, transaction_date text, price_paid int, query_date text)""")
    def create_dlog(cursor):
        """
        Create a log for when other tables are updated and which postcodes are included
        :param cursor: sqlite3 cursor object from db connection
        :return:
        """
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_log (postcode_district text, epc bool, 
        epc_date text, land_reg bool, land_reg_date text)""")
        return

    create_epc(cur)
    create_landreg(cur)
    create_dlog(cur)
    con.close()


if __name__ == "__main__":
    create_db()
