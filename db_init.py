import sqlite3

#
def create_db(db_name = 'cda.db'):
    """
    Create a db file - If a database doesn't already exists, the connect() function will create one in local dir
    :param db_name: Filename of the database, default name is 'cda.db'
    :return:
    """
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    def create_epc(cur):
        """
        Create a table for EPC records
        :param cur:
        :return:
        """
        cur.execute('''CREATE TABLE IF NOT EXISTS epc 
                    (address text, address1 text ,uprn text,postcode text, current_energy_rating text, total_floor_area real,
                    lodgement_datetime text, query_date text
                    )''')
        return

    def create_dlog(cur):
        """
        Create a log for when other tables are updated and which postcodes are included
        :param cur:
        :return:
        """
        cur.execute("""CREATE TABLE IF NOT EXISTS data_log (postcode_district text, epc bool, 
        epc_date text, land_reg bool, land_reg_date text)""")
        return
    create_epc(cur)
    create_dlog(cur)
    con.close()

if __name__ == "__main__":
    create_db(db_name= 'test1.db')
