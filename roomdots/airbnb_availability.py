import requests
import sys
import json
import time
import psycopg2
import itertools
from datetime import date, timedelta

class AirbnbAvailabilityParser(object):
    def __init__(self, debug=True):
        self.cookies = {}
        self.debug = debug
        self.db = None
        try:
            self.db=psycopg2.connect("dbname='roomdots' user='roomdots' host='localhost' password='roomdots'")
        except:
            print("I am unable to connect to the database.")
            sys.exit(1)
        self.cursor = self.db.cursor()

    def __insert_into_db(self, fieldnames, key_fieldnames, report_data):
        #staging_table_name = 'in_dcm_app_daily_staging'
        table_name = 'city_listing_availability'
        insert_tuple_list = list()
        print('*** BEGINNING DATABASE INSERT ***')

        for dict_data in report_data:
            insert_list = list()
            for field in fieldnames:
                if dict_data[field] == '' or dict_data[field] is None:
                    insert_list.append(None)
                else:
                    insert_list.append(dict_data[field])
            if len(insert_list) < len(fieldnames):
                print("length error")
                sys.exit()
            insert_tuple_list.append(tuple(insert_list))


        staging_table_name = table_name + '_staging'
        update_fieldnames = tuple([x for x in fieldnames if x not in key_fieldnames])
        fieldnames_str = "(" + ", ".join(fieldnames) + ")"
        fieldnames_s_list = itertools.repeat('%s', len(fieldnames))

        insert_staging_query = 'INSERT INTO ' + staging_table_name + ' ' + fieldnames_str + ' VALUES (' + \
            ', '.join(fieldnames_s_list) + ')'

        update_count = 0
        insert_count = 0
        try:
            chunk_size = 1
            for i in range(0, len(insert_tuple_list), chunk_size):
                staging_insert_list = insert_tuple_list[i:i+chunk_size]
                self.cursor.executemany(insert_staging_query, staging_insert_list)
                self.db.commit()
            join_list = ['a.' + x + ' = b.' + x for x in key_fieldnames]
            update_list = [x + ' = b.' + x for x in update_fieldnames]

            update_query = 'UPDATE ' + table_name + ' AS a SET ' + ', '.join(update_list) + ' FROM ' + staging_table_name + ' AS b WHERE ' + \
                ' AND '.join(join_list)
            insert_query = 'INSERT INTO ' + table_name + ' SELECT a.* FROM ' + staging_table_name + \
                ' AS a LEFT OUTER JOIN ' + table_name + ' AS b ON (' + ' AND '.join(join_list) + ') WHERE b.' + \
                key_fieldnames[0] + ' IS NULL'

            try:
                self.cursor.execute(update_query)
                update_count = self.cursor.rowcount
                self.cursor.execute(insert_query)
                insert_count = self.cursor.rowcount
                self.cursor.execute('TRUNCATE TABLE ' + staging_table_name)
                self.db.commit()
            except psycopg2.Error as e:
                print("Error: %s" % (e.args[0]))
                self.db.rollback()
                self.cursor.execute('TRUNCATE TABLE ' + staging_table_name)
                self.db.commit()

            print(table_name, '- INSERT:', insert_count)
            print(table_name, '- UPDATE:', update_count)

        except psycopg2.Error as e:
            print("Error: %s" % (e.args[0]))
            self.db.rollback()

    def availability_url(self, listing_id):
        end_date = date.today() + timedelta(days=90)
        return ('https://m.airbnb.com/api/-/v1/listings/%s/calendar?end_date=%s' % (listing_id, end_date.isoformat()))

    def get(self, url, referer='', xhr=False):
        if self.debug:
            print(url)

        time.sleep(1)

        headers = {'User-agent': 'Mozilla/5.0 (Linux; U; Android 2.3; en-us) AppleWebKit/999+ (KHTML, like Gecko) Safari/999.9',
                   'referer': referer}
        if xhr:
            headers['x-requested-with'] = 'XMLHttpRequest'

        r = requests.get(url, headers=headers, cookies=self.cookies)
        self.cookies = r.cookies

        return r


    def crawl(self):

        listings = {}
        location = 'NY, United States'
        offset = 0
        page_count = 0
        page_limit = 25
        found_listings_ids = set()


        search_first_loop = True

        self.cursor.execute('SELECT room_id FROM city_listings')

        listing_dates_data = list()

        room_ids = [e for l in self.cursor.fetchall() for e in l]
        for room_id in room_ids:
            r = self.get(self.availability_url(room_id), referer='https://m.airbnb.com/s/%s' % location)
            try:
                js = json.loads(r.text)
                try:
                    dates_list = js['calendar']['dates']
                except KeyError:
                    print('Could not load dates for room ID ' + room_id)
                    continue
                for date in dates_list:
                    q = {'site': 'Airbnb', 'room_id': room_id}
                    for f in ('date', 'available'):
                        try:
                            q[f] = date[f]
                        except KeyError:
                            print('Passing, could not match ' + f)
                            break
                    q['has_availability'] = q['available']
                    del q['available']
                    q['price'] = date.get('price_native', None)
                    listing_dates_data.append(q)

            except ValueError as e:
                print('Could not load JSON')
                print(r.text)
                continue

        db_fields = ["site", "room_id", "date", "has_availability", "price" ]
        key_fields = ["site", "room_id", "date", "has_availability",]
        
        self.__insert_into_db(db_fields, key_fields, listing_dates_data)
            

if __name__ == "__main__":

    ab = AirbnbAvailabilityParser()
    print('start')
    start_time = time.time()
    ab.crawl()
    end_time = time.time()
    print(str(end_time - start_time))
