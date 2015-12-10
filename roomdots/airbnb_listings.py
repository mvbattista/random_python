import requests
import sys
import json
import time
import psycopg2
import itertools

class AirbnbListingsParser(object):
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
        table_name = 'city_listings'
        insert_tuple_list = list()
        print('*** BEGINNING DATABASE INSERT ***')

        for dict_id in report_data:
            dict_data = report_data[dict_id]
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
            last_update_query = 'UPDATE ' + staging_table_name + ' SET last_update = NOW()'

            try:
                self.cursor.execute(last_update_query)
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

    def search_url(self, loc, offset, page_limit):
        return ('https://m.airbnb.com/api/-/v1/listings/search?location=%s&offset=%s&items_per_page=%s'
                % (loc, offset, page_limit))

    def listing_url(self, listing_id):
        return ('https://m.airbnb.com/api/-/v1/listings/%s/' % listing_id)

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
        while True:
            r = self.get(self.search_url(location, offset, page_limit), referer='https://m.airbnb.com/s/%s' % location)
            try:
                js = json.loads(r.text)
                try:
                    if js['listings_count'] == 1001:
                        count = js['listings_count'] - 1
                    else:
                        count = js['listings_count']
                except KeyError:
                    count = 0
                    print('KeyError, count now set to 0')
                if len(js['listings']) == 0:
                    break
                else:
                    new_listings = [listing['listing'].get('id', None) for listing in js['listings']
                                    if listing['listing']['id'] not in found_listings_ids]
                    search_city = js['geography'].get('city', None)
                    availability = json.dumps(js['facets']['availability'], sort_keys=True, separators=(',', ':'))
                    for l in new_listings:
                        found_listings_ids.add(l)
                        listings[l] = {'search_city': search_city, 'availability': availability, 'site': 'Airbnb',}
            except ValueError as e:
                sys.exit(1)

            if (offset + page_limit < count):
                offset += page_limit
                if offset + page_limit > count:
                   page_limit = count-offset
                page_count += 1
                clicks_first_loop = False
            else:
                break

        db_entries = []
        db_fields = ["site", "room_id", "last_update", "search_city", "user_id", "room_user", "address", "city", 
            "state", "zip_code", "country", "street", "neighborhood", "latitude", "longitude", "name", 
            "description", "language", "currency", "map_image", "thumbnail", "thumbnails", "photos", 
            "photo_captions", "price", "price_extra_person", "security_deposit", "property_type", 
            "room_type", "square_feet", "beds", "bedrooms", "bathrooms", "bed_type", "capacity", 
            "guests_included", "has_availability", "max_nights", "min_nights", "amenities", 
            "availability", "cancel_policy", "in_building", "check_in", "check_out", "summary", 
            "special_off", "neighborhood_overview", "house_rules", "rating", "weekend_price", 
            "weekly_price", "monthly_price", "cleaning_fee", ]
        l_fields = ("user_id", )
        
        # DB_field => API Field
        translation = {"room_id": "id", "last_update": "last_update", 
            "user_id": "user_id", "room_user": "user", "address": "address", "city": "city", "state": "state", 
            "zip_code": "zipcode", "country": "country", "street": "street", "neighborhood": "neighborhood", 
            "latitude": "lat", "longitude": "lng", "name": "name", "description": "description", 
            "language": "language", "currency": "listing_native_currency", "map_image": "map_image_url", "thumbnail": "thumbnail_url", 
            "thumbnails": "thumbnail_urls", "photos": "photos", "photo_captions": "picture_captions", "price": "price", 
            "price_extra_person": "price_for_extra_person_native", "security_deposit": "security_deposit_native", "property_type": "property_type", 
            "room_type": "room_type", "square_feet": "square_feet", "beds": "beds", "bedrooms": "bedrooms", "bathrooms": "bathrooms", 
            "bed_type": "bed_type", "capacity": "person_capacity", "guests_included": "guests_included", 
            "has_availability": "has_availability", "max_nights": "max_nights", "min_nights": "min_nights", 
            "amenities": "amenities", "cancel_policy": "cancellation_policy", "in_building": "in_building", 
            "check_in": "check_in_time", "check_out": "check_out_time", "summary": "summary", "special_off": "special_offer", 
            "neighborhood_overview": "neighborhood_overview", "house_rules": "house_rules", "rating": "star_rating", 
            "weekend_price": "listing_weekend_price_native", "weekly_price": "listing_weekly_price_native", 
            "monthly_price": "listing_monthly_price_native", "cleaning_fee": "listing_cleaning_fee_native", }
        for l_id, l_data in listings.items():
            q = l_data.copy()
            attempts = 0
            while attempts < 3:
                r = self.get(self.listing_url(l_id), referer='https://m.airbnb.com/s/%s' % l_id)
                try:
                    js = json.loads(r.text)
                    if 'listing' in js:
                        z = js['listing']
                        break
                    else:
                        print(str(l_id) + ' request failed, skipping...')
                        break
                except ValueError as e:
                    print('received ValueError:', e, file=sys.stderr)
                    attempts += 1
            if attempts == 3:
                print(r.text)
                sys.exit(1)

            db_entries.append(q)
            for d_f, a_f in translation.items():
                q[d_f] = z.get(a_f, None)
            for n in ('room_user', 'photos', 'availability'):
                q[n] = json.dumps(q[n], separators=(',', ':'))
            listings[l_id] = q

        # pickle.dump(listings, open('listings.p', 'wb'))
        self.__insert_into_db(db_fields, ['room_id'], listings)
            

if __name__ == "__main__":

    ab = AirbnbListingsParser()
    print('start')
    start_time = time.time()
    ab.crawl()
    end_time = time.time()
    print(str(end_time - start_time))
