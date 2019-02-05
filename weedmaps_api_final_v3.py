import requests
import numpy
import json
import urllib
import sys
import sqlite3

conn = sqlite3.connect('weedmaps.sqlite')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS US_Dispensaries')

cur.execute('CREATE TABLE US_Dispensaries (business TEXT, address TEXT, city TEXT, state TEXT, zip_code INTEGER, timezone TEXT, license_type TEXT, type_desc TEXT, web_url TEXT, count INTEGER)')

state_list = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware', 'florida', 
'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 
'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new-hampshire', 
'new-jersey', 'new-mexico', 'new-york', 'north-carolina', 'north-dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 
'rhode-island', 'south-carolina', 'south-dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 
'west-virginia', 'wisconsin', 'wyoming', 'puerto-rico']

for place in state_list:
	complete = 0
	page_count = 1


	while page_count <= 1000:

		page_countr = str(page_count)

		service_urla1 = 'https://api-g.weedmaps.com/wm/v2/listings?filter%5Bplural_types%5D%5B%5D=doctors&filter%5Bplural_types%5D%5B%5D=dispensaries&filter%5Bplural_types%5D%5B%5D=deliveries&filter%5Bregion_slug%5Bdeliveries%5D%5D='
		service_urla2 = '&filter%5Bregion_slug%5Bdispensaries%5D%5D='
		service_urla3 = '&filter%5Bregion_slug%5Bdoctors%5D%5D='
		service_urla4 = '&page_size=100&size=100&page='
		url = service_urla1 + place + service_urla2 + place + service_urla3 + place + service_urla4 + page_countr

		uh = urllib.request.urlopen(url)
		data = uh.read()
		
		if page_count % 100 == 0:
			complete = 1
		else:
			page_count = page_count + 1

		try:
			js = json.loads(data)
		except:
			js = None
			print(state,'_fail')

		for numb in range(0,100):
			try:
				business = js["data"]["listings"][numb]["name"].encode("utf-8")
				state = js["data"]["listings"][numb]["state"].encode("utf-8")
				city = js["data"]["listings"][numb]["city"].encode("utf-8")
				type_desc = js["data"]["listings"][numb]["type"].encode("utf-8")
				web_url = js["data"]["listings"][numb]["web_url"].encode("utf-8")
				license_type = js["data"]["listings"][numb]["license_type"].encode("utf-8")
				address = js["data"]["listings"][numb]["address"].encode("utf-8")
				zip_code = js["data"]["listings"][numb]["zip_code"].encode("utf-8")
				timezone = js["data"]["listings"][numb]["timezone"].encode("utf-8")


				cur.execute('SELECT count FROM US_Dispensaries WHERE business = ? AND address = ? AND city = ? AND state = ? AND zip_code = ? AND timezone = ? AND license_type = ? AND type_desc = ? AND web_url = ?', (business, address, city, state, zip_code, timezone, license_type, type_desc, web_url,))
				row = cur.fetchone()
				if row is None:
					cur.execute('INSERT INTO US_Dispensaries (business, address, city, state, zip_code, timezone, license_type, type_desc, web_url, count) VALUES (?,?,?,?,?,?,?,?,?,1)', (business, address, city, state, zip_code, timezone, license_type, type_desc, web_url))
				else:
					cur.execute('UPDATE US_Dispensaries SET count = count + 1 WHERE business = ? AND address = ? AND city = ? AND state = ? AND zip_code = ? AND timezone = ? AND license_type = ? AND type_desc = ? AND web_url = ?',
								(business, address, city, state, zip_code, timezone, license_type, type_desc, web_url,))
				conn.commit()

				cur.execute('SELECT business FROM US_Dispensaries WHERE count = 150')
				check_completion = cur.fetchone()
				if check_completion is None:
					continue
				else:
					complete = 1
					break
				conti
			except:
				continue
		if complete == 1:
			print('Completed:',place)
			break
	if place == 'puerto-rico':
		break
cur.close()
print('All Done!')
