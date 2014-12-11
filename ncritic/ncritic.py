from bs4 import BeautifulSoup
import requests
import codecs

page_count = 1
next_page = True

url = 'http://blip.tv/pr/show_get_full_episode_list?users_id=246467&lite=1&esi=1&page='
f = open('output.txt', 'w')

while next_page == True:
	r = requests.get(url + str(page_count))
	print 'Page ' + str(page_count)
	data = r.text
	soup = BeautifulSoup(data)
	array_of_hs = soup.find_all("h3")
	if len(array_of_hs) == 0:
		next_page = False
	else:
		for h in soup.find_all("h3"):
			title = h.get('title')
			for a in h.find_all('a'):
				href = 'http://blip.tv' + a.get('href')
				s = title + "\n\t" + href + "\n"
				#s.decode("utf-8")
				f.write(s.encode('ascii', 'ignore'))
			#print h.get('title')
			#print h.text
		page_count += 1
f.close()
