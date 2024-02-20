import requests


G_API_KEY = '...'
#Google 
G_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

payload = {
	'destinations': '25.052676976485934,121.52099887200537', #中山站
	'origins': '25.051865021429354,121.5440804694192', #南京復興站
	'key': G_API_KEY
}

r = requests.get(url=G_URL, params=payload)
print(r.text)


def generate_destinations(lng: float, lat: float):
	'''based on a dock location, generate the travel destinations to build density map'''
	pass

def get_travel_time():
	'''wrapper function to call Direction Matrix API and extract relevant data'''