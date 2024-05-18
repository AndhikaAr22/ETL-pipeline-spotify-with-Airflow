import requests
import base64
import json
from airflow.models import Variable


# class spotify
class Spotify:
    client_id_spotify = Variable.get('client_id_spotify')
    client_secret_spotify = Variable.get('client_secret_spotify')
    url_token = Variable.get('url_token_spotify')
    def __init__(self, url_param):
        self.url_param = url_param
  
        
    def get_token_spotify(self):
        token_url = self.url_token
        credentials = f'{self.client_id_spotify}:{self.client_secret_spotify}'
        # print(credentials.encode())
        # print('=================')
        base64_credentials = base64.b64encode(credentials.encode()).decode()
        # print(base64_credentials)
        # print('=================')
        # print(base64.b64encode(credentials.encode()))
        payload = {
            'grant_type':'client_credentials'
        }   

        # header untuk minta
        headers = {
            'Authorization': f'Basic {base64_credentials}'
        }

        # Kirim permintaan untuk mendapatkan token
        response = requests.post(token_url, data=payload, headers=headers)

        # Periksa apakah permintaan berhasil
        if response.status_code == 200:
            token_data = response.json()
            token = token_data['access_token']
            print(f"Token Spotify: ----> sudah ada")
            return token
        else:
            print("Gagal mendapatkan token Spotify.")
            return None

    def get_id_artist(self):
        id_artist = []
        name_artist = []
        token_spotify = self.get_token_spotify()
        url = self.url_param
        params = {
            'q': 'genre:"indonesian"',
            'type': 'artist',
            'limit': 50,
            'market': 'ID'
        }
        headers = {
            'Authorization': f'Bearer {token_spotify}'  
        }
        response = requests.get(url, params=params, headers=headers)
        search_data = response.json()
        with open('/opt/airflow/dags/data/search_data.json', 'w') as f:
            json.dump(search_data, f)
        for artist in search_data['artists']['items']:
            artis_name = artist['name']
            artis_id = artist['id']
            name_artist.append(artis_name)
            id_artist.append(artis_id)

        print("Nama-nama artis dari market Indonesia: ---> sudah ada")
        print(name_artist)
        print("ID artis dari market Indonesia:---> sudah ada")
        # print(id_artist)
        return id_artist

    def get_all_data_spotify(self):
        all_source_data = []
        token_spotify = self.get_token_spotify()
        data_id = self.get_id_artist()
        # headers token
        headers = {
            'Authorization': f'Bearer {token_spotify}'
        }
        for artist_id in data_id:
            # url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=ID'
            url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks'
            top_track_response = requests.get(url, headers=headers)
            source_data = top_track_response.json()
            all_source_data.append(source_data)
        # print(all_source_data)
        return all_source_data


