import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def get_albums(spotify_data):
    albums = []
    for item in spotify_data['items']:
        album_id = item['track']['album']['id']
        album_name = item['track']['album']['name']
        album_release_date = item['track']['album']['release_date']
        album_total_tracks = item['track']['album']['total_tracks']
        album_url = item['track']['album']['external_urls']['spotify']
        album = {
            'id': album_id,
            'name': album_name,
            'release_date': album_release_date,
            'total_tracks': album_total_tracks,
            'url': album_url
            }
        albums.append(album)
    return albums

def get_artists(spotify_data):
    artists = []
    for row in spotify_data['items']:
        for key, value in row.items():
            if key == "track":
                for item in value['artists']:
                    artist = {'artist_id':item['id'], 'artist_name':item['name'], 'external_url': item['href']}
                    artists.append(artist)
    return artists

def get_songs(spotify_data):
    songs = []
    for row in spotify_data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                    'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                    'artist_id':artist_id
                   }
        songs.append(song)

    return songs


def lambda_handler(event, context):
    client = boto3.client('s3')
    bucket = 'spotify-etl-dandu'
    key='raw/to_be_processed/'
    data=[]
    keys=[]
    for file in client.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key=file['Key']
        if file_key.endswith('.json'):
            response = client.get_object(Bucket=bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            data.append(jsonObject)
            keys.append(file_key)

    for i in data:
        albums = get_albums(i)
        artists = get_artists(i)
        songs = get_songs(i)

        albums_df = pd.DataFrame.from_dict(albums)
        albums_df = albums_df.drop_duplicates(subset=['id'])
        artists_df = pd.DataFrame.from_dict(artists)
        artists_df = artists_df.drop_duplicates(subset=['artist_id'])
        songs_df = pd.DataFrame.from_dict(songs)

        ##Converting to datetime values    
        albums_df['release_date'] = pd.to_datetime(albums_df['release_date'])
        songs_df['song_added'] =  pd.to_datetime(songs_df['song_added'])

        songs_key = 'transformed/songs/songs_' + str(datetime.now()) + '.csv'
        songs_cache = StringIO()
        songs_df.to_csv(songs_cache, index=False)
        songs_body = songs_cache.getvalue()
        client.put_object(Bucket=bucket, Key=songs_key, Body=songs_body)

        albums_key = "transformed/albums/albums_" + str(datetime.now()) + ".csv"
        albums_buffer=StringIO()
        albums_df.to_csv(albums_buffer, index=False)
        albums_content = albums_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=albums_key, Body=albums_content)
        
        artists_key = "transformed/artists/artists_" + str(datetime.now()) + ".csv"
        artists_buffer=StringIO()
        artists_df.to_csv(artists_buffer, index=False)
        artists_content = artists_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=artists_key, Body=artists_content)
    
    s3_resource = boto3.resource('s3')
    for key in keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        dest_key = 'raw/processed/' + key.split('/')[-1]
        print(bucket,key,dest_key)
        client.copy_object(CopySource=copy_source,Bucket=bucket,Key=dest_key)
        client.delete_object(Bucket=bucket, Key=key)

