from pathlib import Path
import numpy as np
import pandas as pd
import requests
import json
from retrying import retry
from requests_oauthlib import OAuth1Session
import time
import os


def twitter_get_session(consumer_key, consumer_secret, access_token, access_token_secret):
  try:
    oauth_session = OAuth1Session(
    client_key=consumer_key,
    client_secret=consumer_secret,
    resource_owner_key=access_token,
    resource_owner_secret=access_token_secret
    )
    return oauth_session
  except Exception as e:
    print("Unable to create oauth_session")
    print(e)
    raise


def twitter_get_data(endpoint, cred_list):
  for cred in cred_list:
    consumer_key = cred["consumer_key"]
    consumer_secret = cred["consumer_secret"]
    access_token = cred["token_key"]
    access_token_secret = cred["token_secret"]
    oauth_session = twitter_get_session(
      consumer_key,
      consumer_secret,
      access_token,
      access_token_secret
    )
    user_data = oauth_session.get(endpoint)
    if user_data.status_code == 403 and user_data.json().get("reason") != 'client-not-enrolled':
      print(user_data.json())
      print(user_data.status_code)
      print("Invalid Input(id or screen_name)!! Returning None")
      return None
    elif user_data.status_code == 400 or user_data.json().get("reason") == 'client-not-enrolled':
      print(user_data.json())
      print(user_data.status_code)
      print("Invalid key!! Retrying with next set of creds")
    elif user_data.status_code == 429:
      print(user_data.json())
      print(user_data.status_code)
      print("Too Many Requests!!")
    elif user_data.status_code == 200 and user_data.json().get("errors", True):
      print(user_data.status_code)
      return user_data.json()
    elif user_data.status_code == 200 and user_data is None:
      print("Tokenization Timed out!")
      return None
    elif user_data.status_code == 200 and user_data.json().get("errors", True):
      print(user_data.status_code)
      return user_data.json()
    elif user_data.status_code == 400 or user_data.json().get("errors", False):
      print(user_data.status_code)
      return user_data.json()
    else:
      print(user_data.json())
      print(user_data.status_code)
      raise ValueError('Status code not handled!!')
  print("Exhausted all keys! Waiting for 15min")
  time.sleep(900)
  twitter_get_data(endpoint, cred_list)


def get_twitter_followers(user_id, cred_list):
  followers_url = f"https://api.twitter.com/2/users/{user_id}/followers"
  user_data = twitter_get_data(followers_url, cred_list)
  if user_data == None or user_data.get("errors", False):
    return []
  final_list = user_data["data"]
  while 'next_token' in user_data["meta"]:
    pagination_token = user_data["meta"]["next_token"]
    pagination_token_chunk = f"?pagination_token={pagination_token}"
    user_data = twitter_get_data(followers_url+pagination_token_chunk, cred_list)
    final_list += user_data["data"]
  return final_list


def get_twitter_following(user_id, cred_list):
  final_list = []
  print(user_id)
  following_url = f"https://api.twitter.com/2/users/{user_id}/following"
  user_data = twitter_get_data(following_url, cred_list)
  print(user_data)
  if user_data == None or user_data.get("errors", False):
    return []
  final_list += user_data["data"]
  while user_data.get("next_token", False):
    if user_data["meta"] is None:
      print("Tokenization Timed out!")
      print(user_data)
    else:
      pagination_token = user_data["meta"]["next_token"]
      pagination_token_chunk = f"?pagination_token={pagination_token}"
      user_data = twitter_get_data(following_url+pagination_token_chunk, cred_list)
      print(user_data)
      if user_data == None or user_data.get("errors", False):
          final_list += []
      else:
          final_list += user_data["data"]
  return final_list


cred_list = [{"consumer_key":"<key_here>","consumer_secret":"<secret_here>","token_key":"<key_here>","token_secret":"<secret_here>"}]

path = "twitter_linking_data/combined_hcp_data"
df_chunk_size = 7

# Get the files from the path
files = Path(path).glob('*.csv')

dfs = list()
try:
    for f in files:
        df_base = pd.read_csv(f)
        part_len_list = [i for i in range(0, len(df_base), df_chunk_size)]
        part_len_list.append(len(df_base) + 1)
        print(part_len_list)
        for part in range(len(part_len_list) - 1):
            start_row = part_len_list[part]
            end_row = part_len_list[part + 1]
            df_chunk = df_base[start_row:end_row]
            df_chunk["twitter_following"] = df_chunk['twitter_id'].apply(lambda id: get_twitter_following(int(id),cred_list))
            outdir = str(f).replace("combined_hcp_data","ff_data").replace(".csv","")
            if not os.path.exists(outdir):
                os.mkdir(outdir)
            print(outdir + "/part_" + str(start_row) + ".csv")
            df_chunk.to_csv(outdir + "/part_" + str(start_row) + ".csv", index=False)
        dfs.append(str(f))
finally:
    with open('done_list.txt', 'a+') as f:
        for line in dfs:
            f.write(f"{line}\n")
