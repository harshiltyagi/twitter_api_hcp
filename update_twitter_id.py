import pandas as pd
from pathlib import Path
import requests
import json
from retrying import retry
from requests_oauthlib import OAuth1Session
import time


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
    if user_data.status_code == 403:
      print(user_data.json())
      print(user_data.status_code)
      print("Invalid Input(id or screen_name)!! Returning None")
      return None
    elif user_data.status_code == 400:
      print(user_data.json())
      print(user_data.status_code)
      print("Invalid key!! Retrying with next set of creds")
      pass
    elif user_data.status_code == 429:
      print(user_data.json())
      print(user_data.status_code)
      print("Too Many Requests!! waiting for 60sec")
      time.sleep(120)
      pass
    elif user_data.status_code == 200 and user_data.json().get("errors", True):
      user_data.status_code
      return user_data.json()
    elif user_data.status_code == 400 or user_data.json().get("errors", False):
      user_data.status_code
      return user_data.json()
    else:
      print(user_data.json)
      print(user_data.status_code)
      raise ValueError('Status code not handled!!')


def get_twitter_user_id(screen_name, cred_list):
  endpoint = f"https://api.twitter.com/2/users/by?usernames={screen_name}"
  user_data = twitter_get_data(endpoint, cred_list)
  if user_data == None or user_data.get("errors", False):
    return ""
  return user_data["data"][0]["id"]


cred_list = [{"consumer_key":"<key_here>","consumer_secret":"<secret_here>","token_key":"<key_here>","token_secret":"<secret_here>"}]
path = "twitter_linking_data/hcp_raw_files"

# Get the files from the path
files = Path(path).glob('*.csv')

dfs = []
for f in files:
    data = pd.read_csv(f)
    dfs.append(data)

merged_df = pd.concat(dfs, ignore_index=True)

# fetches twitter ids for all the users
merged_df["twitter_id"] = merged_df['twitter_hander'].apply(lambda id: get_twitter_user_id(id,cred_list))

# saving refrence to make sure twitter api fetched data can be recovered
int_df = merged_df

# adding leading zeroes to hcp_uci column
merged_df['twitter_id'] = merged_df['twitter_id'].astype(str)
merged_df["twitter_id"] = merged_df['twitter_hander'].apply(lambda id: get_twitter_user_id(id,cred_list))
merged_df['hcp_uci'] = merged_df['hcp_uci'].astype(int)
int_df['hcp_uci'] = int_df['hcp_uci'].apply(lambda hcp_uci: str(hcp_uci).zfill(18))

# int_df["annotation_date"] = "2023-04-05"
int_df["hcp_id"] = "depricated"
int_df["one_key"] = "depricated"
int_df.loc[
    (int_df['specialty_group']=='ONCOLOGY') | 
    (int_df['specialty_group']=='HAEMATOLOGY') | 
    (int_df['specialty_group']=='HEMATOLOGY'),
    'specialty_group'
] = 'ONCO_HEMATO'
int_df.loc[int_df['specialty_group']=='OPTHALMOLOGY','specialty_group'] = 'OPHTHALMOLOGY'
int_df = int_df[int_df.twitter_id  != '']
int_df = int_df.drop('load_dts', axis=1)
int_df

# writing as filtered dataframe as per country code and speciality as <country_code>_hcp_<speciality>.csv
for country_code in int_df['country_code'].unique():
    cc_temp_df = int_df.loc[int_df['country_code'] == country_code]
    for speciality in cc_temp_df['specialty_group'].unique():
        sp_temp_df = cc_temp_df.loc[cc_temp_df['specialty_group'] == speciality]
        sp_temp_df.to_csv(f"final_new_hcp/{country_code.lower()}_hcp_{speciality.lower()}.csv", index=False)
