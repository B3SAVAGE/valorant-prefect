import json

import pandas as pd
import requests
import pandas
import prefect
import pyarrow
import fastparquet

# @task
# def get_endpoint():
# response = requests.get("https://valorant-api.com/v1/agents")
# data = response.json()["data"]
#
# data = pd.json_normalize(data, sep='_')
#
# agents_df = pd.DataFrame(data)
# abilities_df = pd.json_normalize(agents_df.to_dict('records'), record_path=['abilities'], meta=['isBaseContent'])
#
#
# voiceline_df = pd.json_normalize(agents_df.to_dict('records'), record_path=['voiceLine_mediaList'], meta=['voiceLine_maxDuration'])

# voiceline_df.to_csv("voicelines.csv")
# abilities_df.to_csv("abilities.csv")
# agents_df.to_csv("agents.csv")

voiceline_df = pd.read_csv("voicelines.csv")
agents_df = pd.read_csv("agents.csv")
abilities_df = pd.read_csv("abilities.csv")


agents_df.drop(['voiceLine_mediaList'], axis=1)

agents_df = agents_df.drop(columns=['Unnamed: 0'])
agents_df = agents_df.drop(columns=['voiceLine_mediaList'])
agents_df = agents_df.drop(columns=['role'])

voiceline_df = voiceline_df.drop(columns=['Unnamed: 0'])

abilities_df = abilities_df.drop(columns=['Unnamed: 0'])

abilities_df = abilities_df.reset_index(drop=True)
agents_df = agents_df.reset_index(drop=True)
voiceline_df = voiceline_df.reset_index(drop=True)

# combined_df = pd.concat([agents_df, abilities_df], axis= 1)
# combined_df.to_csv("combined.csv")
# print(combined_df)
agents_df.to_parquet("agents_df.parquet")
