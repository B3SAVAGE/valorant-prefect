from prefect import task
import pandas as pd
import pyarrow
import fastparquet

@task
def transform(data):
    transform_data = pd.json_normalize(data, sep='_')

    agents_df = pd.DataFrame(transform_data)
    abilities_df = pd.json_normalize(agents_df.to_dict('records'), record_path=['abilities'], meta=['isBaseContent'])

    voiceline_df = pd.json_normalize(agents_df.to_dict('records'), record_path=['voiceLine_mediaList'],
                                     meta=['voiceLine_maxDuration'])

    voiceline_df.to_csv("voicelines.csv")
    abilities_df.to_csv("abilities.csv")
    agents_df.to_csv("agents.csv")

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

    # combined_df = pd.concat([agents_df, agents_df], axis=1)
    # combined_df.to_csv("combined.csv")
    # print(combined_df)
    # combined_df.to_csv("combined.csv")

    agents_df.to_parquet("agents.parquet")

    return agents_df
