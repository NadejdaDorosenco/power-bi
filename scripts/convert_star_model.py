import pandas as pd
import numpy as np


def create_date_table(df):
    df["datetime"] = pd.to_datetime(dict(year=df_carac.an, month=df.mois, day=df.jour, hour=df.hrmn.str.split(":").str[0], minute=df.hrmn.str.split(":").str[1]))
    unique_dates = pd.Series(sorted(df["datetime"].unique()))
    data = {
        "id_date": np.arange(0, len(unique_dates)),
        "an": unique_dates.dt.year,
        "mois": unique_dates.dt.month,
        "jour": unique_dates.dt.day,
        "hrmn": unique_dates.dt.time,
        "datetime": unique_dates,
    }
    df_date = pd.DataFrame(data=data)

    return df_date

def create_event_table(df_carac):
    list_of_event = df_carac["Num_Acc"].unique()
    data = {"Num_Acc": list_of_event}

    return pd.DataFrame(data=data)

def create_fact_table(df_vehicule, df_usager, df_carac, df_lieu, df_date, df_event):
    df_carac = df_carac.merge(df_date, on='datetime', how="left")[["id_date", "Num_Acc"]]
    df_usager_and_vehicule = df_usager.merge(df_vehicule, on=['id_vehicule','Num_Acc'], how="outer")

    df_merged = df_usager_and_vehicule.merge(df_carac, on="Num_Acc", how="outer")\
        .merge(df_lieu, on="Num_Acc", how="outer").merge(df_event, on="Num_Acc", how="outer")
    df_merged = df_merged[df_merged["id_usager"].notna() & df_merged["id_lieu"].notna() &
                          df_merged["Num_Acc"].notna() & df_merged["id_date"].notna() &
                          df_merged["id_vehicule"].notna()]
    data = {
        "id_usager": df_merged["id_usager"].astype(int),
        "id_lieu": df_merged["id_lieu"],
        "id_event": df_merged["Num_Acc"],
        "id_date": df_merged["id_date"],
        "id_vehicule": df_merged["id_vehicule"]
    }
    df_fact_table = pd.DataFrame(data=data)

    return df_fact_table

def clean_tables(df_date, df_lieu, df_usager, df_vehicule, df_event):
    df_date.drop("datetime", axis=1, inplace=True)
    df_lieu.drop("Num_Acc", axis=1, inplace=True)
    df_usager.drop(["Num_Acc","id_vehicule","num_veh"], axis=1, inplace=True)
    df_vehicule.drop("Num_Acc", axis=1, inplace=True)
    df_event.rename(columns={"Num_Acc": "id_event"}, inplace=True)


if __name__ == '__main__':
    df_vehicule = pd.read_csv("data/vehicules-2021.csv", sep=";")
    df_usager = pd.read_csv("data/usagers-2021.csv", sep=";")
    df_carac = pd.read_csv("data/carcteristiques-2021.csv", sep=";")
    df_lieu = pd.read_csv("data/lieux-2021.csv", sep=";")

    # rajout d'une clé primaire pour les tables qui n'en possèdent pas
    df_usager["id_usager"] = np.arange(0, df_usager.shape[0])
    df_carac["id_carac"] = np.arange(0, df_carac.shape[0])
    df_lieu["id_lieu"] = np.arange(0, df_lieu.shape[0])

    # rajout d'information dans la table lieu provenant de la table caracteristique
    df_lieu[["dep","com","agg","lat","long"]] = df_carac[df_carac["Num_Acc"] == df_lieu["Num_Acc"]][["dep","com","agg","lat","long"]]

    df_date = create_date_table(df_carac)
    df_event = create_event_table(df_carac)
    df_event_fact_table = create_fact_table(df_vehicule, df_usager, df_carac, df_lieu, df_date, df_event)

    # clean tables
    clean_tables(df_date, df_lieu, df_usager, df_vehicule, df_event)

    #save new model
    path_to_saved = "data/data_transform/"

    df_vehicule.to_csv(path_to_saved + "table_vehicule.csv", index=False)
    df_usager.to_csv(path_to_saved + "table_usager.csv", index=False)
    df_lieu.to_csv(path_to_saved + "table_lieu.csv", index=False)
    df_date.to_csv(path_to_saved + "table_date.csv", index=False)
    df_event.to_csv(path_to_saved + "table_event.csv", index=False)
    df_event_fact_table.to_csv(path_to_saved + "table_de_fait_accident.csv", index=False)





