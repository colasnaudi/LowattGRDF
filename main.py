import os
import json
import datetime
from lowatt_grdf.api import API
from bemserver_api_client import BEMServerApiClient
from bemserver_api_client.enums import DataFormat

GRDF_CLIENT_ID = os.getenv("GRDF_CLIENT_ID")
GRDF_CLIENT_SECRET = os.getenv("GRDF_CLIENT_SECRET")
PCE = os.getenv("PCE")
BEMSERVER_EMAIL = os.getenv("BEMSERVER_EMAIL")
BEMSERVER_SECRET = os.getenv("BEMSERVER_SECRET")

CAMPAIGN_CONTEXT_ID = 3

grdf = API(GRDF_CLIENT_ID, GRDF_CLIENT_SECRET)

client = BEMServerApiClient("api.staging.bemserver.org", True, BEMServerApiClient.make_http_basic_auth(BEMSERVER_EMAIL, BEMSERVER_SECRET))

def upload_timeseries(client, campaign_ctxt_id, pce_client, date_debut, date_fin):
    # ---------------------------------------- GET THE DATA ---------------------------------------- #
    raw_data = grdf.donnees_consos_informatives(pce_client, date_debut, date_fin)

    # ---------------------------------------- FORMAT THE DATA ---------------------------------------- #
    timeseries_data_raw = {
        "ConsoGaz": {},  # Nom de la timeserie
    }

    timeseries_data_clean = {
        "ConsoGaz": {},  # Nom de la timeserie
    }

    for data in raw_data:
        consommation = data.get("consommation", {})
        releve_debut = data.get("releve_debut", {})

        index_brut_debut = releve_debut.get("index_brut_debut", {}).get("valeur_index", 0)  # Index brut de début
        # Autres données
        # index_converti_debut = releve_debut.get("index_converti_debut", {}).get("valeur_index", 0) # Index converti de début
        # energy = consommation.get("energie", 0) # Energy en kWh
        # volume_brut = consommation.get("volume_brut", 0) # Volume brut en m3
        # volume_converti = consommation.get("volume_converti", 0) # Volume converti en Nm3

        start_date = consommation.get("date_debut_consommation")

        status_conso = consommation.get("statut_conso")

        #if status_conso == "Provisoire":
        timeseries_data_raw["ConsoGaz"][start_date] = index_brut_debut
        # else:
        #     timeseries_data_clean["ConsoGaz"][start_date] = index_brut_debut

    if timeseries_data_raw["ConsoGaz"] != "{}":
        timeseries_data_raw = json.dumps(timeseries_data_raw)
    else:
        timeseries_data_raw = None
    
    # if timeseries_data_clean["ConsoGaz"] != "{}":
    #     timeseries_data_clean = json.dumps(timeseries_data_clean)
    # else:
    #     timeseries_data_clean = None

    # ---------------------------------------- UPLOAD THE DATA ---------------------------------------- #
    data_format = DataFormat.json

    if timeseries_data_raw != None:
        response = client.timeseries_data.upload_by_names(
            campaign_ctxt_id,
            1,
            timeseries_data_raw,
            data_format
        )
        print(response)
        
    # if timeseries_data_clean != None:
    #     response = client.timeseries_data.upload_by_names(
    #         id_timeserie,
    #         2,
    #         timeseries_data_clean,
    #         data_format
    #     )
    #     print(response)


# ---------------------------------------- MAIN ---------------------------------------- #
if __name__ == "__main__":
    # Get the data from the last 15 days
    today = datetime.date.today()
    date_debut = today - datetime.timedelta(days=15)
    date_fin = today
    upload_timeseries(client, CAMPAIGN_CONTEXT_ID, PCE, date_debut, date_fin)
