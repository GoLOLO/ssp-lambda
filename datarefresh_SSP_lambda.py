import pandas as pd
import os
import traceback
from sqlalchemy import create_engine
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

# Step 1: Establish Redshift Connection using SQLAlchemy
def get_data_from_redshift():
    # SQLAlchemy connection string
    password = os.environ.get('MY_DB_PASSWORD')
    connection_string = f'postgresql+psycopg2://admin:{password}@cdphe-air-poc.cdxmbynvb2pn.us-east-1.redshift.amazonaws.com:5439/dev'
    
    # Create an engine
    engine = create_engine(connection_string)

    # Step 2: Fetch Data
    query = "SELECT * FROM dbo.vw_stationary_sources_air_pollution_full;"
    df = pd.read_sql(query, engine)

    return df

# Step 3: ArcGIS Online Authentication
def overwrite_feature_layer(df):
    # Retrieve passwords from environment variables
    arcgis_password = os.environ.get('MY_ARCGIS_PASSWORD')
    # Directory check and creation if not exists
    directory_path = "D:\\Users\\awsoit-loren.speer\\Documents\\MyData"
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Saving the DataFrame to CSV in Lambda's temporary storage
    file_path = "/tmp/vw_stationary_sources_air_pollution_full.csv"
    df.to_csv(file_path, index=False)

    gis = GIS("https://www.arcgis.com", "APCD_ArcGIS_Viewer", arcgis_password)
    
    # Fetch your existing feature layer
    item = gis.content.get('8b41022cfdda4a1e94bf3846e2a74737')
    flayer_collection = FeatureLayerCollection.fromitem(item)

    try:
        # Step 4: Update Feature Layer
        flayer_collection.manager.overwrite(file_path)  # Using the path of the CSV we just saved
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    data = get_data_from_redshift()
    overwrite_feature_layer(data)


