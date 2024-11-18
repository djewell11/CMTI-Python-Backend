# import pandas as pd
# from cmti_tools.importdata import ingest_omi
# from cmti_tools.tables import Mine

# def test_ingest_omi(session):
#     data = {'MD_ID': [1], 'AMIS_ID': [1], 'Name': ['Test Mine'], 'Latitude': [50.0], 'Longitude': [-128.0]}
#     df = pd.DataFrame(data)
#     production_df = pd.DataFrame({'MD_ID': [1], 'ID': [100]})
#     commodity_df = pd.DataFrame({'PRODUCTION_ID': [100], 'COMMODITY_CODE': ['Gold']})

#     ingest_omi(df, production_df, commodity_df)

#     assert session.query(Mine).filter_by(name="Test Mine").first() is not None