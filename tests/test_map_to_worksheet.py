import pandas as pd
import cmti_tools.importdata.source_importers as importers

def test_omi_to_worksheet():
    omi_df = pd.DataFrame(data={
        'MDI_IDENT': 'MDI130M', 
        'NAME': 'Skelin Quarry', 
        'STATUS': 'Inactive', 
        'TWP_AREA': 'TWP 1',
        'P_COMMOD': 'COPPER',
        'S_COMMOD': 'ZINC',
        'ALL_NAMES': 'Speyside Quarry, S-Skelin Quarry',
        'LONGITUDE': -79.961,
        'LATITUDE': 43.590,
        'RGP_DIST': 'Southern Ontario',
        'DEP_CLASS': 'Quarry',
        'LL_DATUM': 'NAD83',
        'DETAIL': 'some website dot com'
    },
    index = [0]
    )

    