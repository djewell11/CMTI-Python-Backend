from cmti_tools.importdata import OMIImporter
from cmti_tools import create_module_variables
from pandas import Series

name_dict = create_module_variables().get('name_convert_dict')

def test_process_data_omi():
    omi_importer = OMIImporter()
    row = Series(
        {'MDI_IDENT': 'MDI130M', 
         'NAME': 'Skelin Quarry', 
         'Status': 'Inactive', 
         'P_COMMOD': 'COPPER',
         'S_COMMOD': 'ZINC',
         'ALL_NAMES': 'Speyside Quarry, S-Skelin Quarry',
         'LONGITUDE': -79.961,
         'LATITUDE': 43.590,
         'RGP_DIST': 'Southern Ontario',
         'DETAIL': 'www.google.com'
         })
    row_records = omi_importer.process_row(row, name_dict)
    assert(len(row_records) == 16)