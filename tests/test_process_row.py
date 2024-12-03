from cmti_tools.importdata import WorksheetImporter, OMIImporter, OAMImporter, BCAHMImporter
from cmti_tools import create_module_variables
from pandas import Series, read_csv

module_variables = create_module_variables()
name_dict = module_variables.get('name_convert_dict')
metals_dict = module_variables.get('metals_dict')
cm_list = module_variables.get('cm_list')

# Test the workseet
def test_process_row_worksheet():
    worksheet_importer = WorksheetImporter()
    row = Series

# Test the Ontario Mineral Inventory (OMI)
def test_process_row_omi():
    omi_importer = OMIImporter()
    row = Series(
        {
            'MDI_IDENT': 'MDI130M', 
            'NAME': 'Skelin Quarry', 
            'Status': 'Inactive', 
            'P_COMMOD': 'COPPER',
            'S_COMMOD': 'ZINC',
            'ALL_NAMES': 'Speyside Quarry, S-Skelin Quarry',
            'LONGITUDE': -79.961,
            'LATITUDE': 43.590,
            'RGP_DIST': 'Southern Ontario',
            'DETAIL': 'some website dot com'
         })
    row_records = omi_importer.process_row(row, name_dict)
    assert len(row_records) == 16

# Test the Orphaned and Abandoned Mine Inventort (OAM)
oam_comm_data = read_csv(r'cmti_tools\data\OAM_commodity_names.csv')
oam_comm_names = dict(zip(oam_comm_data['Symbol'], oam_comm_data['Full_Name']))

def test_process_row_oam():
    oam_importer = OAMImporter()
    row = Series(
        {
            'OID': 10782,
            'Lat_DD': 54.766,
            'Long_DD': -102.754,
            'Jurisdiction': 'SK',
            'Name': 'Western Nuclear',
            'Status': 'Abandoned',
            'Last_Operator': 'Mine Operator Inc.',
            'Commodity_Code': 'CU, ZN, PB',
            'Commodity_Full_Name': None,
            'Mine_Type': 'Mine',
            'Start_Date': 1947,
            'Last_Year': 2024,
            'Mined_Quantity': 150_000,
            'URL': 'somewebsite dot com'
        })
    row_records = oam_importer.process_row(row, oam_comm_names=oam_comm_names, metals_dict=metals_dict, convert_dict=name_dict, cm_list=cm_list)
    assert len(row_records) == 8

# Test the BC Abandoned and Historic Mine database (BC AHM)
def test_process_row_BCAHM():
    bcahm_importer = BCAHMImporter()
    row = Series(
        {
            'OBJECTID': 1,
            'MINFILNO': '082NE001',
            'NAME1': 'MCKINLEY',
            'NAME2': 'MCKINLEY (L.140S)',
            'STATUS': 'Past Producer',
            'LATITUDE': 49.541,
            'LONGITUDE': -118.388,
            'UTM_NORT': 5488505,
            'UTM_EAST': 399620,
            'UTM_ZONE': 11,
            'COMMOD_C1': 'CU',
            'COMMOD_C2': 'AG',
            'COMMOD_C3': 'PB',
            'First_Year': 1949,
            'Last_Year': 1949,
            'NTSMAP_C1': '082E09W',
            'DEPOSITTYPE_D1': 'Cu skarn',
            'DEPOSITCLASS_D1': 'Skarn',
            'DEPOSITTYPE_D2': 'Pb-An-skarn',
            'DEPOSITCLASS_D2': None
        })
    row_records = bcahm_importer.process_row(row)
    assert len(row_records) == 8