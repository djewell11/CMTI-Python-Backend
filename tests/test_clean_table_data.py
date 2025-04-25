import pandas as pd
from pytest import approx
from cmti_tools.importdata.source_importers import *
from cmti_tools import create_module_variables

module_variables = create_module_variables()
name_dict = module_variables.get('name_convert_dict')
metals_dict = module_variables.get('metals_dict')
cm_list = module_variables.get('cm_list')

# Test worksheet/CMTI
def test_clean_table_data_worksheet():
    cmti_df = pd.DataFrame(data={
        'Site_Name': 'Springhill',
        'Site_Type': 'Mine',
        'Site_Aliases': 'Spring Hill, Lime Mine',
        'CMIM_ID': 'NB000084',
        'Last_Revised': '2024/01/01',
        'Latitude': 46.,
        'Longitude': -65.37,
        'Easting': 12345,
        'Northing': 54321,
        'NAD': '83',
        'UTM_Zone': None,
        'NTS_Area': '15K01',
        'Province_Territory': 'NB',
        'Mining_District': 'Havelock',
        'Commodity1': 'Limestone',
        'Commodity2': None,
        'Commodity3': None,
        'Commodity4': None,
        'Commodity5': None,
        'Commodity6': None,
        'Commodity7': None,
        'Commodity8': None,
        'Mine_Type': 'Open Pit',
        'Mine_Status': 'Active',
        'Mining_Method': '',
        'Owner_Operator': 'Mine Co.',
        'Site_Access': 'Road',
        'Past_Owners': 'Quarry Inc, Limestone Company',
        'Dev_Stage': 'Production',
        'Hazard_Class': 'Low',
        'Source_1': 'Personal Communication',
        'Source_1_Link': 'some website dot com',
        'Source_1_ID': '00001',
        'Source_2': None,
        'Source_3': None,
        'Source_4': None,
        'Orebody_Type': 'Sedimentary',
        'Construction_Year': 1901,
        'Year_Opened': 1902,
        'Year_Closed': 2002,
        'Tailings_Area': '25000 m2',
        'Tailings_Volume': 2500,
        'Tailings_Capacity': 4000,
        'Tailings_Storage_Method': 'Dry Stack',
        'Current_Max_Height': 750
    },
    index=[0]
    )
    worksheet_importer = WorksheetImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    cleaned_data = worksheet_importer.clean_input_table(cmti_df)

    assert cleaned_data.dtypes['NAD'] == 'Int64'
    assert cleaned_data.dtypes['Construction_Year'] == 'Int64'
    assert cleaned_data.loc[0, 'Tailings_Area'] == approx(0.025, 0.1)
    assert cleaned_data.loc[0, 'UTM_Zone'] == 20

def test_clean_table_omi():
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

    omi_importer = OMIImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    cleaned_data = omi_importer.clean_input_table(omi_df)

    assert cleaned_data.dtypes['STATUS'] == 'object'
    assert cleaned_data.dtypes['LONGITUDE'] == 'float64'

def test_clean_table_oam():

    oam_comm_data = pd.read_csv(r'cmti_tools\data\OAM_commodity_names.csv')
    oam_comm_names = dict(zip(oam_comm_data['Symbol'], oam_comm_data['Full_Name']))

    oam_df = pd.DataFrame(data=
        {
            'OID': 10782,
            'Lat_DD': 54.766,
            'Long_DD': "-102.754",
            'Jurisdiction': 'SK',
            'Juris_ID': 'SK000001',
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
        }
    , index=[0])

    oam_importer = OAMImporter(oam_comm_names=oam_comm_names, cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    clean_data = oam_importer.clean_input_table(oam_df)

    assert clean_data['Lat_DD'][0] == 54.766
    assert clean_data['Long_DD'][0] == -102.754
    assert clean_data.dtypes['Last_Year'] == 'Int64'
    assert clean_data.dtypes['Mined_Quantity'] == 'float'

def test_clean_table_bcahm():

    bcahm_df = pd.DataFrame(data={
        "OBJECTID": 1,
        "MINFILNO": "BCABC0001",
        "NAME1": "BC Metals",
        "NAME2": "BC Ore",
        "STATUS": "Active",
        "LATITUDE": 47.1234,
        "LONGITUDE": -123.1234,
        "UTM_ZONE": pd.NA,
        "UTM_NORT": 1234567,
        "UTM_EAST": 7654321,
        "ELEV": 45.5,
        "COMMOD_C1": "Gold",
        "COMMOD_C2": "Silver",
        "COMMOD_C3": "Nickel",
        "DEPOSITTYPE_D1": "Surficial Placers",
        "DEPOSITTYPE_D2": "Unkown",
        "DEPOSITCLASS_D1": "Placer",
        "DEPOSITCLASS_D2": "Unknown",
        "NTSMAP_C1": "085NEW",
        "NTSMAP_C2": "086NEW",
        "Permit1": "M123",
        "Permit2": "Null",
        "Mine_Name": "BC Metals",
        "Mine_Statu": "Active",
        "Region": "BC",
        "Tailings": "yes",
        "Disposal_Method": "Dry Stack",
        "Mined": 2462,
        "Milled": 726,
        "Mine_type": "Placer",
        "Permitee1": "BC Metal Inc",
        "Permittee2": "Null",
        "URL": "www dot bcmetals dot com",
        "Current_st": "Site Not Visited",
        "Permit1_Status": "Active",
        "Permit2_Status": "Null",
        "First_Year": "1901",
        "Last_Year": "2001"
    }, index=[0])
    
    bcahm_importer = BCAHMImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    clean_data = bcahm_importer.clean_input_table(bcahm_df)
    assert clean_data.dtypes['First_Year'] == 'Int64'
    assert clean_data.dtypes['UTM_NORT'] == 'Int64'


def test_clean_table_nsmtd():
    nsmtd_df = pd.DataFrame(data={
        'Name': 'Forest Hill (McConnell)',
        'Latitude': 5018046.700,
        'Longitude': 598196.893,
        'Tonnes': 6038.0,
        'Commodity': 'Gold',
        'Dates': '1896-1916',
        'AreaHa': 0.996788
    }, index=[0])

    nsmtd_importer = NSMTDImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    clean_data = nsmtd_importer.clean_input_table(nsmtd_df)

    assert clean_data.dtypes['Tonnes'] == 'Int64'