from pandas import Series, read_csv
import cmti_tools.importdata.source_importers as importers
from cmti_tools import create_module_variables

module_variables = create_module_variables()
name_dict = module_variables.get('name_convert_dict')
metals_dict = module_variables.get('metals_dict')
cm_list = module_variables.get('cm_list')

# Test the workseet/CMTI
def test_create_row_records_worksheet():
    worksheet_importer = importers.WorksheetImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    row = Series(
    {
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
        'UTM_Zone': 19,
        'NTS_Area': '15K01',
        'Province_Territory': 'NB',
        'Mining_District': 'Havelock',
        'Commodity1': 'Limestone',
        'Commodity1_Produced': 50_000,
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
        'Orebody_Class': 'Suplhide',
        'Ore_Minerals': 'Pyrite, Gold',
        'Ore_Processed': 1000,
        'Ore_Processed_Unit': 'tonnes',
        'Construction_Year': 1901,
        'Year_Opened': 1902,
        'Year_Closed': 2002,
        'Tailings_Area': 500,
        'Tailings_Area_From_Images': 550,
        'Tailings_Area_Notes': 'None',
        'Tailings_Volume': 2500,
        'Tailings_Capacity': 4000,
        'Tailings_Storage_Method': 'Dry Stack',
        'Current_Max_Height': 750,
        'Acid_Generating': 'True',
        'Treatment': 'None',
        'Rating_Index': 'A',
        'History_Stability_Concerns': 'None',
        # Likely to be removed:
        'Raise_Type': 'Upstream',
        'DS_Comments': 'None',
        'SA_Comments': 'None',
        'Shaft_Depth': 100,
        'Reserves_Resources': 1000,
        'SEDAR': None,
        'Notes': 'None',
        'Other_Mineralization': 'None',
        'Forcing_Features': 'None',
        'Feature_References': 'None',
        'NOAMI_Status': 'Active',
        'NOAMI_Site_Class': 'Class 1',
        'Hazard_Class': 'Low',
        'Hazard_System': 'CWS',
        'PRP_Rating': 'A',
        'Rehab_Plan':True ,
        'EWS': 'PLNF',
        'EWS_Rating': 'A',
    })
    row_records = worksheet_importer.create_row_records(row, comm_col_count=1, source_col_count=1)
    assert len(row_records) == 10

# Test the Ontario Mineral Inventory (OMI)
def test_create_row_records_omi():
    omi_importer = importers.OMIImporter(name_convert_dict=name_dict)
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
    row_records = omi_importer.create_row_records(row)
    assert len(row_records) == 8

# Test the Orphaned and Abandoned Mine Inventort (OAM)
oam_comm_data = read_csv(r'cmti_tools\data\OAM_commodity_names.csv')
oam_comm_names = dict(zip(oam_comm_data['Symbol'], oam_comm_data['Full_Name']))

def test_create_row_records_oam():
    oam_importer = importers.OAMImporter(oam_comm_names=oam_comm_names, cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
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
    row_records = oam_importer.create_row_records(row, oam_comm_names=oam_comm_names)
    assert len(row_records) == 8

# Test the BC Abandoned and Historic Mine database (BC AHM)
def test_create_row_records_BCAHM():
    bcahm_importer = importers.BCAHMImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
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
    row_records = bcahm_importer.create_row_records(row)
    assert len(row_records) == 11

def test_create_row_records_NSMTD():
    nsmtd_importer = importers.NSMTDImporter(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_dict)
    row = Series(
        {
            'OBJECTID': 1,
            'Name': 'Gold Mine',
            'Latitude': '45.123',
            'Longitude': '-63.456',
            'Tonnes': 1000,
            'Commodity': 'Au',
            'Crusher1': 450,
            'Crusher2': 600,
            'Dates': '1876-1918',
            'InfoSource': 'www dot some website dot com',
            'AreaHa': 27,
            'Shape_Area': 27000
        }
    )
    row_records = nsmtd_importer.create_row_records(row)
    assert len(row_records) == 6