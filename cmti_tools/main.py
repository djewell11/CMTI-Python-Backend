import os
import argparse
import warnings
from configparser import ConfigParser
from pathlib import Path

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from cmti_tools.importdata import source_importers
from cmti_tools.datamappers import *
from cmti_tools.idmanager import ID_Manager
# from cmti_tools import shift_values

BASE_DIR = Path(__file__).resolve().parent

def build_cmti():
  
  parser = argparse.ArgumentParser(description="Import sources, map to CMTI metadata and apply quality control. To change supplemental data tables, edit the config.toml file.")
  parser.add_argument("--cmti_worksheet", help="Path to the CMTI worksheet", type=str) # cmti_worksheet is 'optional' because it has a default to fall back to. It has to be included either here or in config
  parser.add_argument("--omi", help="Path to the Ontario Mineral Inventory table", type=str)
  parser.add_argument("--oam", help="Path to the Orphaned and Abanoned Mines table", type=str)
  parser.add_argument("--bcahm", help="Path to the British Columbia Abandoned and Historic Mines table", type=str)
  parser.add_argument("--nsmtd", help="Path to the Nova Scotia Mine Tailings Database table", type=str)
  parser.add_argument("--create_ids", help="Generate CMTI IDs for records if missing", action="store_true")
  parser.add_argument("--out", help="Path to the output file", type=str, required=True)
  parser.add_argument("--config", help="Path to the config file", type=str, default=BASE_DIR / "config.toml")

  args = parser.parse_args() 
  
  config = ConfigParser()
  config.read(args.config)

  def create_path(source_path: str) -> Path:
    if source_path is None:
      return
    normalized = source_path.replace("\\", "/")
    out_path = (BASE_DIR / normalized).expanduser().resolve()
    return str(out_path)

  # Data Table Paths
  cm_list_path = create_path(config.get('supplemental', 'critical_minerals'))
  cm_list = pd.read_csv(cm_list_path)['Critical Minerals List'].tolist()
  metals_path = create_path(config.get('supplemental', 'metals'))
  metals = pd.read_csv(metals_path)
  metals_dict = dict(zip(metals['Commodity'], metals['Type']))
  elements_path = create_path(config.get('supplemental', 'elements'))
  elements = pd.read_csv(elements_path)
  name_convert_dict = dict(zip(elements['symbol'], elements['name']))
  oam_comm_path = create_path(config.get('supplemental', 'oam_comm_names'))
  oam_comm_df = pd.read_csv(oam_comm_path)
  oam_comm_names = dict(zip(oam_comm_df['Symbol'], oam_comm_df['Full_Name']))

  # Import sources, map to worksheet and append

  # Source Paths

  cmti_path = create_path(args.cmti_worksheet)
  if cmti_path is None:
    raise ValueError("Valid CMTI worksheet path is required to provide column names. Table rows can be empty.")
  else:
    cmti_path = cmti_path
  omi_path = create_path(args.omi)
  oam_path = create_path(args.oam)
  bcahm_path = create_path(args.bcahm)
  nsmtd_path = create_path(args.nsmtd)
  out = create_path(args.out)

  # Check output path
  if not Path(out).parent.exists():
    raise ValueError(f"Output directory does not exist: {Path(out).parent} or cannot be found.")
  if not out.endswith('.csv'):
    print("Output file should be a .csv. Adding '.csv' to the end of the file name.")
    out = out + ".csv"

  # CMTI Worksheet

  worksheet_importer = source_importers.WorksheetImporter(
      cm_list=cm_list,
      name_convert_dict=name_convert_dict,
      metals_dict=metals_dict
    )

  cmti_df = worksheet_importer.clean_input_table(cmti_path, calculate_UTM=True, force_dtypes=True, convert_units=False)
  
  print("Worksheet imported.")

  # Clean up records

  # If multiple years are present for construction year, take the first one
  for year in cmti_df['Construction_Year'].items():
    if isinstance(year[1], str):
      try:
        years = [int(year) for year in year[1].split(',')]
        year_start = min(years)
        cmti_df.at[year[0], 'Construction_Year'] = year_start
      except ValueError:
        pass

  # OMI
  if omi_path is not None:
    omi_importer = source_importers.OMIImporter(
        cm_list=cm_list,
        name_convert_dict=name_convert_dict,
        metals_dict=metals_dict
    )

    omi_df = omi_importer.clean_input_table(omi_path)

    omi_mapped = omi_importer.map_to_worksheet(cmti_df, omi_df, omi_mapping)
    omi_mapped['Province_Territory'] = 'BC'
    omi_mapped['Site_Type'] = 'Mine'
    omi_mapped['Source_1'] = 'OMI'

    cmti_df = pd.concat([cmti_df, omi_mapped])
    print("OMI imported.")

  #OAM
  if oam_path is not None:
    # OAM commodities need special treatment
    oam_importer = source_importers.OAMImporter(
        cm_list=cm_list,
        name_convert_dict=name_convert_dict,
        metals_dict=metals_dict,
        oam_comm_names=oam_comm_names
    )

    oam_df = oam_importer.clean_input_table(oam_path)
    oam_mapped = oam_importer.map_to_worksheet(cmti_df, oam_df, oam_mapping)
    oam_mapped['Site_Type'] = 'Mine'
    oam_mapped['Source_1'] = 'OAM'

    cmti_df = pd.concat([cmti_df, oam_mapped])
    print("OAM imported.")

  # BC AHM
  if bcahm_path is not None:
    bcahm_importer = source_importers.BCAHMImporter(
        cm_list=cm_list,
        name_convert_dict=name_convert_dict,
        metals_dict=metals_dict
    )

    bcahm_df = bcahm_importer.clean_input_table(bcahm_path)

    bcahm_df = bcahm_df.fillna({'DEPOSITTYPE_D1': 'Null', 'DEPOSITTYPE_D2': 'Null'})
    bcahm_mapped = bcahm_importer.map_to_worksheet(cmti_df, bcahm_df, bcahm_mapping)
    bcahm_mapped['Province_Territory'] = "BC"
    bcahm_mapped['Site_Type'] = 'Mine'
    bcahm_mapped['Source_1'] = 'BC AHM'

    cmti_df = pd.concat([cmti_df, bcahm_mapped])
    print("BC AHM imported.")

  # NSMTD
  if nsmtd_path is not None:
    # nsmtd_df = pd.read_csv(nsmtd_path)
    # nsmtd_df = nsmtd_df[['OBJECTID', 'Name', 'Latitude', 'Longitude', 'Tonnes', 'Commodity', 'Dates', 'InfoSource', 'AreaHa']]
    nsmtd_importer = source_importers.NSMTDImporter(
        cm_list=cm_list,
        name_convert_dict=name_convert_dict,
        metals_dict=metals_dict
    )

    nsmtd_df = nsmtd_importer.clean_input_table(nsmtd_path)
    nsmtd_mapped = nsmtd_importer.map_to_worksheet(cmti_df, nsmtd_df, nsmtd_mapping)

    # Years are in one column and sometimes have bad formatting
    for value in nsmtd_mapped['Year_Opened'].items():
      if pd.notna(value):
        # Some entries have multiple intervals
        try:
          if isinstance(value[1], str):
            years = []
            year_entry = value[1].replace('.', '')
            intervals = year_entry.split(',')
            for interval in intervals:
              for year in interval.split('-'):
                if len(year) == 4:
                  years.append(int(year))
            year_start = min(years)
            year_end = max(years)
            nsmtd_mapped.at[value[0], 'Year_Opened'] = year_start
            nsmtd_mapped.at[value[0], 'Year_Closed'] = year_end
          else:
            nsmtd_mapped.at[value[0], 'Year_Opened'] = value[1]
        except ValueError:
          pass

    nsmtd_mapped['Province_Territory'] = "NS"
    nsmtd_mapped['Site_Type'] = 'Mine'
    nsmtd_mapped['Source_1'] = 'NSMTD'
    nsmtd_mapped['Mine_Status'] = "Inactive"

    cmti_df = pd.concat([cmti_df, nsmtd_mapped])
    print("NSMTD imported.")

  # There are currently some extra columns. Remove them
  # cmti_df = cmti_df.drop(columns=['Tailings_Mass', 'Tailings_Image_Notes'])

  # Perform some row-wise QA

  # source_cols = [col for col in cmti_df.columns.tolist() if col.startswith("Source")]

  # Fill in IDs
  if args.create_ids:
    id_manager = ID_Manager()
    id_vals = cmti_df['CMTI_ID'].dropna()
    id_manager.update_prov_ids(id_vals)
    cmti_df.reset_index(drop=True, inplace=True)

  for i, row in cmti_df.iterrows():
    
    # # Shift source columns over
    # sources_shifted = shift_values(row, source_cols)
    # for s_col, val in sources_shifted.items():
    #   cmti_df.at[i, s_col] = val

    # Fill in IDs
    if args.create_ids and pd.isna(row.CMTI_ID) and pd.notna(row.Province_Territory):
      pt = row.Province_Territory
      prov_id = getattr(id_manager, pt)
      prov_id.generate_id()
      cmti_df.at[i, 'CMTI_ID'] = prov_id.formatted_id

  cmti_df.to_csv(out, index=False)
  print(f"Output written to {out}")

if __name__ == "__main__":
  build_cmti()