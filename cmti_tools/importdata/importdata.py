import os
import pandas as pd
from configparser import ConfigParser
from configparser import Error as ConfigError
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeBase # Imported for typehints
from sqlalchemy.exc import IntegrityError
from abc import ABC, abstractmethod

from cmti_tools import get_digits
from cmti_tools import get_table_values
from cmti_tools import convert_commodity_name
from cmti_tools import get_commodity
from cmti_tools import lon_to_utm_zone
from cmti_tools import create_name_dict
from cmti_tools.tables import *
from cmti_tools.idmanager import ProvID
from cmti_tools.idmanager import ID_Manager

# Bulk import functions

# Abstract Classes implementation

class DataImporter(ABC):
  """
  An abstract base class for importing data sources.
  """
  def __init__(self, name_convert_dict:str|dict|None=None, cm_list:str|dict|None=None, metals_dict:str|dict|None=None):
    self.id_manager = ID_Manager()

    # Use ConfigParser to get data files if not provided
    config = ConfigParser()
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config.toml"))
    config.read(cfg_path)

    if name_convert_dict == 'config':
      with open(config.get('sources', 'elements'), mode='r') as elements_file:
        self.name_convert_dict = create_name_dict(elements_file)
    elif name_convert_dict is not None:
        self.name_convert_dict = name_convert_dict
    
    if cm_list == 'config':
      with open(config.get('sources', 'critical_minerals'), mode='r') as critical_minerals_file:
        critical_minerals = pd.read_csv(critical_minerals_file, header=0)
        self.cm_list = critical_minerals
    elif cm_list is not None:
      self.cm_list = cm_list
    
    if metals_dict == 'config':
      with open(config.get('sources', 'metals'), mode='r') as metals_file:
        metals_csv = pd.read_csv(metals_file, header=0, encoding='utf-8')
        self.metals_dict = dict(zip(metals_csv['Commodity'], metals_csv['Type']))
    elif metals_dict is not None:
      self.metals_dict = metals_dict

  @abstractmethod
  def process_row(self, row: pd.Series) -> list[object]:
    """
    Process a single row and generates a DeclarativeBase objects based on inputs.

    """
    pass

  def generate_records(self, dataframe:pd.DataFrame) -> list[object]:
    session_records = []
    for _, row in dataframe.iterrows():
      row_records = self.process_row(row)
      session_records = session_records + row_records
    return session_records

  def ingest_records(self, record_list:list[object], session:Session) -> None:
      session.add_all(record_list)
      session.commit()

  def commit_object(self, obj, session:Session):
    """
    Commit an object to session. Deprecated: each DataImporter child class has its own commit method. Will be removed eventually.

    :param obj: An SQL Alchemy ORM object
    :type obj: sqlalchemy.ORM.DeclarativeBase
    """

    try:
      session.add(obj)
      session.commit()
    except IntegrityError as e:
      print(e)
      session.rollback()

class WorksheetImporter(DataImporter):
  def __init__(self, name_convert_dict):
    super().__init__()
    self.name_convert_dict = name_convert_dict
    
  def process_row(self, row: pd.Series, name_convert_dict: dict, elements_file, 
                  auto_generate_cmdb_ids = False):
    
    self.row_records = []
    site_type = row['Site_Type']
    if site_type == "Mine":
      self.process_mine(row)
    elif site_type == "TSF":
      self.process_tsf(row)
    elif site_type == "Impoundment":
      self.process_impoundment(row)
    return self.row_records

  def process_mine(self, row: pd.Series, auto_generate_cmdb_ids):
    try:  
      if auto_generate_cmdb_ids:
        self.id_manager = ID_Manager()

      mine_vals = get_table_values(row, {
        "CMIM_ID": "cmdb_id",
        "Site_Name": "name",
        "Province_Territory": "prov_terr",
        "Last_Revised": "last_revised",
        "NAD": "nad",
        "UTM_Zone": "utm_zone",
        "Easting": "easting",
        "Northing": "northing",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "NTS_Area": "nts_area",
        "Mining_District": "mining_district",
        "Mine_Type": "mine_type",
        "Mine_Status": "mine_status",
        "Mining_Method": "mining_method",
        "Dev_Stage": "development_stage",
        "Site_Access": "site_access",
        # "Construction_Year": "construction_year"
      })

      if pd.isna(row.CMIM_ID) and self.auto_generate_cmdb_ids:
        prov_id = getattr(self.id_manager, mine_vals['prov_terr'])
        mine_vals['cmdb_id'] = prov_id.formatted_id
        prov_id.update_id()

      mine = Mine(**mine_vals)
      self.row_records.append(mine)

      # Mine alias (alternative names)
      # There are often multiple comma-separated aliases. Split them up
      aliases = row['Site_Aliases']
      if pd.notna(aliases):
        # Check if more than one
        aliasesList = [alias.strip() for alias in aliases.split(",")]
        for aliasName in aliasesList:
          alias = Alias(alias=aliasName)
          alias.mine=mine
          self.row_records.append(alias)

      # Commodities
      commodity_cols = list(filter(lambda x: x.startswith("Commodity"), row.index))
      elements = pd.read_csv(self.elements_file)
      name_convert_dict = dict(zip(elements['symbol'], elements['name']))
      for comm in commodity_cols:
        comm_record = get_commodity(row, comm, mine, name_convert_dict=name_convert_dict)
        self.row_records.append(comm_record)

      # Owners
      ownerVals = get_table_values(row, {"Owner_Operator": "name"})
      owner = Owner(**ownerVals)
      if pd.notna(owner.name):
        owner.mine = mine
        mine.owners.append(owner)
        self.row_records.append(owner)

      #References and links
      source_quantity = 4 # Number of source columns
      source_cols = [f"Source_{n+1}" for n in range(source_quantity)]
      for col in source_cols:
        source = row[col]
        if pd.notna(source):
          source_id = row[f"{col}_ID"]
          link = row[f"{col}_Link"]
          reference = Reference(mine=mine, source=source, source_id=source_id, link=link)
          self.row_records.append(reference)

      # Default tailings facility. Every mine gets one
      defaultTSFVals = get_table_values(row, {
          "Mine_Status": "status",
          "Hazard_Class": "hazard_class"
      })
      defaultTSFVals["name"] = f"defaultTSF_{mine.name}".strip()
      defaultTSFVals["default"] = True
      defaultTSF = TailingsFacility(**defaultTSFVals)
      mine.tailings_facilities.append(defaultTSF)
      self.row_records.append(defaultTSF)

      # Default impoundment. Every default tailings facility gets one
      defaultImpoundmentVals = get_table_values(row, {
        "Tailings_Area": "area",
        "Tailings_Volume": "volume",
        "Tailings_Capacity": "capacity",
        "Tailings_Storage_Method": "storage_method",
        "Current_Max_Height": "max_height",
        "Acid_Generating" : "acid_generating",
        "Treatment": "treatment"
        # "Rating_Index": "rating_index",
        # "History_Stability_Concerns": "stability_concerns"
      })
      defaultImpoundmentVals['default'] = True
      # QA for quantified columns
      for key in ["area", "volume", "capacity", "max_height"]:
        val = defaultImpoundmentVals.get(key)
        if isinstance(val, str):
          try:
            digits = get_digits(defaultImpoundmentVals[key])
            defaultImpoundmentVals[key] = digits
          except ValueError:
            # If value can't be converted (i.e., doesn't contain digits), remove it. Property in row_ will be blank
            del(defaultImpoundmentVals[key])
      defaultImpoundmentVals["name"] = f"{defaultTSF.name}_impoundment"
      defaultImpoundment = Impoundment(parentTsf=defaultTSF, **defaultImpoundmentVals)
      self.row_records.append(defaultImpoundment)
    except Exception as e:
      print(e)
  
  # def process_tsf(self, row: pd.Series):
  #   try:  
  #     tsfVals = get_table_values(row, {
  #         "Site_Name": "name",
  #         "CMIM_ID": 'cmdb_id',
  #         "Mine_Status": "status",
  #         "Hazard_Class": "hazard_class",
  #         "Latitude": "latitude",
  #         "Longitude": "longitude"
  #     })
      
  #     tsf = TailingsFacility(**tsfVals)
      
  #     # Get parent mines. It's possible to have more than one
  #     parentID = str(row['Parent_ID']).strip(",")
  #     for id in parentID:
  #       mine = session.query(Mine).filter(Mine.cmdb_id == id).first()
  #       if pd.notna(mine):
  #         mine.tailings_facilities.append(tsf)
      
  #     self.row_records.append(tsf)
  #   except Exception as e:
  #     print(e)

  # def process_impoundment(self, row: pd.Series):
  #   try:  
  #     parentID = row['Parent_ID']
  #     # parentTsf = session.query(TailingsFacility).filter(TailingsFacility.cmdb_id == parentID).first()
  #     if pd.notna(parentTsf):
  #       impoundmentVals = get_table_values(row, {
  #         "Site_Name": "name",
  #         "Tailings_Area": "area",
  #         "Tailings_Volume": "volume",
  #         "Tailings_Capacity": "capacity",
  #         "Tailings_Storage_Method": "storage_method",
  #         "Current_Max_Height": "max_height",
  #         "Acid_Generating" : "acid_generating",
  #         "Treatment": "treatment",
  #         "Rating_Index": "rating_index",
  #         "History_Stability_Concerns": "stability_concerns"
  #       })
  #       impoundmentVals['default'] = False
  #       # QA for quantified columns
  #       for key in ["area", "volume", "capacity", "max_height"]:
  #         val = impoundmentVals.get(key)
  #         if isinstance(val, str):
  #           try:
  #             digits = get_digits(impoundmentVals[key])
  #             impoundmentVals[key] = digits
  #           except ValueError:
  #             # If value can't be converted (i.e., doesn't contain digits), remove it. Property in row_ will be blank
  #             del(impoundmentVals[key])

  #       impoundment = Impoundment(parentTsf=parentTsf, **impoundmentVals)
  #       self.row_records.append(impoundment)
    except Exception as e:
      print(e)

class OMIImporter(DataImporter):
  def __init__(self):
    super().__init__(name_convert_dict = 'config')
    self.prov_id = ProvID("ON")
  
  def process_row(self, row: pd.Series, name_convert_dict: dict=None) -> list[object]:
    
    # name_convert_dict = self.name_convert_dict
    # name_convert_dict = self.name_convert_dict if None else name_convert_dict # Use class name_convert_dict if provided, or override by providing new value
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict
    row_records = []
    try:
      row_id = self.prov_id.formatted_id
      self.prov_id.update_id()
      mine = Mine(
        cmdb_id = row_id,
        name = row['NAME'],
        latitude = row['LATITUDE'],
        longitude = row['LONGITUDE'],
        prov_terr="ON",
        mining_district = row['RGP_DIST']
      )
      row_records.append(mine)

      # Aliases
      aliases = [name.strip() for name in row['ALL_NAMES'].split(",")]
      for alias_val in aliases:
        alias = Alias(mine=mine, alias=alias_val)
        row_records.append(alias)
      
      # Commodities
      primary_commodities = row['P_COMMOD']
      secondary_commodities = row['S_COMMOD']
      commodities = primary_commodities + secondary_commodities
      for comm in commodities:
        comm_converted = convert_commodity_name(comm, name_convert_dict)
        comm_record = CommodityRecord(mine=mine, commodity=comm_converted, source='OMI', source_id=row['MDI_IDENT'])
        # TODO: Incorporate is_critical and metal_type
        row_records.append(comm_record)

      # Default TSF
      tsf = TailingsFacility(default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      # Default Impoundment
      impoundment = Impoundment(parentTsf = tsf, default = True, name = f"{tsf.name}_impoundment")
      row_records.append(impoundment)

      omi_reference = Reference(mine=mine, source = "OMI", source_id = row["MDI_IDENT"], link=row['DETAIL'])
      row_records.append(omi_reference)

      return row_records
    except Exception as e:
      print(e)

class OAMImporter(DataImporter):
  def __init__(self):
    super().__init__()

  def check_year(self, val):
    if isinstance(val, str):
      return get_digits(val)
    elif pd.isna(val):
      return None
    else:
      return val

  def process_row(self, row: pd.Series, oam_comm_names: dict, cm_list: list, metals_dict: dict, convert_dict: dict):
    row_records = []
    try:
      provID = getattr(self.id_manager, row["Jurisdiction"])
      provID.update_id()
      cmdb_id = provID.formatted_id

      mine = Mine(
        cmdb_id = cmdb_id,
        name = row["Name"].title(),
        latitude = row["Lat_DD"],
        longitude = row["Long_DD"],
        prov_terr = row["Jurisdiction"],
        mine_status = row["Status"],
        mine_type = row["Mine_Type"],
        construction_year = row["Start_Date"]
      )
      row_records.append(mine)

      comm_code = row['Commodity_Code']
      comm_full = row['Commodity_Full_Name'] # Records have either code or full name. Check both.
      comm_name = comm_code if pd.notna(comm_code) else comm_full # This assumes that no row_ have both.
      if pd.notna(comm_name):
        try:
          # Sometimes multiple listed in code, split it up and add one entry for each
          commodities = [comm.strip() for comm in comm_name.split(",")]
          for comm in commodities:
            # Convert to full name using OAM name values, then to element names
            comm_full_oam = convert_commodity_name(comm, oam_comm_names, output_type='full', show_warning=False)
            comm_name = convert_commodity_name(comm_full_oam, convert_dict, output_type='symbol', show_warning=False)
            start_year = self.check_year(row['Start_Date'])
            end_year = self.check_year(row['Last_Year'])
            produced = row["Mined_Quantity"] if pd.notna(row["Mined_Quantity"]) else None
            commodity_record = CommodityRecord(
              mine=mine,
              commodity=comm_name,
              produced=produced,
              source_year_start=start_year,
              source_year_end=end_year
            )
            commodity_record.is_critical = True if comm_name in cm_list else False
            commodity_record.is_metal = metals_dict.get(comm_name)

            row_records.append(commodity_record)
        except Exception as e:
          print(e)

      tsf = TailingsFacility(default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      impoundment = Impoundment(parentTsf = tsf, default = True, name = f"{tsf.name}_impoundment")
      row_records.append(impoundment)

      owner = Owner(name = row["Last_Operator"])
      owner.mines.append(mine)
      row_records.append(owner)

      oam_reference = Reference(mine = mine, source = "OAM", source_id = row["OID"], link = row["URL"])
      row_records.append(oam_reference)

      return row_records
    except Exception as e:
      print(e)

class BCAHMImporter(DataImporter):
  def __init__(self):
    super().__init__()
    self.provID = ProvID('BC')

  def process_row(self, row: pd.Series):
    row_records = []
    try:
      bcahm_id = self.provID
      mine_vals = {
        "name": row["NAME1"],
        "latitude": row["LATITUDE"],
        "longitude": row["LONGITUDE"],
        "utm_zone": row["UTM_ZONE"],
        "northing": row["UTM_NORT"],
        "easting": row["UTM_EAST"],
        "year_opened": row["First_Year"],
        "year_closed": row["Last_Year"],
        "nts_area": row["NTSMAP_C1"],
        "prov_terr": "BC",
        "mine_status": "Inactive"
      }

      # If either lat or lon are missin, don't add that record
      if (pd.isna(mine_vals["latitude"]) or mine_vals["latitude"] == 'Null') or \
        (pd.isna(mine_vals["longitude"]) or mine_vals["longitude"] == 'Null'):
          return
      
      # Check coordinates for null strings as well
      if mine_vals['northing'] == 'Null' or pd.isna(mine_vals['northing']):
        del(mine_vals['northing'])
      if mine_vals['easting'] == 'Null' or pd.isna(mine_vals['easting']):
        del(mine_vals['easting'])
      if pd.isna(mine_vals['utm_zone']) or mine_vals['utm_zone'] == 'Null':
        mine_vals['utm_zone'] = lon_to_utm_zone(mine_vals['longitude'])
      
      mine = Mine(cmdb_id = bcahm_id.formatted_id, **mine_vals)
      row_records.append(mine)
      bcahm_id.update_id()

      # Create alias if there's another name
      if pd.notna(row["NAME2"]):
        alias = Alias(mine=mine, alias=row["NAME2"])
        row_records.append(alias)
      
      # TSF
      tsf = TailingsFacility(default = True, name = f"default_TSF_{mine_vals['name']}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)
      bcahm_id.update_id()

      # Impoundment
      impoundment = Impoundment(parentTsf=tsf, default=True, name=f"{tsf.name}_impoundment")
      row_records.append(impoundment)
      bcahm_id.update_id()

      #Reference
      reference = Reference(mine = mine, source = "BCAHM", source_id = str(row.OBJECTID))
      row_records.append(reference)
      if row.MINFILNO != "Null":
        minefileref = Reference(mine = mine, source = "BC Minfile", source_id = row.MINFILNO)
        row_records.append(minefileref)

      # Orebody
      orebody = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D1"], ore_class = row["DEPOSITCLASS_D1"])
      row_records.append(orebody)
      if row["DEPOSITTYPE_D2"] != "Null":
        orebody2 = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D2"], ore_class = row["DEPOSITCLASS_D2"])
        row_records.append(orebody2)

      return row_records
    except Exception as e:
      print(e)