import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from abc import ABC, abstractmethod

from cmti_tools.tools import get_digits
from cmti_tools import get_table_values
from cmti_tools import convert_commodity_name
from cmti_tools import get_commodity
from cmti_tools import lon_to_utm_zone
from cmti_tools.tables import *
from cmti_tools.idmanager import ProvID
from cmti_tools.idmanager import ID_Manager

# Bulk import functions

# Abstract Classes implementation

class DataImport(ABC):
  def __init__(self, session: Session):
    self.session = session
    self.id_manager = ID_Manager()

  @abstractmethod
  def process_row(self, row: pd.Series):
    """
    Process a single row and add to db
    """
    pass

  def commit_object(self, obj):
    """
    Commit an object to session
    """
    try:
      self.session.add(obj)
      self.session.commit()
    except IntegrityError as e:
      print(e)
      self.session.rollback()
  
  def ingest_dataframe(self, dataframe: pd.DataFrame):
    """
    Iterates through dataframe rows and calls process_row function.

    :param dataframe: OMI, OAM, or BC AHM dataframe
    :type dataframe: pandas.DataFrame
    """
    for _, row in dataframe.iterrows():
      self.process_row(row)
    self.session.close()

class WorksheetImporter(DataImport):
  def __init__(self, session: Session, name_convert_dict: dict, elements_file, auto_generate_cmdb_ids = False):
    super().__init__(session)
    self.name_convert_dict = name_convert_dict
    self.elements_file = elements_file
    self.auto_generate_cmdb_ids = auto_generate_cmdb_ids
    if auto_generate_cmdb_ids:
      self.id_manager = ID_Manager()
    
  def process_row(self, row: pd.Series):
    site_type = row['Site_Type']
    if site_type == "Mine":
      self.process_mine(row)
    elif site_type == "TSF":
      self.process_tsf(row)
    elif site_type == "Impoundment":
      self.process_impoundment(row)

  def process_mine(self, row: pd.Series):
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
    self.commit_object(mine)

    # Mine alias (alternative names)
    # There are often multiple comma-separated aliases. Split them up
    aliases = row['Site_Aliases']
    if pd.notna(aliases):
      # Check if more than one
      aliasesList = [alias.strip() for alias in aliases.split(",")]
      for aliasName in aliasesList:
        alias = Alias(alias=aliasName)
        alias.mine=mine
        self.commit_object(alias)

    # Commodities
    commodity_cols = list(filter(lambda x: x.startswith("Commodity"), row.index))
    elements = pd.read_csv(self.elements_file)
    name_convert_dict = dict(zip(elements['symbol'], elements['name']))
    for comm in commodity_cols:
      comm_record = get_commodity(row, comm, mine, name_convert_dict=name_convert_dict)
      self.commit_object(comm_record)

    # Owners
    ownerVals = get_table_values(row, {"Owner_Operator": "name"})
    owner = Owner(**ownerVals)
    if pd.notna(owner.name):
      owner.mine = mine
      mine.owners.append(owner)
      self.commit_object(owner)

    #References and links
    source_quantity = 4 # Number of source columns
    source_cols = [f"Source_{n+1}" for n in range(source_quantity)]
    for col in source_cols:
      source = row[col]
      if pd.notna(source):
        source_id = row[f"{col}_ID"]
        link = row[f"{col}_Link"]
        reference = Reference(mine=mine, source=source, source_id=source_id, link=link)
        self.commit_object(reference)

    # Default tailings facility. Every mine gets one
    defaultTSFVals = get_table_values(row, {
        "Mine_Status": "status",
        "Hazard_Class": "hazard_class"
    })
    defaultTSFVals["name"] = f"defaultTSF_{mine.name}".strip()
    defaultTSFVals["default"] = True
    defaultTSF = TailingsFacility(**defaultTSFVals)
    mine.tailings_facilities.append(defaultTSF)
    self.commit_object(defaultTSF)

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
          # If value can't be converted (i.e., doesn't contain digits), remove it. Property in table will be blank
          del(defaultImpoundmentVals[key])
    defaultImpoundmentVals["name"] = f"{defaultTSF.name}_impoundment"
    defaultImpoundment = Impoundment(parentTsf=defaultTSF, **defaultImpoundmentVals)
    self.commit_object(defaultImpoundment)
  
  def process_tsf(self, row: pd.Series):
    tsfVals = get_table_values(row, {
        "Site_Name": "name",
        "CMIM_ID": 'cmdb_id',
        "Mine_Status": "status",
        "Hazard_Class": "hazard_class",
        "Latitude": "latitude",
        "Longitude": "longitude"
    })
    
    tsf = TailingsFacility(**tsfVals)
    
    # Get parent mines. It's possible to have more than one
    parentID = str(row['Parent_ID']).strip(",")
    for id in parentID:
      mine = self.session.query(Mine).filter(Mine.cmdb_id == id).first()
      if pd.notna(mine):
        mine.tailings_facilities.append(tsf)
    
    self.commit_object(tsf)

  def process_impoundment(self, row: pd.Series):
    parentID = row['Parent_ID']
    parentTsf = self.session.query(TailingsFacility).filter(TailingsFacility.cmdb_id == parentID).first()
    if pd.notna(parentTsf):
      impoundmentVals = get_table_values(row, {
        "Site_Name": "name",
        "Tailings_Area": "area",
        "Tailings_Volume": "volume",
        "Tailings_Capacity": "capacity",
        "Tailings_Storage_Method": "storage_method",
        "Current_Max_Height": "max_height",
        "Acid_Generating" : "acid_generating",
        "Treatment": "treatment",
        "Rating_Index": "rating_index",
        "History_Stability_Concerns": "stability_concerns"
      })
      impoundmentVals['default'] = False
      # QA for quantified columns
      for key in ["area", "volume", "capacity", "max_height"]:
        val = impoundmentVals.get(key)
        if isinstance(val, str):
          try:
            digits = get_digits(impoundmentVals[key])
            impoundmentVals[key] = digits
          except ValueError:
            # If value can't be converted (i.e., doesn't contain digits), remove it. Property in table will be blank
            del(impoundmentVals[key])

      impoundment = Impoundment(parentTsf=parentTsf, **impoundmentVals)
      self.commit_object(impoundment)

class OMIImporter(DataImport):
  def __init__(self, session: Session):
    super().__init__(session)
    self.prov_id = ProvID("ON")
  
  def process_row(self, row: pd.Series):
    try:
      row_id = self.prov_id.formatted_id
      self.prov_id.update_id()
      mine = Mine(
        cmdb_id = row_id,
        name = row["NAME"],
        latitude = row["LATITUDE"],
        longitude = row["LONGITUDE"],
        prov_terr="ON",
        mining_district = row['RGP_DIST']
      )
      self.commit_object(mine)

      # Aliases
      aliases = [name.strip() for name in row['ALL_NAMES'].split(",")]
      for alias_val in aliases:
        alias = Alias(mine=mine, alias=alias_val)
        self.commit_object(alias)
      
      # Commodities
      primary_commodities = row['P_COMMOD']
      secondary_commodities = row['S_COMMOD']
      commodities = primary_commodities + secondary_commodities
      for comm in commodities:
        comm_converted = convert_commodity_name(comm)
        comm_record = CommodityRecord(mine=mine, commodity=comm_converted, source='OMI', source_id=row['MDI_IDENT'])
        # TODO: Incorporate is_critical and metal_type
        self.commit_object(comm_record)

      # Default TSF
      tsf = TailingsFacility(default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      self.commit_object(tsf)

      # Default Impoundment
      impoundment = Impoundment(parentTsf = tsf, default = True, name = f"{tsf.name}_impoundment")
      self.commit_object(impoundment)

      omi_reference = Reference(mine=mine, source = "OMI", source_id = row["MDI_IDENT"])
      self.commit_object(omi_reference)
    except Exception as e:
      print(e)

class OAMImporter(DataImport):
  def __init__(self, session: Session, oam_comm_names: pd.DataFrame, cm_list: list, metals_dict: dict, convert_dict: dict):
    super().__init__(session)
    self.oam_comm_names = oam_comm_names
    self.cm_list = cm_list
    self.metals_dict = metals_dict
    self.convert_dict = convert_dict

  def check_year(self, val):
    if isinstance(val, str):
      return get_digits(val)
    elif pd.isna(val):
      return None
    else:
      return val

  def process_row(self, row: pd.Series):
    provID = getattr(self.id_manager, row["Jurisdiction"])
    provID.update_id()
    cmdb_id = provID.formatted_id
    self._oam_row_to_cmti(row, cmdb_id)

  def _oam_row_to_cmti(self, row: pd.Series, cmdb_id: str):
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
    self.commit_object(mine)

    comm_code = row['Commodity_Code']
    comm_full = row['Commodity_Full_Name'] # Records have either code or full name. Check both.
    comm_name = comm_code if pd.notna(comm_code) else comm_full # This assumes that no records have both.
    if pd.notna(comm_name):
      try:
        # Sometimes multiple listed in code, split it up and add one entry for each
        commodities = [comm.strip() for comm in comm_name.split(",")]
        for comm in commodities:
          # Convert to full name using OAM name values, then to element names
          comm_full_oam = convert_commodity_name(comm, self.oam_comm_names, output_type='full', show_warning=False)
          comm_name = convert_commodity_name(comm_full_oam, self.convert_dict, output_type='symbol', show_warning=False)
          # print(f"{comm} -- {comm_full_oam} -- {comm_name}")
          start_year = self.check_year(row['Start_Date'])
          end_year = self.check_year(row['Last_Year'])
          produced = row["Mined_Quantity"] if pd.notna(row["Mined_Quantity"]) else None
          commodityRecord = CommodityRecord(
            mine=mine,
            commodity=comm_name,
            produced=produced,
            source_year_start=start_year,
            source_year_end=end_year
          )
          commodityRecord.is_critical = True if comm_name in self.cm_list['Commodity'].tolist() else False
          commodityRecord.is_metal = self.metals_dict.get(comm_name)
      except Exception as e:
        self.session.rollback()
        print(e)

    tsf = TailingsFacility(default = True, name = f"default_TSF_{mine.name}".strip())
    mine.tailings_facilities.append(tsf)
    self.commit_object(tsf)

    impoundment = Impoundment(parentTsf = tsf, default = True, name = f"{tsf.name}_impoundment")
    self.commit_object(impoundment)

    owner = Owner(name = row["Last_Operator"])
    owner.mines.append(mine)
    self.commit_object(owner)

    oam_reference = Reference(mine = mine, source = "OAM", source_id = row["OID"], link = row["URL"])
    self.commit_object(oam_reference)

class BCAHMImporter(DataImport):
  def __init__(self, session: Session):
    super().__init__(session)
  
  def process_row(self, row: pd.Series):
    cmdb_id = self.id_manager.BC.formatted_id
    self._bc_ahm_row_to_cmti(row, cmdb_id)
    
  def _bc_ahm_row_to_cmti(self, row: pd.Series, cmdb_id: str):
    """
    Takes a row of the BC Abandoned and Historic Mine (BC AHM) 
    
    :param row: A row from the bc_ahm DataFrame.
    :type row: pandas.Series

    :param cmdb_id: BC formatted id
    :type cmdb_id: str
    """
    
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
    
    mine = Mine(cmdb_id = cmdb_id, **mine_vals)
    self.commit_object(mine)
    self.id_manager.BC.update_id()

    # Create alias if there's another name
    if pd.notna(row["NAME2"]):
      alias = Alias(mine=mine, alias=row["NAME2"])
      self.commit_object(alias)
    
    # TSF
    tsf = TailingsFacility(default = True, name = f"default_TSF_{mine_vals['name']}".strip())
    mine.tailings_facilities.append(tsf)
    self.commit_object(tsf)
    self.id_manager.BC.update_id()

    # Impoundment
    impoundment = Impoundment(parentTsf=tsf, default=True, name=f"{tsf.name}_impoundment")
    self.commit_object(impoundment)
    self.id_manager.BC.update_id()

    #Reference
    reference = Reference(mine = mine, source = "BCAHM", source_id = str(row.OBJECTID))
    self.commit_object(reference)
    if row.MINEFILNO != "Null":
      minefileref = Reference(mine = mine, source = "BC Minfile", source_id = row. MINFILNO)
      self.commit_object(minefileref)

    # Orebody
    orebody = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D1"], ore_class = row["DEPOSITCLASS_D1"])
    self.commit_object(orebody)
    if row["DEPOSITTYPE_D2"] != "Null":
      orebody2 = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D2"], ore_class = row["DEPOSITCLASS_D2"])
      self.commit_object(orebody2)

# OMI - Ontario Mineral Inventory

def omi_row_to_cmti(row, cmdb_id, production_df, production_comm_df, session, cm_list, metals_dict):
  """
  Takes a row of the Ontario Mineral Inventory (OMI) databse and inserts it into the CMTI database.

  :param row: A row of the omi database from a pandas DataFrame.
  :type row: pandas.Series

  :param cmdb_id: The ID to be applied to the new record. Recommend using in conjunction with ProvIDs to avoid duplicate IDs.
  :type cmdb_id: str

  :param production_df: The production table of the OMI.
  :type production_df: pandas.DataFrame

  :param production_comm_df: The production_commoditiies table of the OMI.
  :type production_comm_df: pandas.DataFrame

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session

  :param cm_list: A list of critical minerals. Default: data_tables['cm_list'].
  :type cm_list: list

  :param metals_dict: A dictionary denoting metal type per commodity. Commodities may be metal, non-metal, or REE. Default: data_tables['metals_dict']
  :type metals_dict: dict
  """

  # Mine object
  mineVals = get_table_values(row, {
  "NAME": "name",
  "LAT_DEC": "latitude",
  "LNG_DEC": "longitude",
  "MINING_METHOD": "mining_method",
  })
  mineVals['prov_terr'] = 'ON'
  mineVals['cmdb_id'] = cmdb_id
  mine = Mine(**mineVals)
  # Commodity record object
  prod_ids = production_df[production_df.MD_ID == row.MD_ID].ID
  for prod_id in prod_ids:
    prod_comms = production_comm_df[production_comm_df.PRODUCTION_ID == prod_id]
    for i, comm_row in prod_comms.iterrows():
      commodityVals = get_table_values(comm_row, {
          "COMMODITY_CODE": "commodity",
          "COMMODITY_MASS": "produced",
          "UNIT_OF_MEASURE": "produced_unit"
      })
      # print(commodityVals['commodity'])
      commodityRecord = CommodityRecord(mine=mine, **commodityVals)
      commodityRecord.is_critical = True if commodityVals['commodity'] in cm_list else False
      commodityRecord.is_metal = metals_dict.get(commodityVals['commodity'])
      session.add(commodityRecord)

  # Tailings facility object
  tsfName = f"default_TSF_{mineVals['name']}".strip()
  tsf = TailingsFacility(**{'default': True, 'name': tsfName})
  mine.tailings_facilities.append(tsf)
  # session.merge(tsf)

  # Impoundment object
  impoundmentName = f"{tsfName}_impoundment"
  # check_exists = session.query(Impoundment).filter_by(name=impoundmentName).first()
  # if check_exists:
  #   pass
  # else:
  impoundment = Impoundment(**{'default': True, 'name': impoundmentName})
  tsf.impoundments.append(impoundment)
    # session.merge(impoundment)

  # Reference object
  # OMI reference
  omi_reference = Reference(mine=mine, source_id=row["MD_ID"], source="OMI")
  # session.merge(omi_reference)
  # AMIS reference, if present
  amis_id = row['AMIS_ID']
  if pd.notna(amis_id):
    amis_reference = Reference(mine=mine, source_id=amis_id, source='AMIS')
  # session.merge(amis_reference)
  session.add(mine)
  session.commit()

def ingest_omi(omi_dataframe, omi_production_df, omi_prod_comm_df, session):
  """
  Appends the Ontario Mineral Inventory (OMI) to the CMTI.

  :param omi_dataframe: The OMI DataFrame.
  :type omi_dataframe: pandas.DataFrame

  :param omi_production_df: A DataFrame of the OMI's production table.
  :type omi_production_df: pandas.DataFrame

  :param omi_prod_comm_df: A DataFrame of the OMI's production_commodity table.
  :type omi_prod_comm_df: pandas.DataFrame

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session
  """


  prov_id = ProvID('ON')
  for i, row in omi_dataframe.iterrows():
    try:
      amis_id = row['AMIS_ID']
      # omi_id = row['MD_ID']
      # OMI records with AMIS IDs that match an existing record in the database
      # IMPORTANT - Currently assuming all IDs are AMIS, as this seems to be how the worksheet was filled.
        #Should revisit after QA (currently all sources listed as 'SPE' but seem to match)
      # If secondary matching strategy (name, coordinates, etc.) is desired, it will go in this function.
      cmdb_ids = []
      if pd.notna(amis_id):
        amis_int = str(amis_id)
        amis_matched = select(Mine).filter(
          Mine.prov_terr == 'ON',
          Mine.references.any(
              (Reference.source == 'Abandoned Mines Information System') | (Reference.source == "AMIS")),
        ).where(
          Mine.references.any(Reference.source_id == amis_int)
        )
        with session.execute(amis_matched).unique().scalars() as q:
          for q_row in q.all():
            # If matching, get existing CMIM ID
            cmdb_ids.append(q_row.cmdb_id)
      cmim_len = len(cmdb_ids)
      if cmim_len == 0:
        # Create new ID
        prov_id.update_id()
        newID = prov_id.formatted_id
        omi_row_to_cmti(row, newID, omi_production_df, omi_prod_comm_df)
      elif cmim_len == 1:
        cmdb_id = cmdb_ids[0]
        omi_row_to_cmti(row, cmdb_id, omi_production_df, omi_prod_comm_df)
      elif cmim_len > 1:
        # Not sure how to handle this yet. Maybe use secondary matching to see if record already exists
        pass
    except Exception as e:
      print(e)
    session.commit()
    session.close()

# OAM - Orphaned and Abandoned Mines

def oam_row_to_cmti(row:pd.Series, cmdb_id:str, oam_comm_names:pd.DataFrame, session, cm_list:list,
                    metals_dict:dict, convert_dict:dict):

  """
  Takes a row of the Orphaned and Abandoned Mines (OAM) databse and inserts it into the CMTI database.

  :param row: A row of the omi database from a pandas dataframe.
  :type row: pandas.Series

  :param cmdb_id: The ID to be applied to the new record. Recommend using in conjunction with ProvIDs to avoid duplicate IDs.
  :type cmdb_id: str

  :param oam_comm_names: A DataFrame containing the commodity names specific to the OAM. The OAM uses its own commodity symbols.
  :type oam_comm_names: pandas.Dataframe

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session

  :param cm_list: A list of critical minerals. Default: data_tables['cm_list'].
  :type cm_list: list

  :param metals_dict: A dictionary denoting metal type per commodity. Commodities may be metal, non-metal, or REE. Default: data_tables['metals_dict']
  :type metals_dict: dict

  :param convert_dict: A dictionary containing commodity names and symbol names. Used to convert the oam_comm_names to standard symbols and names. Default: data_tables['convert_dict']
  :type convert_dict: dict
  """

  def _commit_object(obj):
    try:
      session.add(obj)
      session.commit()
    except IntegrityError as ie:
      print(ie)
      session.rollback()

  # Mine object
  mineVals = get_table_values(row, {
  "Name": "name",
  "Lat_DD": "latitude",
  "Long_DD": "longitude",
  "Jurisdiction": "prov_terr",
  "Status": "mine_status",
  "Mine_Type": "mine_type",
  "Start_Date": "construction_year"
  })
  # Names in OAM are ALL CAPS. This might make acronyms or intentionally all-caps names look wrong
  mineVals['name'] = mineVals['name'].title()
  mineVals['cmdb_id'] = cmdb_id
  mine = Mine(**mineVals)
  # commit_object(mine)

  # Commodity record object

  # Year is sometimes generalized as, e.g., '1960s'. For now, we're just calling that '1960'.
  def check_year(val):
    if isinstance(val, str):
      return get_digits(val)
    elif pd.isna(val):
      return None
    else:
      return val
  comm_code = row['Commodity_Code']
  comm_full = row['Commodity_Full_Name'] # Records have either code or full name. Check both.
  comm_name = comm_code if pd.notna(comm_code) else comm_full # This assumes that no records have both.
  if pd.notna(comm_name):
    try:
      # Sometimes multiple listed in code, split it up and add one entry for each
      commodities = [comm.strip() for comm in comm_name.split(",")]
      for comm in commodities:
        # Convert to full name using OAM name values, then to element names
        comm_full_oam = convert_commodity_name(comm, oam_comm_names, output_type='full', show_warning=False)
        comm_name = convert_commodity_name(comm_full_oam, convert_dict, output_type='symbol', show_warning=False)
        # print(f"{comm} -- {comm_full_oam} -- {comm_name}")
        start_year = check_year(row['Start_Date'])
        end_year = check_year(row['Last_Year'])
        produced = row["Mined_Quantity"] if pd.notna(row["Mined_Quantity"]) else None
        commodityRecord = CommodityRecord(
          mine=mine,
          commodity=comm_name,
          produced=produced,
          source_year_start=start_year,
          source_year_end=end_year
        )
        commodityRecord.is_critical = True if comm_name in cm_list['Commodity'].tolist() else False
        commodityRecord.is_metal = metals_dict.get(comm_name)
    except Exception as e:
      session.rollback()
      print(e)

  # Tailings facility object
  tsfName = f"default_TSF_{mineVals['name']}".strip()
  tsf = TailingsFacility(**{'default': True, 'name': tsfName})
  mine.tailings_facilities.append(tsf)
  # commit_object(tsf)

  # Impoundment object
  impoundmentName = f"{tsfName}_impoundment"
  impoundment = Impoundment(**{'default': True, 'name': impoundmentName})
  tsf.impoundments.append(impoundment)
  # commit_object(impoundment)

  # Owner object
  owner = Owner(name=row['Last_Operator'])
  owner.mines.append(mine)
  # commit_object(owner)

  # Reference object
  # OMI reference
  oam_reference = Reference(mine=mine, source_id=row["OID"], source="OAM")
  oam_reference.link = row['URL']
  # commit_object(oam_reference)

  _commit_object(mine)

def ingest_oam(oam_dataframe:pd.DataFrame, session):
  """
  Appends the Orphaned and Abandoned Mines DataFrame to the CMTI.

  :param oam_dataframe: A DataFrame of the OAM.
  :type oam_dataframe: pandas.DataFrame

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session
  """
  id_manager = ID_Manager()
  comm_name_lut = pd.read_csv("/content/gdrive/MyDrive/NRCan/Projects/CMDB/Data/OAM_commodity_names.csv")
  oam_convert_dict = dict(zip(comm_name_lut['Symbol'], comm_name_lut['Full_Name']))
  for i, row in oam_dataframe.iterrows():
    try:
      provID = getattr(id_manager, row['Jurisdiction'])
      provID.update_id()
      cmdb_id = provID.formatted_id
      oam_row_to_cmti(row, cmdb_id, oam_convert_dict)
    except Exception as e:
      print(e)
  session.close()

# BC AHM - Abandoned and Historic Mines

def bc_ahm_row_to_cmti(row:pd.Series, id_manager:ID_Manager, session):
  """
  Takes a row of the BC Abandoned and Historic Mine (BC AHM) 
  
  :param row: A row from the bc_ahm DataFrame.
  :type row: pandas.Series

  :param id_manager: A CmtiIDManager class instance for managing IDs.
  :type id_manager: cmti_tools.tools.CmtiIDMAnager

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session
  """


  def commit_object(obj):
    try:
      session.add(obj)
      session.commit()
    except IntegrityError as ie:
      print(ie)
      session.rollback()

  # Mine values
  mineVals = get_table_values(row, {
  "NAME1": "name",
  "LATITUDE": "latitude",
  "LONGITUDE": "longitude",
  "UTM_ZONE": "utm_zone",
  "UTM_NORT": "northing",
  "UTM_EAST": "easting",
  "First_Year": "year_opened",
  "Last_Year": "year_closed",
  "NTSMAP_C1": "nts_area"
  })
  mineVals['prov_terr'] = "BC"
  mineVals['mine_status'] = "Inactive"

  # If either lat or lon are missing, don't add that record
  if pd.isna(mineVals['latitude']) or mineVals['latitude'] == 'Null' or \
    pd.isna(mineVals['longitude']) or mineVals['longitude'] == 'Null':
      return

  # Check coordinates - some have 'Null' str
  if mineVals['northing'] == 'Null' or pd.isna(mineVals['northing']):
    del(mineVals['northing'])
  if mineVals['easting'] == 'Null':
    del(mineVals['easting'])
  if pd.isna(mineVals['utm_zone']) or mineVals['utm_zone'] == 'Null':
    mineVals['utm_zone'] = lon_to_utm_zone(mineVals['longitude'])

  bc_id = id_manager.BC

  cmdb_id = bc_id.formatted_id
  mine = Mine(cmdb_id=cmdb_id, **mineVals)
  bc_id.update_id()

  if pd.notna(row.NAME2):
    alias = Alias(mine=mine, alias=row.NAME2)

  # Commodities
  # get_commodities(row, ['COMMOD_C1', 'COMMOD_C2', 'COMMOD_C3'], mine, cm_list)

  # Owner(s)
  # for permittee in ['Permitee1', 'Permittee2']: # 'Permitee'[1] is a typo in the bc_ahm
  #    if pd.notna(row[permittee]):
  #     owner = Owner(name=permittee)
  #     mine.owners.append(owner)
  # if row.Permitee1 != "Null":
  #   permittee = Owner(row.Permitee1)
  #   mine.owners.append(permittee)
  # if row.Permittee2 != "Null":
  #   mine.owners.append(Owner(row.Permittee2))

  # TSF
  tsfName = f"default_TSF_{mineVals['name']}".strip()
  tsf = TailingsFacility(default=True, cmdb_id=cmdb_id, name=tsfName)
  mine.tailings_facilities.append(tsf)
  bc_id.update_id()

  # Impoundment
  impoundment = Impoundment(parentTsf=tsf, default=True, cmdb_id=cmdb_id, name=f"{tsfName}_impoundment")
  bc_id.update_id()

  # Reference
  reference = Reference(mine=mine, source="BCAHM", source_id=str(row.OBJECTID))
  if row.MINFILNO != "Null":
    minfileref = Reference(mine=mine, source="BC Minfile", source_id=row.MINFILNO)

  # Orebody
  orebody = Orebody(mine=mine, ore_type=row.DEPOSITTYPE_D1, ore_class=row.DEPOSITCLASS_D1)
  if row.DEPOSITTYPE_D2 != "Null":
    orebody2 = Orebody(mine=mine, ore_type=row.DEPOSITTYPE_D2, ore_class=row.DEPOSITCLASS_D2)

  commit_object(mine)

def ingest_bc_ahm(bc_ahm_dataframe, session):
  """
  Appends the BC Abandoned and Historic Mines (BC AHM) database to the CMTI.

  :param bc_ahm_dataframe: The BC AHM DataFrame.
  :type bc_ahm_dataframe: pandas.DataFrame

  :param session: The SQL Alchemy Session associated with the CMTI.
  :type session: sqlalchemy.sessionmaker.Session
  """

  id_manager = ID_Manager()
  for i, row in bc_ahm_dataframe.iterrows():
      bc_ahm_row_to_cmti(row, id_manager, session)
    # try:
    # except Exception as e:
    #   print(e, row) # placeholder except