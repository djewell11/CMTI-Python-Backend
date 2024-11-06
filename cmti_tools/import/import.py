# Comment to test push
import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from cmtitools.tools import get_digits
from cmtitools.tools import get_table_values
from cmtitools.tools import convert_commodity_name
from cmtitools.tools import lon_to_utm_zone
from cmtitools.tools import data_tables
from cmtitools.tools import session
from cmtitools.tables import Mine, Owner, Alias, TailingsFacility, Impoundment, CommodityRecord, Reference, Orebody
from cmtitools.idmanager import ProvID
from cmtitools.idmanager import CmtiIDManager

# Bulk import functions

# OMI

def omi_row_to_cmti(row, cmdb_id, production_df, production_comm_df, cmList=data_tables['cmList'], metalsDict=data_tables['metalsDict'], session=session):
  # For each row in the OMI dataframe, extract necessary values for each object

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
      commodityRecord.is_critical = True if commodityVals['commodity'] in cmList else False
      commodityRecord.is_metal = metalsDict.get(commodityVals['commodity'])
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

def ingest_omi(omi_dataframe, omi_production_dataframe, omi_prod_comm_df):

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
        omi_row_to_cmti(row, newID, omi_production_dataframe, omi_prod_comm_df)
      elif cmim_len == 1:
        cmdb_id = cmdb_ids[0]
        omi_row_to_cmti(row, cmdb_id, omi_production_dataframe, omi_prod_comm_df)
      elif cmim_len > 1:
        # Not sure how to handle this yet. Maybe use secondary matching to see if record already exists
        pass
    except Exception as e:
      print(e)
    session.commit()
    session.close()

# OAM

def oam_row_to_cmti(row, cmdb_id, oam_comm_names, cmList=data_tables['cmList'], metalsDict=data_tables['metalsDict'], convert_dict=['convert_dict'], session=session):

  def commit_object(obj):
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
        commodityRecord.is_critical = True if comm_name in cmList['Commodity'].tolist() else False
        commodityRecord.is_metal = metalsDict.get(comm_name)
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

  commit_object(mine)

def ingest_oam(oam_dataframe, session=session):
  id_manager = CmtiIDManager()
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

# BC AHM

def bc_ahm_row_to_cmti(row, id_manager, session=session):

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
  # get_commodities(row, ['COMMOD_C1', 'COMMOD_C2', 'COMMOD_C3'], mine, cmList)

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

def ingest_bc_ahm(bc_ahm_dataframe, session=session):
  id_manager = CmtiIDManager()
  for i, row in bc_ahm_dataframe.iterrows():
      bc_ahm_row_to_cmti(row, id_manager)
    # try:
    # except Exception as e:
    #   print(e, row) # placeholder except