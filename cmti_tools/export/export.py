from typing import Literal
import pandas as pd
import csv
from cmti_tools.tables import Mine
from cmti_tools.tools import convert_commodity_name
from cmti_tools.tools import lon_to_utm_zone
from sqlalchemy import select

def orm_to_csv(orm_class:object, out_name:str, session):
  """
  Exports an ORM class object as a csv.

  :param orm_class: An ORM object.
  :type orm_class: sqlalchemy.orm.DeclarativeBase

  :param out_name:
  The name of the output csv. Include .csv extension. Include full filepath if location other than working
  directory is desired.
  :type out_name: str.

  :param session: The sqlalchemy session.
  :type session: sqlalchemy.Session.

  :return: None.
  """
  query = session.query(orm_class)
  columns = orm_class.__table__.columns
  csvColumns = [col.key for col in columns]
  with open(out_name, 'w') as file:
    writer = csv.writer(file)
    # Write header (columns names)
    writer.writerow(csvColumns)
    # Get rows and write
    [writer.writerow([getattr(row, column.name) for column in orm_class.__mapper__.columns]) for row in query]
  session.close()

def db_to_dataframe(worksheet:pd.DataFrame, session, name_convert_dict, method:Literal['append', 'overwrite']='append', ignore_default_records:bool=True):

  """
  Converts database (in form of sqlalchemy Session) to a Pandas dataframe.

  :param worksheet: The original worksheet table used to generate the database, or a table with the desired columns.
  :type worksheet: pandas.Dataframe.

  :param session: An existing sqlalchemy session.
  :type session: sqlalchemy.orm.Session.

  :param ignore_default_records: Whether to ignore or use the "default" TSF and Impoundment values generated in the database. Default: true.
  :type ignore_default_records: bool.
  """

  new_rows = []
  if method == 'append':
    existing_ids = worksheet['CMIM_ID'].tolist()
    query_stmt = select(Mine).filter(Mine.cmdb_id not in existing_ids)
  elif method == 'overwrite':
    query_stmt = select(Mine)
  else:
    raise ValueError("Method must be 'append' or 'overwrite'")
  
  # Get all mine records
  with session.execute(query_stmt).scalars() as site_records:
    for r in site_records:
      new_row = {} # Each value is assigned to a dictionary

      # Direct values of mine table
      new_row['Site_Name'] = r.name
      new_row['Site_Type'] = 'Mine'
      new_row['CMIM_ID'] = r.cmdb_id
      new_row['NAD'] = 83
      new_row['UTM_Zone'] = r.utm_zone
      new_row['Easting'] = r.easting
      new_row['Northing'] = r.northing
      new_row['Latitude'] = r.latitude
      new_row['Longitude'] = r.longitude
      new_row['Country'] = "Canada"
      new_row['Province_Territory'] = r.prov_terr
      new_row['Mine_Type'] = r.mine_type
      new_row['Mining_Method'] = r.mining_method
      new_row['Mine_Status'] = r.mine_status
      new_row['Dev_Stage'] = r.development_stage
      new_row['Site_Access'] = r.site_access
      new_row['Construction_Year'] = r.construction_year

      # Values of children of mine object
      # Commodities
      comm_number = 1
      for comm in r.commodities:
        # Maintain list of existing commodities to avoid duplicates
        row_commodities = [new_row[f'Commodity{n}'] for n in range(1, comm_number)]
        comm_col = f'Commodity{comm_number}'
        code = convert_commodity_name(comm.commodity, name_convert_dict, 'symbol', show_warning=False)
        if code not in row_commodities:
          new_row[comm_col] = code
          new_row[f'{code}_Grade'] = comm.grade
          new_row[f'{code}_Produced'] = comm.produced
          new_row[f'{code}_Contained'] = comm.contained
          comm_number += 1
        else:
          # print(f"{comm} already in row")
          pass

      # Owner
      if len(r.owners) == 1:
        new_row['Owner'] = r.owners[0].name

      # Alias
      alias_list = []
      for alias in r.aliases:
        alias_name = alias.alias
        if len(r.aliases) <= 1:
          alias_list.append(alias_name)
        elif alias_name not in alias_name and alias_name != 'Unknown': # Avoid duplicates and 'Unknown'
          alias_list.append(alias_name)
      new_alias = ', '.join(alias_list)
      new_row['Site_Aliases'] = new_alias

      # Tailings Facilities

      # This probably ain't it.
      # Relies on one default TSF per mine and one default impoundment per TSF. Conceptually there's no reason for this not to be true, but it's not enforced.

      for tsf in r.tailings_facilities:
        # Some k/v pairs are used in parent if no non-default TSF exists
        tsf_common_values = {}

        tsf_common_values['Hazard_Class'] = tsf.hazard_class

        # If TSF is not the mine's default, it gets its own row
        if not tsf.is_default:
          tsf_row = {}
          tsf_row['Site_Name'] = tsf.name
          tsf_row['Site_Type'] = 'TSF'
          tsf_row['Latitude'] = tsf.latitude or r.latitude
          tsf_row['Parent'] = r.name
          tsf_row['Parent_ID'] = r.cmdb_id
          tsf_row['Longitude'] = tsf.longitude or r.longitude
          tsf_row['CMIM_ID'] = tsf.cmdb_id
          tsf_row['NAD'] = r.nad
          tsf_row['UTM_Zone'] = r.utm_zone or lon_to_utm_zone(tsf_row['Longitude'])
          tsf_row['Easting'] = r.easting
          tsf_row['Northing'] = r.northing
          tsf_row['Country'] = "Canada"
          tsf_row['Province_Territory'] = r.prov_terr
          tsf_row['Mine_Type'] = r.mine_type
          tsf_row['Mining_Method'] = r.mining_method
          tsf_row['Mine_Status'] = tsf.status or r.mine_status
          tsf_row['Dev_Stage'] = r.development_stage
          tsf_row['Site_Access'] = r.site_access
          tsf_row['Hazard_Class'] = tsf.hazard_class

          # Combine common and non-default TSF values, with priority to non-default values
          tsf_row = tsf_common_values | tsf_row

        for impoundment in tsf.impoundments:
          # Some k/v pairs are assigned to parent if non-default impoundment exists
          impoundment_common_values = {}

          impoundment_common_values['Tailings_Area'] = impoundment.area
          impoundment_common_values['Tailings_Capacity'] = impoundment.capacity
          impoundment_common_values['Tailings_Volume'] = impoundment.volume
          impoundment_common_values['Acid_Generating'] = impoundment.acid_generating
          impoundment_common_values['Tailings_Storage_Method'] = impoundment.storage_method
          impoundment_common_values['Current_Max_Height'] = impoundment.max_height
          impoundment_common_values['Treatment'] = impoundment.treatment
          impoundment_common_values['Rating_Index'] = impoundment.rating_index
          impoundment_common_values['History_Stability_Concerns'] = impoundment.stability_concerns

          if not impoundment.is_default:
            impoundment_row = {}
            impoundment_row['Site_Name'] = impoundment.name
            impoundment_row['Site_Type'] = 'Impoundment'
            impoundment_row['Parent'] = impoundment.parentTsf.name
            impoundment_row['Parent_ID'] = impoundment.parentTsf.cmdb_id
            impoundment_row['Latitude'] = impoundment.parentTsf.latitude or r.latitude
            impoundment_row['Longitude'] = impoundment.parentTsf.longitude or r.longitude
            impoundment_row['CMIM_ID'] = impoundment.cmdb_id
            impoundment_row['NAD'] = r.nad
            impoundment_row['UTM_Zone'] = r.utm_zone or lon_to_utm_zone(impoundment_row['Longitude'])
            impoundment_row['Easting'] = r.easting
            impoundment_row['Northing'] = r.northing
            impoundment_row['Country'] = "Canada"
            impoundment_row['Province_Territory'] = r.prov_terr
            impoundment_row['Mine_Type'] = r.mine_type
            impoundment_row['Mining_Method'] = r.mining_method
            impoundment_row['Mine_Status'] = impoundment.parentTsf.status
            impoundment_row['Site_Access'] = r.site_access

            # Combine common and non-default impoundment values
            impoundment_row = tsf_row | impoundment_common_values | impoundment_row
            new_rows.append(impoundment_row) # Use data from parent TSF if missing from impoundment, giving priority to impoundment

          else: # If impoundment is default, add common impoundment values to tsf
            tsf_common_values = tsf_common_values | impoundment_common_values
          
          if tsf.is_default:
            new_row = new_row | tsf_common_values
          else:
            new_rows.append(tsf_row)
        
      # References
      # Get all non-null references
      refs = [ref for ref in r.references if pd.notna(ref.source) and ref.source != 'Unknown']
      source_number = 1
      while source_number <= 4 and source_number <= len(refs):
        ref = refs[source_number - 1]
        new_row[f'Source_{source_number}'] = ref.source
        new_row[f'Source_{source_number}_ID'] = ref.source_id
        new_row[f'Source_{source_number}_Link'] = ref.link
        source_number += 1

      # Add the new_row dict to the list of rows
      new_rows.append(new_row)

  new_records = pd.DataFrame(new_rows, columns=worksheet.columns)
  if method == 'append':
    out_df = pd.concat([worksheet, new_records], axis=0, ignore_index=True, join='outer')
  elif method == 'overwrite':
    out_df = new_records
  return out_df

# def export_database():
#   !pg_dump cmdb > cmdb_backup.sql
#   !pg_dump -C -h localhost -U postgres cmdb | psql -h remotehost -U remoteuser dbname
