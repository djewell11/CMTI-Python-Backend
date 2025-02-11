from typing import Literal
import pandas as pd
import csv
from cmti_tools.tables import Mine
from cmti_tools.tools import convert_commodity_name
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
      tsf = [_tsf for _tsf in r.tailings_facilities if _tsf.is_default == True][0] # We're assuming only one default TSF
      new_row['Hazard_Class'] = tsf.hazard_class

      impoundment = [_imp for _imp in r.tailings_facilities if _imp.is_default == True][0] # We're assuming only one default impoundment
      new_row['Tailings_Area'] = impoundment.area
      new_row['Tailings_Capacity'] = impoundment.capacity
      new_row['Tailings_Volume'] = impoundment.volume
      new_row['Acid_Generating'] = impoundment.acid_generating
      new_row['Tailings_Storage_Method'] = impoundment.storage_method
      new_row['Current_Max_Height'] = impoundment.max_height
      new_row['Treatment'] = impoundment.treatment
      new_row['Rating_Index'] = impoundment.rating_index
      new_row['History_Stability_Concerns'] = impoundment.stability_concerns

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
