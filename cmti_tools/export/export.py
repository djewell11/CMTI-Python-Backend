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

def db_to_dataframe(worksheet:pd.DataFrame, session, name_convert_dict, ignore_default_records:bool=True):

  """
  Converts database (in form of sqlalchemy Session) to a Pandas dataframe.

  :param worksheet: The original worksheet table used to generate the database, or a table with the desired columns.
  :type worksheet: pandas.Dataframe.

  :param session: An existing sqlalchemy session.
  :type session: sqlalchemy.orm.Session.

  :param ignore_default_records: Whether to ignore or use the "default" TSF and Impoundment values generated in the database. Default: true.
  :type ignore_default_records: bool.
  """

  # new_records = pd.DataFrame(columns=worksheet.columns)
  new_rows = []
  existing_ids = worksheet['CMIM_ID'].tolist()
  new_sites_stmt = select(Mine).filter(Mine.cmdb_id not in existing_ids)
  with session.execute(new_sites_stmt).scalars() as new_sites:
    for r in new_sites:
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

      # TSF and impoundment should get their own queries
      # check ignore_defaults

      # Impoundment

      # References
      # Get all non-null references
      refs = [ref for ref in r.references if ref.source != 'Unknown' and pd.notna(ref.source)]
      source_number = 1
      while source_number <= 4 and source_number <= len(refs):
        ref = refs[source_number - 1]
        new_row[f'Source_{source_number}'] = ref.source
        new_row[f'Source_{source_number}_ID'] = ref.source_id
        new_row[f'Source_{source_number}_Link'] = ref.link
        source_number += 1


      # source_number = 1
      # for ref in r.references:
      #   if source_number <= 4 and ref.source != 'Unknown':
      #     new_row[f'Source_{source_number}'] = ref.source
      #     new_row[f'Source_{source_number}_ID'] = ref.source_id
      #     new_row[f'Source_{source_number}_Link'] = ref.link
      #     source_number += 1
      #   else:
      #     print(f"More than 4 sources detected for site {r.id}")
  
      # Add the new_row dict to the list of rows
      new_rows.append(new_row)

  new_records = pd.DataFrame(new_rows)
  out_df = pd.concat([worksheet, new_records], axis=0, ignore_index=True, join='outer')
  return out_df

# def export_database():
#   !pg_dump cmdb > cmdb_backup.sql
#   !pg_dump -C -h localhost -U postgres cmdb | psql -h remotehost -U remoteuser dbname
