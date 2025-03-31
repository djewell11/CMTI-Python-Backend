from cmti_tools.importdata import MapperManager

def test_create_mapper():
    manager = MapperManager()
    manager.add_mapper(destination_column_name ="Site_Name", source_column_name = 'Name', dtype="str", default="Unknown")
    manager.add_mapper(destination_column_name="Site_Type", source_column_name = 'Type', dtype="str", default="Unknown")
    manager.add_mapper(destination_column_name="Tailings_Volume", source_column_name = 'Volume', dtype="f4", default=0.0)
    
    df = manager.get_mapper_df()

    assert(len(manager._mappers) == 3)
    assert len(df['DB_Column']) == 3
    assert df.loc[0, 'DB_Column'] == "Site_Name"