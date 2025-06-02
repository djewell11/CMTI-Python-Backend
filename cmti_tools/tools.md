Module cmti_tools.tools
=======================

Functions
---------

`convert_commodity_name(name: str, name_convert_dict: dict, output_type: str = 'full', show_warning=False)`
:   Takes element names and converts them to either symbol or full name. Ignores names not found in name_convert_dict.
    
    :param name: The commodity name.
    :type value: str.
    
    :param name_convert_dict: A dictionary where keys are symbols and values are full names.
    :type name_convert_dict: dict.
    
    :param output_type: The type of output desired, either "full" or "symbol". Commodities that don't match elements on the 
      periodic table will revert to "full". Default: "full".
    :type output_type: str.
    
    :param show_warning:
      Determines whether or not a warning is printed when "name" isn't present in "name_convert_dict".
      Absences are expected for non-element commodities. Default: False
    :type name_convert_dict: bool.

`create_name_dict(elements_csv: pandas.core.frame.DataFrame) ‑> dict`
:   

`get_commodity(row: pandas.core.series.Series, commodity_column: str, critical_mineral_list: list, name_convert_dict: dict, metals_dict: dict, mine: cmti_tools.tables.tables.Mine, name_type: str = 'full') ‑> cmti_tools.tables.tables.CommodityRecord`
:   Takes multiple commodity columns from the spreadsheet and creates a CommodityRecord.
    
    :param row: A dataframe row.
    :type row: pandas.Series.
    
    :param commodity_columns: A list of column names containing commodity values.
    :type commodity_columns: list.
    
    :param critical_mineral_list: A list of critical minerals.
    :type critical_mineral_list: list.
    
    :param name_convert_dict: A dictionary that can convert commodity element symbols to full names or vice versa.
    :type name_convert_dict: dict.
    
    :param metals_dict: A dictionary that determines whether a commodity is a non-metal, metal, or REE.
    :type metals_dict: dict.
    
    :param mine: An sqlalchemy ORM class of type Mine.
    :type mine: sqlalchemy.orm.DeclarativeBase.
    
    :param name_type: The output style for the commodity name, as entered in convert_commodity_name.
      Either "full" or "symbol". Default: "full".
    :type name_type: str.
    
    :return: CommodityRecord

`get_digits(value: str, output: str = 'float')`
:   Used for columns that contain quantities and may have erroneously included units.
    
    :param value: A quantity that includes numerical values.
    :type value: str.
    
    :param output: The output type. Either 'float' or 'int'. Default: 'float'.
    :type output: str.
    
    :return: A numerical value with units removed.
    :rtype: Either float or int, according to param 'output'.

`get_table_values(row: pandas.core.series.Series, columnDict: dict, default_null: object = None)`
:   Takes column values from columnDict and produces a new dictionary where key = database column and
    value = original (dataframe/excel) value. This dictionary can be used to create an ORM object via dict unpacking.
    
    :param row: A dataframe row.
    :type row: pandas.Series.
    
    :param columnDict: A dictionary where key = dataframe column name and value = database column name
    :type name_type: str.
    
    :param default_null: The value that will be added to the output dictionary if column value is missing.
    :type default_null: Any
    
    :return: A dictionary of table values.
    :rtype: dict.

`load_config(user_config_path=None)`
:   

`lon_to_utm_zone(lon_deg: float)`
:   Takes the longitude in decimal degrees and returns the UTM zone as an int.
    Assumes coordinates are in the northern hemisphere.
    
    :param lon_deg: The longitudinal coordinate in decimal degrees. Include a negative sign for western hemisphere.
    :type lon_deg: float.

`override_config_value(config, section, key, value)`
:   

`shift_values(row: pandas.core.series.Series, col_list: list, blank_values: list = ['Unknown']) ‑> dict`
:   Shifts values of numbered columns to infill from the left.
    
    :param row: A dataframe row.
    :type row: pandas.Series.
    
    :param col_list: A list of column names to shift.
    :type col_list: list.
    
    :param blank_values: A list of values that are considered blank. Default: ["Unknown"].
    :type blank_values: list.

`value_to_range(value: int | float, unit_singular: str, unit_plural: str = None, intervals: list = [1, 10, 100, 1000, 10000, 100000, 1000000])`
:   Converts a single value to a string representing range.
    
    :param value: A numerical value.
    :type value: int or float.
    
    :param unit_singular: Name of unit if quantity is 1.
    :type unit_singular: str.
    
    :param unit_plural: Name of unit is quantity is more than 1.
    :type unit_plural: str. Default: None.
    
    :param intervals: A list of cutoff values to create range categories.
    :type intervals: list.