Module cmti_tools.importdata.importdata
=======================================

Classes
-------

`DataImporter(name_convert_dict: str | dict | None = None, cm_list: str | dict | None = None, metals_dict: str | dict | None = None)`
:   An abstract base class for importing data sources.
    Manages data initialization, record creation, and database ingestion.
    
    Initializes the DataImporter class with optional configurations for 
    name conversion, critical minerals, and metals classification.

    ### Ancestors (in MRO)

    * abc.ABC

    ### Descendants

    * cmti_tools.importdata.source_importers.importers.BCAHMImporter
    * cmti_tools.importdata.source_importers.importers.NSMTDImporter
    * cmti_tools.importdata.source_importers.importers.OAMImporter
    * cmti_tools.importdata.source_importers.importers.OMIImporter
    * cmti_tools.importdata.source_importers.importers.WorksheetImporter

    ### Methods

    `clean_input_table(self, input_table: pandas.core.frame.DataFrame, force_dtypes: bool = True, convert_units: bool = True) ‑> pandas.core.frame.DataFrame`
    :   Clean values in the input table and make column data types consistent.

    `coerce_dtypes(self, input_types_table, input_table: pandas.core.frame.DataFrame) ‑> pandas.core.frame.DataFrame`
    :   Coerces the data types of the input table based on the types_table.

    `create_row_records(self, row: pandas.core.series.Series) ‑> None`
    :   Process a single row and generates a DeclarativeBase objects based on inputs. 
        Implemented by child classes.

    `map_to_worksheet(self, worksheet: pandas.core.frame.DataFrame, source: pandas.core.frame.DataFrame, mapping: dict) ‑> pandas.core.frame.DataFrame`
    :   Maps source DataFrame to worksheet DataFrame using the provided mapping dictionary.
        Creates a new DataFrame with the same columns as the worksheet and populates it with values from the source DataFrame.
        
        :param worksheet: The DataFrame to be populated.
        :type worksheet: pandas.DataFrame.
        
        :param source: The DataFrame to be mapped.
        :type source: pandas.DataFrame.
        
        :param mapping: A dictionary mapping source columns to worksheet columns.
        :type mapping: dict.
        
        :return: The populated worksheet DataFrame.

    `series_to_table(self, table: sqlalchemy.orm.decl_api.DeclarativeBase, series: pandas.core.series.Series, datamapping: dict) ‑> sqlalchemy.orm.decl_api.DeclarativeBase`
    :   Creates a table object from a pandas Series. Fetches table attributes from the series automatically based on mapping

`converter_factory(types_table, unit_conversion_dict: dict = None, **kwargs)`
:   A class that generates converters for use in pandas, based on expected column datatypes and default values.
    
    ...
    
    Attributes
    ----------
    types_table: pandas.DataFrame
      A DataFrame with columns "Column", "Type", and "Default". Column must have unique values.
    
    Methods
    ----------
    create_converter(column):
      Uses types_table to create a converter for input column. Returns a function based on column dtype.
    
    create_converter_dict():
      Calls create_converter for each Column value in types_table and returns a dictionary of converter functions.
    
    :param types_table: A DataFrame with columns "Column", "Type", and "Default". Column values must be unique.
    :type types_table: pandas.DataFrame.
    
    :param unit_conversion_dict: A dictionary of unit conversions for specific columns. Key = column, value = desired unit. Default: None.

    ### Methods

    `create_converter(self, column: str)`
    :   Creates a function for the input column that either returns the default or performs some cleanup action.
        
        :param column: The Column value from types_table.
        :type column: str.
        
        :return: Function

    `create_converter_dict(self)`
    :   Runs create_converter for all columns in types_table.Column.
        
        :return: dict.