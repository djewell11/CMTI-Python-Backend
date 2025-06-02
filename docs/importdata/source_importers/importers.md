Module cmti_tools.importdata.source_importers.importers
=======================================================

Classes
-------

`BCAHMImporter(cm_list: list = 'config', metals_dict: dict = 'config', name_convert_dict: dict = 'config')`
:   An abstract base class for importing data sources.
    Manages data initialization, record creation, and database ingestion.
    
    Initializes the BCAHMImporter class with configuration data.
    
    :param cm_list: List of critical minerals.
    :type cm_list: list
    
    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict
    
    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict

    ### Ancestors (in MRO)

    * cmti_tools.importdata.importdata.DataImporter
    * abc.ABC

    ### Methods

    `create_row_records(self, row: pandas.core.series.Series, cm_list: list = None, metals_dict: dict = None, name_convert_dict: dict = None)`
    :   Processes a row of data from the BCAHM dataset and creates associated database records.
        
        :param row: The data row to be processed.
        :type row: pd.Series
        
        :param cm_list: List of critical minerals, defaults to the class attribute.
        :type cm_list: list
        
        :param metals_dict: Dictionary of metals and their properties, defaults to the class attribute.
        :type metals_dict: dict
        
        :param name_convert_dict: Dictionary for commodity name conversion, defaults to the class attribute.
        :type name_convert_dict: dict
        
        :return list[object]: A list of created data records.

`NSMTDImporter(name_convert_dict='config', cm_list='config', metals_dict='config')`
:   An abstract base class for importing data sources.
    Manages data initialization, record creation, and database ingestion.
    
    Initializes the DataImporter class with optional configurations for 
    name conversion, critical minerals, and metals classification.

    ### Ancestors (in MRO)

    * cmti_tools.importdata.importdata.DataImporter
    * abc.ABC

    ### Methods

    `create_row_records(self, row: pandas.core.series.Series, cm_list: list = None, metals_dict: dict = None, name_convert_dict: dict = None)`
    :   Processes a row of data from the BCAHM dataset and creates associated database records.
        
        :param row: The data row to be processed.
        :type row: pd.Series
        
        :param cm_list: List of critical minerals, defaults to the class attribute.
        :type cm_list: list
        
        :param metals_dict: Dictionary of metals and their properties, defaults to the class attribute.
        :type metals_dict: dict
        
        :param name_convert_dict: Dictionary for commodity name conversion, defaults to the class attribute.
        :type name_convert_dict: dict
        
        :return list[object]: A list of created data records.

`OAMImporter(oam_comm_names: dict = 'config', cm_list='config', metals_dict='config', name_convert_dict='config')`
:   An abstract base class for importing data sources.
    Manages data initialization, record creation, and database ingestion.
    
    Initializes the OAMImporter class with configuration data and commodity names.
    
    :param oam_comm_names: Dictionary of OAM commodity names.
    :type oam_comm_names: dict
    
    :param cm_list: List of critical minerals.
    :type cm_list: list
    
    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict
    
    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict

    ### Ancestors (in MRO)

    * cmti_tools.importdata.importdata.DataImporter
    * abc.ABC

    ### Methods

    `check_year(self, val)`
    :   Checks and extracts the year from a value.
        
        :param val: Value to be checked for a year.
        :type val: str or float
        :return int or None: Extracted year or None if not available.

    `create_row_records(self, row: pandas.core.series.Series, oam_comm_names: dict = None, cm_list: list = None, metals_dict: dict = None, name_convert_dict: dict = None)`
    :   Processes a row of OAM data and creates associated database records.
        
        :param row: The data row to be processed.
        :type row: pd.Series
        
        :param oam_comm_names: Dictionary of OAM commodity names.
        :type oam_comm_names: dict
        
        :param cm_list: List of critical minerals.
        :type cm_list: list
        
        :param metals_dict: Dictionary mapping metal names to properties.
        :type metals_dict: dict
        
        :param name_convert_dict: Dictionary for converting commodity names.
        :type name_convert_dict: dict
        
        :return list[object]: A list of created data records.

`OMIImporter(cm_list: list = 'config', metals_dict: dict = 'config', name_convert_dict: dict = 'config', force_dtypes: bool = True)`
:   An abstract base class for importing data sources.
    Manages data initialization, record creation, and database ingestion.
    
    Initializes the OMIImporter class with configuration data.
    
    :param cm_list: List of critical minerals.
    :type cm_list: list
    
    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict
    
    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict

    ### Ancestors (in MRO)

    * cmti_tools.importdata.importdata.DataImporter
    * abc.ABC

    ### Methods

    `create_row_records(self, row: pandas.core.series.Series, name_convert_dict: dict = None) ‑> list[object]`
    :   Processes a row of data and creates associated database records.
        
        :param row: The data row to be processed.
        :type row: pd.Series
        
        :param name_convert_dict: Optional dictionary for commodity name conversion.
        :type name_convert_dict: dict, optional
        
        :return list[object]: A list of created data records.

`WorksheetImporter(name_convert_dict='config', cm_list='config', metals_dict='config', auto_generate_cmti_ids: bool = False)`
:   Imports worksheet data into the database.
    
    Initializes the DataImporter class with optional configurations for 
    name conversion, critical minerals, and metals classification.

    ### Ancestors (in MRO)

    * cmti_tools.importdata.importdata.DataImporter
    * abc.ABC

    ### Methods

    `clean_input_table(self, input_table, drop_NA_columns=['Site_Name', 'Site_Type', 'CMTI_ID', 'Latitude', 'Longitude'], calculate_UTM=True, force_dtypes=True, convert_units: bool = True, **kwargs)`
    :   :param input_table: The input table to be cleaned.
        :type input_table: pd.DataFrame or str
        
        :param drop_NA_columns: Columns where row should be dropped if value is missing. Provides a way of removing rows that lack required values before committing to database. 
          Default: ['Site_Name', 'Site_Type', 'CMTI_ID', 'Latitude', 'Longitude']
        :type drop_NA_columns: list
        
        :param calculate_UTM: If True, calculate UTM Zone based on Longitude. Default: True
        :type calculate_UTM: bool
        
        :param force_dtypes: If True, enforce data types for all columns. Default: True
        :type force_dtypes: bool
        
        :param convert_units_dict: Dictionary where key == column and value == desired unit. Leave empty to ignore. Default: {}
        :type convert_units_dict: dict
        
        :param kwargs: Additional keyword arguments for unit definitions:
          :param unit_definitions: Dictionary of unit definitions to be added to Pint UnitRegistry. Values should follow pattern '{unit} = {str of definition}'. E.g.: 'm2 = meter ** 2'.
            Default: None
          :type unit_definitions: dict
        
          :param dimensionless_value_units: Dictionary of dimensionless value units. Key = column, value = unit. If None, a default list will be used. Set as {} to disable. Default: None
          :type dimensionless_value_units: dict

    `create_row_records(self, row, cm_list: list = None, metals_dict: dict = None, name_convert_dict: dict = None, comm_col_count: int = 8, source_col_count: int = 4) ‑> list[sqlalchemy.orm.decl_api.DeclarativeBase]`
    :   Processes a worksheet row based on its 'Site_Type' and creates database records.
        
        :param row: A pandas Series containing data from a worksheet row.
        :type row: pd.Series
        
        :param cm_list: Critical Minerals list
        :type cm_list: list
        
        :param metals_dict: Metals dictionary
        :type metals_dict: dict
        
        :param name_convert_dict: Name Convert dictionary
        :type name_convert_dict: dict
        
        :param comm_col_count: Commodity Column count to indicate amount of commodities in record
        :type comm_col_count: int
        
        :param source_col_count: Source Column count to indicate amount of sources in record
        :type source_col_count: int
        
        :return list: row_records

    `process_impoundment(self, row: pandas.core.series.Series, parent_TSF: cmti_tools.tables.tables.TailingsFacility)`
    :

    `process_mine(self, row: pandas.core.series.Series, comm_col_count, source_col_count) ‑> list[sqlalchemy.orm.decl_api.DeclarativeBase]`
    :   Processes mine-specific data and creates Mine, Owner, Alias, 
        Commodity, Reference, and default TSF and Impoundment records.

    `process_tsf(self, row: pandas.core.series.Series, parent_mines: cmti_tools.tables.tables.Mine | list[cmti_tools.tables.tables.Mine])`
    :