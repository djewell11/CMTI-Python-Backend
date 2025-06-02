Module cmti_tools.qualitycontrol.qualitycontrol
===============================================

Functions
---------

`check_categorical_values(row: pandas.core.series.Series, qa_dict: dict, ignore_unknown=True, ignore_na=True, ignore_blank=True)`
:   Verifies that value given matches list of approved values in template. #TODO determine behaviour for bad values (currently prints to console).
    
    :param row: A row from an input DataFrame.
    :type row: pandas.Series
    
    :param qa_dict: A dictionary where keys are columns and values are lists of approved strings for those columns.
    :type qa_dict: dict
    
    :param ignore_unknown: Whether to flag entries where value == 'Unknown' or 'N/A/Unknown'. Default: True.
    :type ignore_unknown: bool
    
    :param ignore_na: Whether to flag entries where value is an NA type. Default: True.
    :type ignore_na: bool
    
    :param ignore_blank: Whether to flag entries where value == ''. Default: True.
    :type ignore_blank: bool

`convert_unit(value, desired_unit: str, dimensionless_value_unit: str = None, ureg: pint.registry.UnitRegistry = None)`
:   Converts a value to a desired unit using a pint.UnitRegistry object.
    
    :param value: The input value.
    :type value: int, float, or str
    
    :param desired_unit: The desired output unit. If None, the usual defaults are used.
    :type desired_unit: str
    
    :param ureg: A pint.UnitRegistry object.
    :type ureg: pint.UnitRegistry
    
    :param dimensionless_value_unit: The unit of the input value if dimensionless. If None, dimensionless values are ignored. Will not overwrite strings with dimensions. Default: None.
    :type dimensionless_value_unit: str 
    
    :return: The converted value.
    :rtype: int, float

Classes
-------

`DataGrader(main: dict, comms: dict, years: dict, source: dict, comm_col_count=8, source_col_count=4)`
:   

    ### Methods

    `assign_score(self, row)`
    :

    `calculate_commodity_values(self, row)`
    :

    `calculate_main_values(self, row)`
    :

    `calculate_source_values(self, row)`
    :

    `calculate_year_values(self, row)`
    :

    `perfect_row(self)`
    :