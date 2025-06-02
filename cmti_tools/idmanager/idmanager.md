Module cmti_tools.idmanager.idmanager
=====================================

Classes
-------

`ID_Manager()`
:   A container providing easy access to a ProvID object for all Canadian jurisdictions.

    ### Methods

    `update_all_session(self, session)`
    :

    `update_prov_ids(self, mine_ids: list | pandas.core.series.Series)`
    :   Updates the max_id for each provID object based on the provided list of mine IDs.
        
        :param mine_ids: list or Series of mine IDs. Entries must be in the format of 'XX000001' where XX is the province/territory code.
        :type mine_ids: list | Pandas.Series
        
        :param mine_ids: list or Series of mine IDs

`ProvID(code: str)`
:   An object containing a string representing a provincial ID.
    
    Attributes:
      code (str): A two-letter code denoting the province or territory.
      max_id (int): The largest existing ID per that code. i.e., the integer to start with when creating  additionaly IDs.
      formatted_id (str): A concatenated string of the code and max_id.

    ### Instance variables

    `code`
    :

    `formatted_id`
    :

    `max_id`
    :

    ### Methods

    `format_id(self, id_val: int) ‑> str`
    :   Concatenates province/territory code and max_id to create ID string.
        
        :return: str

    `generate_id(self)`
    :   Generates a new ID by incrementing the max_id by 1.

    `get_max_id(self, mine_ids: list | pandas.core.series.Series) ‑> int`
    :   Query the list of IDs to get the highest ID for a given provID.
        
        :param mine_ids: list or Series of mine IDs. Entries must be in the format of 'XX000001' where XX is the province/territory code.
        :type mine_ids: list | Pandas.Series
        
        :return: int

    `query_session_id(self, session)`
    :   Query the session to get the highest ID for a given provID.
        
        :return: int

    `update_id(self, id_val: int)`
    :   Updates the max_id and formatted_id attributes.