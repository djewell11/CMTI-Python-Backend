Module cmti_tools.tables.tables
===============================

Classes
-------

`Alias(*, alias_id=None, mine_id=None, alias=None, mine=None)`
:   Alias(*, alias_id: int = None, mine_id: 'Mine' = None, alias: str = None, mine: 'Mine' = None)

    ### Instance variables

    `alias: str`
    :

    `alias_id: int`
    :

    `mine: cmti_tools.tables.tables.Mine`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

`CommodityRecord(*, commodity, grade=None, grade_unit=None, produced=None, produced_unit=None, contained=None, contained_unit=None, metal_type=None, is_critical=None, source='Unknown', source_id='Unknown', source_year_start=None, source_year_end=None, mine=None)`
:   CommodityRecord(*, commodity: str, grade: str = None, grade_unit: str = None, produced: str = None, produced_unit: str = None, contained: str = None, contained_unit: str = None, metal_type: str = None, is_critical: bool = None, source: str = 'Unknown', source_id: str = 'Unknown', source_year_start: int = None, source_year_end: int = None, mine: 'Mine' = None)

    ### Instance variables

    `commodity: str`
    :

    `contained: str`
    :

    `contained_unit: str`
    :

    `grade: str`
    :

    `grade_unit: str`
    :

    `id: int`
    :

    `is_critical: bool`
    :

    `metal_type: str`
    :

    `mine: cmti_tools.tables.tables.Mine`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

    `produced: str`
    :

    `produced_unit: str`
    :

    `source: str`
    :

    `source_id: str`
    :

    `source_year_end: int`
    :

    `source_year_start: int`
    :

`Impoundment(*, is_default=None, cmti_id='NULL', parent_tsf_id, name=None, area=None, area_from_images=None, area_notes=None, raise_type=None, capacity=None, volume=None, acid_generating=None, storage_method=None, max_height=None, treatment=None, rating_index=None, stability_concerns=None, parentTsf)`
:   Impoundment(*, is_default: bool = None, cmti_id: str = 'NULL', parent_tsf_id: 'TailingsFacility', name: str = None, area: float = None, area_from_images: float = None, area_notes: str = None, raise_type: str = None, capacity: float = None, volume: float = None, acid_generating: bool = None, storage_method: str = None, max_height: float = None, treatment: str = None, rating_index: str = None, stability_concerns: str = None, parentTsf: 'TailingsFacility')

    ### Instance variables

    `acid_generating: bool`
    :

    `area: float`
    :

    `area_from_images: float`
    :

    `area_notes: str`
    :

    `capacity: float`
    :

    `cmti_id: str`
    :

    `id: int`
    :

    `is_default: bool`
    :

    `max_height: float`
    :

    `name: str`
    :

    `parentTsf: cmti_tools.tables.tables.TailingsFacility`
    :

    `parent_tsf_id: cmti_tools.tables.tables.TailingsFacility`
    :

    `raise_type: str`
    :

    `rating_index: str`
    :

    `stability_concerns: str`
    :

    `storage_method: str`
    :

    `treatment: str`
    :

    `volume: float`
    :

`Mine(*, cmti_id='NULL', name, prov_terr, latitude, longitude, last_revised=NaT, datum=83, utm_zone=<NA>, easting=<NA>, northing=<NA>, nts_area='Unknown', mining_district='Unknown', mine_type='Unknown', mine_status='Unknown', mining_method='Unknown', orebody_type='Unknown', orebody_class='Unknown', orebody_minerals='Unknown', processing_method='Unknown', ore_processed=<NA>, ore_processed_unit='Unknown', development_stage='Unknown', site_access='Unknown', construction_year=<NA>, year_opened=<NA>, year_closed=<NA>, ds_comments=None, sa_comments=None, shaft_depth=<NA>, reserves_resources='Unknown', other_mineralization='Unknown', sedar='Unknown', notes='Unknown', forcing_features='Unknown', feature_references='Unknown', noami_status='Unknown', noami_site_class='Unknown', hazard_class='Unknown', hazard_system='Unknown', prp_rating='Unknown', rehab_plan=None, ews='Unknown', ews_rating='Unknown', commodities=None, aliases=<factory>, owners=<factory>, tailings_facilities=<factory>, orebody=None, references=<factory>)`
:   Mine(*, cmti_id: str = 'NULL', name: str, prov_terr: str, latitude: float, longitude: float, last_revised: datetime.datetime = NaT, datum: int = 83, utm_zone: int = <NA>, easting: float = <NA>, northing: float = <NA>, nts_area: str = 'Unknown', mining_district: str = 'Unknown', mine_type: str = 'Unknown', mine_status: str = 'Unknown', mining_method: str = 'Unknown', orebody_type: str = 'Unknown', orebody_class: str = 'Unknown', orebody_minerals: str = 'Unknown', processing_method: str = 'Unknown', ore_processed: float = <NA>, ore_processed_unit: str = 'Unknown', development_stage: str = 'Unknown', site_access: str = 'Unknown', construction_year: int = <NA>, year_opened: int = <NA>, year_closed: int = <NA>, ds_comments: Optional[str] = None, sa_comments: Optional[str] = None, shaft_depth: Optional[float] = <NA>, reserves_resources: Optional[str] = 'Unknown', other_mineralization: Optional[str] = 'Unknown', sedar: Optional[str] = 'Unknown', notes: Optional[str] = 'Unknown', forcing_features: Optional[str] = 'Unknown', feature_references: Optional[str] = 'Unknown', noami_status: Optional[str] = 'Unknown', noami_site_class: Optional[str] = 'Unknown', hazard_class: Optional[str] = 'Unknown', hazard_system: Optional[str] = 'Unknown', prp_rating: Optional[str] = 'Unknown', rehab_plan: Optional[bool] = None, ews: Optional[str] = 'Unknown', ews_rating: Optional[str] = 'Unknown', commodities: 'CommodityRecord' = None, aliases: List[ForwardRef('Alias')] = <factory>, owners: List[ForwardRef('OwnerAssociation')] = <factory>, tailings_facilities: List[ForwardRef('TailingsFacility')] = <factory>, orebody: 'Orebody' = None, references: List[ForwardRef('Reference')] = <factory>)

    ### Instance variables

    `aliases: List[cmti_tools.tables.tables.Alias]`
    :

    `cmti_id: str`
    :

    `commodities: cmti_tools.tables.tables.CommodityRecord`
    :

    `construction_year: int`
    :

    `datum: int`
    :

    `development_stage: str`
    :

    `ds_comments: str | None`
    :

    `easting: float`
    :

    `ews: str | None`
    :

    `ews_rating: str | None`
    :

    `feature_references: str | None`
    :

    `forcing_features: str | None`
    :

    `hazard_class: str | None`
    :

    `hazard_system: str | None`
    :

    `id: int`
    :

    `last_revised: datetime.datetime`
    :

    `latitude: float`
    :

    `longitude: float`
    :

    `mine_status: str`
    :

    `mine_type: str`
    :

    `mining_district: str`
    :

    `mining_method: str`
    :

    `name: str`
    :

    `noami_site_class: str | None`
    :

    `noami_status: str | None`
    :

    `northing: float`
    :

    `notes: str | None`
    :

    `nts_area: str`
    :

    `ore_processed: float`
    :

    `ore_processed_unit: str`
    :

    `orebody: cmti_tools.tables.tables.Orebody`
    :

    `orebody_class: str`
    :

    `orebody_minerals: str`
    :

    `orebody_type: str`
    :

    `other_mineralization: str | None`
    :

    `owners: List[cmti_tools.tables.tables.OwnerAssociation]`
    :

    `processing_method: str`
    :

    `prov_terr: str`
    :

    `prp_rating: str | None`
    :

    `references: List[cmti_tools.tables.tables.Reference]`
    :

    `rehab_plan: bool | None`
    :

    `reserves_resources: str | None`
    :

    `sa_comments: str | None`
    :

    `sedar: str | None`
    :

    `shaft_depth: float | None`
    :

    `site_access: str`
    :

    `tailings_facilities: List[cmti_tools.tables.tables.TailingsFacility]`
    :

    `utm_zone: int`
    :

    `year_closed: int`
    :

    `year_opened: int`
    :

`Orebody(*, mine_id=None, ore_type=None, ore_class=None, minerals=None, ore_processed=None, mine)`
:   Orebody(*, mine_id: 'Mine' = None, ore_type: str = None, ore_class: str = None, minerals: str = None, ore_processed: float = None, mine: 'Mine')

    ### Instance variables

    `id: int`
    :

    `mine: cmti_tools.tables.tables.Mine`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

    `minerals: str`
    :

    `ore_class: str`
    :

    `ore_processed: float`
    :

    `ore_type: str`
    :

`Owner(*, id=None, name=None, mines=<factory>)`
:   Owner(*, id: int = None, name: str = None, mines: List[ForwardRef('OwnerAssociation')] = <factory>)

    ### Instance variables

    `id: int`
    :

    `mines: List[cmti_tools.tables.tables.OwnerAssociation]`
    :

    `name: str`
    :

`OwnerAssociation(*, is_current_owner=None, start_year=None, end_year=None, owner, mine)`
:   OwnerAssociation(*, is_current_owner: bool = None, start_year: int = None, end_year: int = None, owner: 'Owner', mine: 'Mine')

    ### Instance variables

    `end_year: int`
    :

    `is_current_owner: bool`
    :

    `mine: cmti_tools.tables.tables.Mine`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

    `owner: cmti_tools.tables.tables.Owner`
    :

    `owner_id: cmti_tools.tables.tables.Owner`
    :

    `start_year: int`
    :

`Reference(*, mine_id=None, source_id=None, source=None, link=None, mine)`
:   Reference(*, mine_id: 'Mine' = None, source_id: str = None, source: str = None, link: str = None, mine: 'Mine')

    ### Instance variables

    `id: int`
    :

    `link: str`
    :

    `mine: cmti_tools.tables.tables.Mine`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

    `source: str`
    :

    `source_id: str`
    :

`TailingsAssociation(*, mine_id, tsf_id, start_year=None, end_year=None)`
:   TailingsAssociation(*, mine_id: 'Mine', tsf_id: 'TailingsFacility', start_year: int = None, end_year: int = None)

    ### Instance variables

    `end_year: int`
    :

    `mine_id: cmti_tools.tables.tables.Mine`
    :

    `start_year: int`
    :

    `tsf_id: cmti_tools.tables.tables.TailingsFacility`
    :

`TailingsFacility(*, id=None, is_default=None, cmti_id='NULL', name='Unknown', status=None, hazard_class=None, latitude=None, longitude=None, mines=<factory>, impoundments=<factory>)`
:   TailingsFacility(*, id: int = None, is_default: bool = None, cmti_id: str = 'NULL', name: str = 'Unknown', status: str = None, hazard_class: str = None, latitude: float = None, longitude: float = None, mines: List[ForwardRef('Mine')] = <factory>, impoundments: List[ForwardRef('Impoundment')] = <factory>)

    ### Instance variables

    `cmti_id: str`
    :

    `hazard_class: str`
    :

    `id: int`
    :

    `impoundments: List[cmti_tools.tables.tables.Impoundment]`
    :

    `is_default: bool`
    :

    `latitude: float`
    :

    `longitude: float`
    :

    `mines: List[cmti_tools.tables.tables.Mine]`
    :

    `name: str`
    :

    `status: str`
    :