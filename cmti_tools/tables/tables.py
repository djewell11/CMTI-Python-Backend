from typing import List
from typing import Optional
from datetime import datetime
from pandas import NA, NaT

from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String, Float, Boolean, DateTime
from sqlalchemy import text
from sqlalchemy.orm import registry
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from dataclasses import dataclass, field

reg = registry() 

Base = reg.generate_base()
# class Base(DeclarativeBase):
#   pass

@reg.mapped
@dataclass(kw_only=True)
class Mine:
  __tablename__ = "mines"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  # Non-nullable (required values)
  id: int = field(init=False, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  cmdb_id: str = field(default="NULL", metadata={"sa": mapped_column(String, nullable=False)})
  name: str = field(metadata={"sa": mapped_column(String, nullable=False)})
  prov_terr: str = field(metadata = {"sa": mapped_column(String(2), nullable=False)})
  latitude: float = field(metadata={"sa": mapped_column(Float, nullable=False)})
  longitude: float = field(metadata={"sa": mapped_column(Float, nullable=False)})
  # Nullable (optional values)
  last_revised: datetime=field(default=NaT, metadata={"sa": mapped_column(DateTime, nullable=True)})
  nad: int = field(default=83, metadata={"sa": mapped_column(Integer, nullable=True)})
  utm_zone: int = field(default=NA, metadata={"sa": mapped_column(Integer, nullable=True)})
  easting: float = field(default=NA, metadata={"sa": mapped_column(Float, nullable=True)})
  northing: float = field(default=NA, metadata={"sa": mapped_column(Float, nullable=True)})
  nts_area: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  mining_district: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  mine_type: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  mine_status: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  mining_method: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  orebody_type: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  orebody_class: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  orebody_minerals: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  processing_method: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  ore_processed: float = field(default=NA, metadata={"sa": mapped_column(Float, nullable=True)})
  ore_processed_unit: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  development_stage: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  site_access: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  construction_year: int = field(default=NA, metadata={"sa": mapped_column(Integer, nullable=True)})
  year_opened: int  = field(default=NA, metadata={"sa": mapped_column(Integer, nullable=True)})
  year_closed: int = field(default=NA, metadata={"sa": mapped_column(Integer, nullable=True)})
  # Likely to be removed:
  ds_comments: Optional[str] = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  sa_comments: Optional[str] = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  shaft_depth: Optional[float] = field(default=NA, metadata={"sa": mapped_column(nullable=True)})
  reserves_resources: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  other_mineralization: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  sedar: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  notes: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  other_mineralization: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  forcing_features: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  feature_references: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  noami_status: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  noami_site_class: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  hazard_class: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  hazard_system: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  prp_rating: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  rehab_plan: Optional[bool] = field(default=None, metadata={"sa": mapped_column(server_default="False")})
  ews: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})
  ews_rating: Optional[str] = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=True)})

  # Relationships
  commodities: "CommodityRecord" = field(default=None, metadata={"sa": relationship("CommodityRecord", back_populates="mine", cascade="all, delete-orphan")})
  aliases: List["Alias"] = field(default_factory=list, metadata={"sa":relationship("Alias", back_populates="mine", cascade="all, delete-orphan")})
  owners: List["OwnerAssociation"] = field(default_factory=list, metadata={"sa": relationship(back_populates = "mine")})
  tailings_facilities: List["TailingsFacility"] = field(
    default_factory=list, 
    metadata={
      "sa": relationship(
        secondary = "tsf_mine_associations",
        back_populates = "mines"
      )
    }
  )
  orebody: "Orebody" = field(default=None, metadata={"sa": relationship("Orebody", back_populates="mine")})
  references: List["Reference"] = field(default_factory=list, metadata={"sa": relationship("Reference", back_populates="mine")})

  def __repr__(self) -> str:
    return f"Mine: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}"

@reg.mapped
@dataclass(kw_only=True)
class CommodityRecord:
  __tablename__ = "commodities"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(init=False, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  commodity: str = field(metadata={"sa": mapped_column(String, primary_key=True)})
  mine_id: "Mine" = field(init=False, metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  grade: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  grade_unit: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  produced: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  produced_unit: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  contained: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  contained_unit: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  metal_type: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True, server_default="Unknown")})
  is_critical: bool = field(default=None, metadata={"sa": mapped_column(Boolean, server_default="False")})
  source: str = field(default="Unknown", metadata={"sa": mapped_column(String, server_default="Unknown")})
  source_id: str = field(default="Unknown", metadata={"sa": mapped_column(String, server_default="Unknown")})
  source_year_start: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})
  source_year_end: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})

  mine: "Mine" = field(default=None, metadata={"sa": relationship("Mine", back_populates="commodities")})

  def __repr__(self) -> str:
    return f"CommodityRecord: {self.commodity!r}, ID: {self.id!r}, Mine Name: {self.mine.name!r}, mine_id: {self.mine_id}, Produced: {self.produced}"

@reg.mapped
@dataclass(kw_only=True)
class Alias:
  __tablename__ = "aliases"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  alias_id: int = field(default=None, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  mine_id: "Mine" = field(default=None, metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  alias: str = field(default=None, metadata={"sa": mapped_column(String, nullable=False, primary_key=True)})

  # mine = relationship("Mine", back_populates="aliases")
  mine: "Mine" = field(default=None, metadata={"sa": relationship("Mine", back_populates="aliases")})

  def __repr__(self) -> str:
    return f"Alias: {self.alias!r}, Mine Name: {self.mine.name!r}, mine_id: {self.mine_id}"

@reg.mapped
@dataclass(kw_only=True)
class Owner:
  __tablename__ = "owners"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(default=None, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  name: str = field(default=None, metadata={"sa": mapped_column(String, nullable=False)})

  mines: List["OwnerAssociation"] = field(default_factory=list, metadata={"sa": relationship(back_populates = "owner")})

  def __repr__(self) -> str:
    return f"Owner: {self.name!r}, ID: {self.id!r}, Mines: {self.mines}"

@reg.mapped
@dataclass(kw_only=True)
class TailingsFacility:
  __tablename__ = "tailings_facilities"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(default=None, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  is_default: bool = field(default=None, metadata={"sa": mapped_column(Boolean, nullable=False, default=False)})
  cmdb_id: str = field(default="NULL", metadata={"sa": mapped_column(String, nullable=False)})
  name: str = field(default="Unknown", metadata={"sa": mapped_column(String, nullable=False)})
  status: str = field(default=None, metadata={"sa": mapped_column(String, server_default="Unknown", nullable=True)})
  hazard_class: str = field(default=None, metadata={"sa": mapped_column(String, server_default="Unknown", nullable=True)})
  # Coordinates are optional for TSFs, will likely be listed as the same as their parent mine
  latitude: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  longitude: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  mines: List["Mine"] = field(default_factory=list, metadata={
    "sa": relationship(
      secondary = "tsf_mine_associations",
      back_populates = "tailings_facilities")
    }
  )
  impoundments: List["Impoundment"] = field(default_factory=list, metadata={
    "sa": relationship("Impoundment", back_populates = "parentTsf", cascade = "all, delete-orphan")
    }
  )
  
  def __repr__(self) -> str:
    return f"TailingsFacility: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}, isDefault: {self.is_default}"

@reg.mapped
@dataclass(kw_only=True)
class Impoundment:
  __tablename__ = "impoundments"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(init=False, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  is_default: bool = field(default=None, metadata={"sa": mapped_column(Boolean, nullable=False, default=False)})
  cmdb_id: str = field(default="NULL", metadata={"sa": mapped_column(String, nullable=False)})
  parent_tsf_id: "TailingsFacility" = field(metadata={"sa":  mapped_column(ForeignKey("tailings_facilities.id"))})
  name: str = field(default=None, metadata={"sa": mapped_column(String, nullable=False)})
  area: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  area_from_images: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  area_notes: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  raise_type: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  capacity: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  volume: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  acid_generating: bool = field(default=None, metadata={"sa": mapped_column(Boolean, nullable=True)}) # TODO: Make this a bool - DONE but not tested
  storage_method: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  max_height: float = field(default=None, metadata={"sa": mapped_column(Float, nullable=True)})
  treatment: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  rating_index: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  stability_concerns: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})

  parentTsf: "TailingsFacility" = field(metadata={"sa": relationship("TailingsFacility", back_populates="impoundments")})

  def __repr__(self) -> str:
    return f"Impoundment: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}"


@reg.mapped
@dataclass(kw_only=True)
class Orebody:
  __tablename__ = "orebodies"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(init=False, metadata={"sa": mapped_column(Integer, primary_key=True, autoincrement=True)})
  mine_id: "Mine" = field(default=None, metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  ore_type: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  ore_class: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  minerals: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True)})
  ore_processed: float = field(default=None, metadata={"sa": mapped_column(nullable=True)})

  mine: "Mine" = field(metadata={"sa": relationship("Mine", back_populates="orebody")})

  def __repr__(self) -> str:
    return f"Orebody: {self.ore_type!r}, ID: {self.id!r}, mineral: {self.mineral}, Mine Name: {self.mine.name}, mine_id: {self.mine_id}"

@reg.mapped
@dataclass(kw_only=True)
class Reference:
  __tablename__ = "references"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  id: int = field(init=False, metadata={"sa": mapped_column(primary_key=True, autoincrement=True)})
  mine_id: "Mine" = field(default=None, metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  source_id: str = field(default=None, metadata={"sa": mapped_column(String, nullable=False, primary_key=True)})
  source: str = field(default=None, metadata={"sa": mapped_column(String, nullable=False)})
  link: str = field(default=None, metadata={"sa": mapped_column(String, nullable=True, server_default="Unknown")})

  mine: "Mine" = field(metadata={"sa": relationship("Mine", back_populates="references")})

  def __repr__(self):
    return f"Reference: {self.source}, Source_ID: {self.source_id}, Mine Name: {self.mine.name}, mine_id: {self.mine_id}"

@reg.mapped
@dataclass(kw_only=True)
class TailingsAssociation:
  __tablename__ = "tsf_mine_associations"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  mine_id: "Mine" = field(metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  tsf_id: "TailingsFacility" = field(metadata={"sa": mapped_column(ForeignKey("tailings_facilities.id"), primary_key=True)})
  start_year: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})
  end_year: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})

  def __repr__(self) -> str:
    return f"tsf_id: {self.tsf_id!r}, mine_id: {self.mine_id}"

@reg.mapped
@dataclass(kw_only=True)
class OwnerAssociation:
  __tablename__ = "owner_associations"
  __allow_unmapped__ = True # dataclasses process fields before SQLAlchemy, so we need to set this to True to allow the dataclass to be mapped
  __sa_dataclass_metadata_key__ = "sa" # This is used by the dataclass fields to identify the SQLAlchemy mapped fields

  owner_id: "Owner" = field(init=False, metadata={"sa": mapped_column(ForeignKey("owners.id"), primary_key=True)})
  mine_id: "Mine" = field(init= False, default=None, metadata={"sa": mapped_column(ForeignKey("mines.id"), primary_key=True)})
  is_current_owner: bool = field(default=None, metadata={"sa": mapped_column(Boolean, nullable=False, default=False)})
  start_year: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})
  end_year: int = field(default=None, metadata={"sa": mapped_column(Integer, nullable=True)})

  owner: "Owner" = field(metadata={"sa": relationship("Owner", back_populates="mines")})
  mine: "Mine" = field(metadata={"sa": relationship("Mine", back_populates="owners")})

  def __repr__(self) -> str:
    return f"Owner ID: {self.owner_id}, mine_id: {self.mine_id}"