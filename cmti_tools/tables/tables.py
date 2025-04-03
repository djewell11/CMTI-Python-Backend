from typing import List
from typing import Optional
from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
  pass

class Mine(Base):
  __tablename__ = "mines"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  cmdb_id: Mapped[str] = mapped_column(nullable=False)
  name: Mapped[str]
  prov_terr: Mapped[str] = mapped_column(String(2), nullable=False)
  last_revised: Mapped[datetime] = mapped_column(nullable=True)
  nad: Mapped[int] = mapped_column(server_default=text("83"))
  utm_zone: Mapped[Optional[int]]
  easting: Mapped[Optional[float]]
  northing: Mapped[Optional[float]]
  latitude: Mapped[Optional[float]]
  longitude: Mapped[Optional[float]]
  nts_area: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  mining_district: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  mine_type: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  mine_status: Mapped[Optional[str]] = mapped_column(server_default = "Unknown")
  mining_method: Mapped[Optional[str]] = mapped_column(server_default = "Unknown")
  orebody_type: Mapped[str] = mapped_column(nullable=True)
  orebody_class: Mapped[str] = mapped_column(nullable=True)
  orebody_minerals: Mapped[str] = mapped_column(nullable=True)
  processing_method: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  ore_processed: Mapped[float] = mapped_column(nullable=True)
  ore_processed_unit: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  development_stage: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  site_access: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  construction_year: Mapped[Optional[int]]
  year_opened: Mapped[Optional[int]]
  year_closed: Mapped[Optional[int]]
  # Likely to be removed:
  ds_comments: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  sa_comments: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  shaft_depth: Mapped[Optional[float]]
  reserves_resources: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  sedar: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  notes: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  other_mineralization: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  forcing_features: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  feature_references: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  noami_status: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  noami_site_class: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  hazard_class: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  hazard_system: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  prp_rating: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  rehab_plan: Mapped[Optional[bool]] = mapped_column(server_default="False")
  ews: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  ews_rating: Mapped[Optional[str]] = mapped_column(server_default="Unknown")

  # Relationships
  commodities = relationship("CommodityRecord", back_populates="mine", cascade="all, delete-orphan")
  aliases: Mapped[List["Alias"]] = relationship("Alias", back_populates="mine", cascade="all, delete-orphan")
  owners: Mapped[List["OwnerAssociation"]] = relationship(back_populates = "mines")
  tailings_facilities: Mapped[List["TailingsFacility"]] = relationship(
      secondary = "tsf_mine_associations",
      back_populates = "mines"
    )
  orebody = relationship("Orebody", back_populates="mine")
  references: Mapped[List["Reference"]] = relationship("Reference", back_populates="mine")

  def __repr__(self) -> str:
    return f"Mine: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}"

class CommodityRecord(Base):
  __tablename__ = "commodities"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  commodity: Mapped[str] = mapped_column(primary_key=True)
  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  grade: Mapped[str] = mapped_column(nullable=True)
  grade_unit: Mapped[str] = mapped_column(nullable=True)
  produced: Mapped[str] = mapped_column(nullable=True)
  produced_unit: Mapped[str] = mapped_column(nullable=True)
  contained: Mapped[str] = mapped_column(nullable=True)
  contained_unit: Mapped[str] = mapped_column(nullable=True)
  metal_type: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  is_critical: Mapped[Optional[bool]]
  source: Mapped [Optional[str]] = mapped_column(server_default="Unknown")
  source_id: Mapped [Optional[str]] = mapped_column(server_default="Unknown")
  source_year_start: Mapped [Optional[int]]
  source_year_end: Mapped [Optional[int]]

  mine = relationship("Mine", back_populates="commodities")

  def __repr__(self) -> str:
    return f"CommodityRecord: {self.commodity!r}, ID: {self.id!r}, Mine Name: {self.mine.name!r}, mine_id: {self.mine_id}, Produced: {self.produced}"

class Alias(Base):
  __tablename__ = "aliases"

  alias_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  alias: Mapped[str] = mapped_column(nullable=False, primary_key=True)

  mine = relationship("Mine", back_populates="aliases")

  def __repr__(self) -> str:
    return f"Alias: {self.alias!r}, Mine Name: {self.mine.name!r}, mine_id: {self.mine_id}"

class Owner(Base):
  __tablename__ = "owners"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  name: Mapped[str] = mapped_column(nullable=False)

  mines: Mapped[List["OwnerAssociation"]] = relationship(back_populates = "owners")

  def __repr__(self) -> str:
    return f"Owner: {self.name!r}, ID: {self.id!r}, Mines: {self.mines}"

class TailingsFacility(Base):
  __tablename__ = "tailings_facilities"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  is_default: Mapped[bool] = mapped_column(nullable=False, default=False)
  cmdb_id: Mapped[Optional[str]] 
  name: Mapped[Optional[str]]
  status: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  hazard_class: Mapped[Optional[str]] = mapped_column(server_default="Unknown")
  # Coordinates are optional for TSFs, will likely be listed as the same as their parent mine
  latitude: Mapped[Optional[float]]
  longitude: Mapped[Optional[float]]
  mines: Mapped[List["Mine"]] = relationship(
    secondary = "tsf_mine_associations",
    back_populates = "tailings_facilities")
  impoundments: Mapped[List["Impoundment"]] = relationship("Impoundment", back_populates = "parentTsf",
                                                           cascade = "all, delete-orphan")
  
  def __repr__(self) -> str:
    return f"TailingsFacility: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}, isDefault: {self.is_default}"

class Impoundment(Base):
  __tablename__ = "impoundments"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  is_default: Mapped[bool] = mapped_column(nullable=False, default=False)
  cmdb_id: Mapped[Optional[str]]
  parent_tsf_id: Mapped["TailingsFacility"] = mapped_column(ForeignKey("tailings_facilities.id"))
  name: Mapped[str] = mapped_column(nullable=False)
  area: Mapped[float] = mapped_column(nullable=True)
  area_from_images: Mapped[float] = mapped_column(nullable=True)
  area_notes: Mapped[str] = mapped_column(nullable=True)
  raise_type: Mapped[str] = mapped_column(nullable=True)
  capacity: Mapped[float] = mapped_column(nullable=True)
  volume: Mapped[float] = mapped_column(nullable=True)
  acid_generating: Mapped[str] = mapped_column(nullable=True) # TODO: Make this a bool
  storage_method: Mapped[str] = mapped_column(nullable=True)
  max_height: Mapped[float] = mapped_column(nullable=True)
  treatment: Mapped[str] = mapped_column(nullable=True)
  rating_index: Mapped[str] = mapped_column(nullable=True)
  stability_concerns: Mapped[str] = mapped_column(nullable=True)

  parentTsf = relationship("TailingsFacility", back_populates="impoundments")

  def __repr__(self) -> str:
    return f"Impoundment: {self.name!r}, ID: {self.id!r}, cmdb_id: {self.cmdb_id}"

class Orebody(Base):
  __tablename__ = "orebodies"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  ore_type: Mapped[str] = mapped_column(nullable=True)
  ore_class: Mapped[str] = mapped_column(nullable=True)
  minerals: Mapped[str] = mapped_column(nullable=True)
  ore_processed: Mapped[float] = mapped_column(nullable=True)

  mine = relationship("Mine", back_populates="orebody")

  def __repr__(self) -> str:
    return f"Orebody: {self.ore_type!r}, ID: {self.id!r}, mineral: {self.mineral}, Mine Name: {self.mine.name}, mine_id: {self.mine_id}"

class Reference(Base):
  __tablename__ = "references"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  source_id: Mapped[str] = mapped_column(nullable=False, primary_key=True)
  source: Mapped[str] = mapped_column(nullable=False)
  link: Mapped[str] = mapped_column(nullable=True, server_default="Unknown")

  mine = relationship("Mine", back_populates="references")

  def __repr__(self):
    return f"Reference: {self.source}, Source_ID: {self.source_id}, Mine Name: {self.mine.name}, mine_id: {self.mine_id}"

class TailingsAssociation(Base):
  __tablename__ = "tsf_mine_associations"

  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  tsf_id: Mapped["TailingsFacility"] = mapped_column(ForeignKey("tailings_facilities.id"), primary_key=True)
  start_year: Mapped[Optional[int]]
  end_year: Mapped[Optional[int]]

  def __repr__(self) -> str:
    return f"tsf_id: {self.tsf_id!r}, mine_id: {self.mine_id}"

class OwnerAssociation(Base):
  __tablename__ = "owner_associations"

  owner_id: Mapped["Owner"] = mapped_column(ForeignKey("owners.id"), primary_key=True)
  mine_id: Mapped["Mine"] = mapped_column(ForeignKey("mines.id"), primary_key=True)
  is_current_owner: Mapped[bool] = mapped_column(nullable=False, default=False)
  start_year: Mapped[Optional[int]]
  end_year: Mapped[Optional[int]]

  owner: Mapped["Owner"] = relationship("Owner", back_populates="mines")
  mine: Mapped["Mine"] = relationship("Mine", back_populates="owners")

  def __repr__(self) -> str:
    return f"Owner ID: {self.owner_id}, mine_id: {self.mine_id}"