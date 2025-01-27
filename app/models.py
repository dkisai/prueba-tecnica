from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base

class Alcaldia(Base):
    __tablename__ = "alcaldias"
    alcaldia_id = Column(Integer, primary_key=True, index=True)
    alcaldia_nombre = Column(String, unique=True, nullable=False)

class Stop(Base):
    __tablename__ = "stops"
    stop_id = Column(Integer, primary_key=True, index=True)
    stop_name = Column(String, nullable=False)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    alcaldia_id = Column(Integer, ForeignKey("alcaldias.alcaldia_id"), nullable=False)
    alcaldia = relationship("Alcaldia")

class VehicleStatus(Base):
    __tablename__ = "vehicles_status"
    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(String, unique=True, nullable=False)
    route_id = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    alcaldia = Column(String)