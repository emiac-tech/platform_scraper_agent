from sqlalchemy import Column, Integer, String, Float, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class PublisherListing(Base):
    __tablename__ = 'publishers_v2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    clean_domain = Column(String(255), unique=True, index=True)
    website_url = Column(String(255))
    host_sites = Column(JSON, default=list) # e.g. ["posticy.com", "icopify.co"]
    item_ids = Column(JSON, default=list)
    categories = Column(JSON, default=list)
    prices_raw = Column(JSON, default=list)
    prices_numerical = Column(JSON, default=list)
    
    moz_da = Column(Integer)
    moz_pa = Column(Integer)
    ahrefs_dr = Column(Integer)
    traffic = Column(Integer)
    
    language = Column(String(100))
    country = Column(String(100))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
