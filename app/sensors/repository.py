from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
import json

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongo: MongoDBClient , sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    mdatab= mongo
    mdatab.getDatabase("DB1")
    mcolect= mdatab.getCollection(sensor.type)
    mcolect.insert_one(json.loads(sensor.json()))
    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: dict) -> schemas.Sensor:
    db_sensordata = json.dumps(data)
    return redis.set(sensor_id,db_sensordata)

def get_data(redis: RedisClient, sensor_id: int) -> schemas.Sensor:
    db_sensordata = redis.get(sensor_id)
    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor
# We use the mongdb querys to do this method
def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    near = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
             "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    sensors = mongodb.collection.find(query)  # Do a query for the sensors in a given radius.
    for sensor in sensors:  # Traverse for every sensor in the doc.
        db_sensor = get_sensor_by_name(db, sensor['name'])
        db_sensor_data = get_data(redis, db_sensor.id)  # Use the sensor id instead of the sensor object

        near.append(db_sensor_data)
    return near