from __future__ import annotations
import asyncio

import logging

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import Scheduler
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

logging.basicConfig(level=logging.INFO)
engine = create_async_engine("postgresql+psycopg2://postgres:X1n!28hBas3EbEVx@localhost:5432/test")
data_store = SQLAlchemyDataStore(engine)
event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)

# Uncomment the next two lines to use the MQTT event broker instead
# from apscheduler.eventbrokers.mqtt import MQTTEventBroker
# event_broker = MQTTEventBroker()

with Scheduler(data_store, event_broker) as scheduler:
    scheduler.run_until_stopped()

asyncio.run(main())