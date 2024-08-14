from apscheduler.schedulers.blocking import BlockingScheduler
from typing import List
import psycopg2
from datetime import datetime, timedelta
from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
import time


class SQLAlchemyJobStoreExtend(SQLAlchemyJobStore):
    pass

jobstores = {
    'default': SQLAlchemyJobStoreExtend(url='postgresql+psycopg2://postgres:X1n!28hBas3EbEVx@localhost:5432/test')
}
executors = {
    'default': ThreadPoolExecutor(2),
    'processpool': ProcessPoolExecutor(2)
}
job_defaults = {
    'coalesce': True,
    'max_instances': 2
}
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

def connect():
    connect = psycopg2.connect(
        dbname='test',
        user='postgres',
        password='X1n!28hBas3EbEVx',
        host='localhost',
        port=5432
    )
    return connect

def save_job(index: int, status: str):
    try:
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO tasks (stop_index, status) VALUES (%s, %s) RETURNING id", (index, status))
                job_id = cursor.fetchone()[0]
            conn.commit()
            return job_id
    except Exception as e:
        print(f"Ошибка при сохранении задачи: {e}")


def update_job(id: int, status: str, index: int = 0):
    with connect() as conn:
        with conn.cursor() as cursor:
            if index is not None:
                cursor.execute(
                    "UPDATE tasks SET stop_index = %s, status = %s WHERE id = %s",
                    (index, status, id)
                )
            else:
                cursor.execute(
                    "UPDATE tasks SET status = %s WHERE id = %s",
                    (status, id)
                )
            conn.commit()


def restore_tasks():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT id, stop_index FROM tasks WHERE status = %s
            ''', ('wait',))   # done, wait, stop
            rows = cursor.fetchall()
            for row in rows:
                id, stop_index = row
                scheduler.add_job(double_int_task, args=([1, 2, 3, 4, 5, 6, 7, 8, 9], stop_index, id), trigger='date', run_date=datetime.now() + timedelta(seconds=5))


def start_tasks():
    # restore_tasks()
    # scheduler.add_job(save_int_job, args=[2])
    # scheduler.add_job(double_int_task, args=[[15,2,3]])
    scheduler.add_job(test_func)


def double_int_task(int_pack: List[int], stop_index: int = 0, rest_job_id: int = 0):
    try:
        # raise Exception('lalala')
        if stop_index is None:
            start_index = 0
        else:
            start_index = stop_index

        for index, num in enumerate(int_pack[start_index:], start=start_index):
            # if not rest_job_id:
            #     job_id = save_job(index, 'wait')
            # else:
            #     job_id = rest_job_id
            scheduler.add_job(save_int_job, args=[num ** 2])
    except Exception as e:
        # update_job(job_id, 'wait', index)
        print("Вылет:", e)


def save_int_job(num: int):
    try:
        time.sleep(5)
        # raise Exception('Пробуем послать ошибку')
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO numbers (number) VALUES (%s)", (num,))
                conn.commit()
        # update_job(job_id, 'done')
        print("Задача выполнена!")
    except Exception as e:
        print("Задача не была выполнена:", e)

def test_func():
    print('done')
    time.sleep(10)

async def main():
    scheduler.start(paused=False)
    start_tasks()
    await asyncio.Event().wait()

asyncio.run(main())

if __name__ == "__main__":
    # start_tasks()
    pass
