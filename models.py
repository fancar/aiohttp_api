#!venv/bin/python3.6
#from config import *
import asyncio
import sys
from datetime import datetime
#import aiomysql
import aiomysql.sa
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable,DropTable
#from sqlalchemy.dialects import mysql


import argparse
from trafaret_config import commandline
from utils import TRAFARET
ap = argparse.ArgumentParser()
commandline.standard_argparse_options(ap, default_config='./config/main_config.yaml')
options = ap.parse_args(sys.argv[1:])
config = commandline.config_from_options(options, TRAFARET)
#__all__ = [table_name]

meta = sa.MetaData()
logs_table_name = 'smartsocket'

#{"DevEUI_uplink": {"Time": "2019-08-15T08:31:48.478+00:00","DevEUI": "001BC503500000F5","FPort": 2,"FCntUp": 17449,"ADRbit": 1,"MType": 2,"FCntDn": 1,"payload_hex": "2f0e10","mic_hex": "a962f3cd","Lrcid": "00000241","LrrRSSI": -103.000000,"LrrSNR": 2.000000,"SpFact": 12,"SubBand": "G1","Channel": "LC2","DevLrrCnt": 8,"Lrrid": "00000509","Late": 0,"LrrLAT": 55.752792,"LrrLON": 37.646118,"Lrrs": {"Lrr": [{"Lrrid": "00000509","Chain": 0,"LrrRSSI": -103.000000,"LrrSNR": 2.000000,"LrrESP": -105.124428},{"Lrrid": "000038F1","Chain": 0,"LrrRSSI": -98.000000,"LrrSNR": -2.000000,"LrrESP": -102.124428},{"Lrrid": "0000059C","Chain": 0,"LrrRSSI": -112.000000,"LrrSNR": -4.000000,"LrrESP": -117.455406}]},"CustomerID": "1100000033","CustomerData": {"alr":{"pro":"LORA/Generic","ver":"1"}},"ModelCfg": "0","InstantPER": 0.000000,"MeanPER": 0.000000,"DevAddr": "0440F634","TxPower": 16.000000,"NbTrans": 1}}

table_logs = sa.Table(
    logs_table_name, meta,
    sa.Column('id', sa.BigInteger, primary_key=True), # default=1, 
    sa.Column('deveui', sa.String(20)),
    sa.Column('status', sa.Boolean()),
    sa.Column('payload_hex', sa.String(100)), # , nullable=False
    sa.Column('raw', sa.String(10000)),
    sa.Column('created_at', sa.DateTime, index=True, default=datetime.utcnow)
)


async def save_json(cur,data):
    rows = {}
    rows["raw"] = str(data)

    try:
        data = data["DevEUI_uplink"]
        rows["deveui"] = d =  data["DevEUI"]
        rows["payload_hex"] = ph = data["payload_hex"]
    except Exception as e:    
        err = 'Error: bad json. Unknown: '+str(e)
        print(err)
        return False, err

    status = False
    x = int(ph[-2:], 16)

    if x > 7:
        if bin(x)[-5] == '1' or bin(x)[-4] =='1':
            status = True

    rows["status"] = status

    print(f"""
        data to save in db:
        "deveui" : {d}
        "payload_hex" : {ph}
        "Switch status" : {status}
        """)

    result = await insert_to_db(cur,table_logs,rows)
    return True, result

async def insert_to_db(cur,table,kwargs):
    sql = table.insert().values(**kwargs)
    #print("QUERY:",sql)
    
    result = await cur.execute(sql)
    return result

async def init_tables(loop,argv):
    # execute bu this file as main
    conf = config['mysql']
    conn = await aiomysql.connect(
      host=conf['host'], port=conf['port'],
      user=conf['user'], password=conf['password'], db=conf['database'],
      loop=loop)

    sql_remove = f'DROP TABLE IF EXISTS {logs_table_name};'
    sql_create = str(CreateTable(table_logs))
    #sql_drop = str(DropTable(table_logs))
    incr = f'ALTER TABLE `{logs_table_name}` CHANGE `id` `id` BIGINT(0) NOT NULL AUTO_INCREMENT'
    # ALTER TABLE `smartsocket` CHANGE `id` `id` BIGINT(0) NOT NULL AUTO_INCREMENT
    #print("sql_create: ",sql_create)

    async with conn.cursor() as cur: #aiomysql.cursors.DeserializationCursor, aiomysql.cursors.DictCursor

        #await table_logs.drop(cur)
        await cur.execute(sql_remove)
        await cur.execute(sql_create)
        await cur.execute(incr)
        
        #r = await cur.fetchall()
        #print(r)
    conn.close()


async def init_db(app):
    # init db when app starts
    conf = app['config']['mysql']
    engine = await aiomysql.sa.create_engine(
        db=conf['database'],
        user=conf['user'],
        password=conf['password'],
        host=conf['host'],
        port=conf['port'],
        minsize=conf['minsize'],
        maxsize=conf['maxsize'],
        autocommit = True,
        loop=app.loop)
    app['db'] = engine


async def close_db(app):
    # disconnect db when app stoped
    app['db'].close()
    await app['db'].wait_closed()    


if __name__ == "__main__":
    print('Эта штука пересоздаст таблицы. Уверены?')
    if input('y - да, n - нет:') == 'y'.lower():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(init_tables(loop,sys.argv[1:]))