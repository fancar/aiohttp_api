#!venv/bin/python3.6
#import asyncio
import json
from aiohttp import web
from aiojobs.aiohttp import atomic
from aiohttp_session import get_session # setup,  session_middleware
from config import *
from models import *
#import uvloop


routes = web.RouteTableDef()

@routes.get('/')
async def main_page(request):
    return web.Response(text="This thing recieves info about IoT smart-sockets and stores into ZaBBiX DB")

@atomic
@routes.post('/data')
async def data(request):
    if request.body_exists and request.content_type == 'application/json':
        try:
            data = await request.json()
        except json.decoder.JSONDecodeError:
            raise web.HTTPBadRequest(text='bad or empty json\n')
        print("data recieved:",data)
        #await write_into_db(data)
        async with request.app['db'].acquire() as conn:
            result,err = await save_json(conn,data) #save data into db
            if result:
                raise web.HTTPAccepted(text='json recieved\n')
            else:
                raise web.HTTPBadRequest(text=err+'\n')
    else:
        raise web.HTTPForbidden(text='only json allowed\n')


async def write_into_db(data):
    await asyncio.sleep(0.5)  # do it in background

async def init(loop):
    app = web.Application(loop=loop)    

    app['config'] = config

    # create connection to the database
    app.on_startup.append(init_db)
    # shutdown db connection on exit
    app.on_cleanup.append(close_db)    # setup Jinja2 template renderer

    # print(jinja2.FileSystemLoader('./templates').list_templates())
    # aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))

    # app['db'] = await aiomysql.connect(
    #   host=DB_HOST, port=DB_PORT,
    #   user=DB_USER, password=DB_PWD, db=DB_NAME,
    #   loop=loop)    
    app.add_routes(routes)

    return app


def main():
    #app = web.Application()
    #asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    
    app = loop.run_until_complete(init(loop))
    
    web.run_app(app, host=app['config']['host'], port=app['config']['port'])

if __name__ == '__main__':
    main()
