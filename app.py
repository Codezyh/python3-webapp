# -*- coding: utf-8 -*-
'''
Created on 2018年8月14日

@author: Leo
'''
#内建模块
import logging; logging.basicConfig(level = logging.INFO)
import asyncio

#第三方模块
from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Hello world</h1>', content_type = 'text/html')

async def init(loop):
    app = web.Application(loop = loop)
    app.router.add_route('GET', '/', index)
    srv = await loop.create_server(app.make_handler(),'127.0.0.1',9000)
    print('server started at http://127.0.0.1:9000...')
    return srv

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()
    