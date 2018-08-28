'''
Created on 2018年8月28日

@author: Leo
'''
#内建模块
import sys
import logging

#第三方模块
import asyncio
import aiomysql

def log(sql, args=()):
    logging.info('%s -> SQL: %s' % (sys._getframe().f_back.f_code.co_name,sql))

   
async def create_pool(loop, **kw):
    '''
          创建一个全局的连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。
          使用连接池的好处是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用
    '''
    logging.info('Create database connection pool...') 
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
        )

async def select(sql, args, size = None):
    log(sql, args)
    
    global __pool
    with await __pool as connect:
        cursor = await connect.cursor(aiomysql.DictCursor)
        await cursor.exec(sql.replace('?', '%s'), args or ())
        if size:
            #获取最多指定数量的记录
            result = await cursor.fetchmany(size)
        else:
            #获取所有记录
            result = await cursor.fetchall()
        await cursor.close()
        
        logging.info('rows returned: %s' % len(result))
        return result

async def execute(sql, args):
    '''
          执行INSERT、UPDATE、DELETE语,因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数
    '''
    log(sql, args)
    
    with await __pool as connect:
        try:
            cursor = await connect.cursor()
            await cursor.exec(sql.replace('?', '%s'), args)
            #返回影响的行数
            affect_count = cursor.rowcount()  
            await cursor.close()     
        except BaseException as e:
            raise
                  
        return  affect_count       
    
if __name__ == '__main__':
    pass