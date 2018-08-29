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

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
        
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type='varchar(100)'):
        super().__init__(name, column_type, primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None,primary_key=False,default=False, column_type='boolean'):
        super().__init__(name, column_type, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0, column_type = 'bigint'):
        super().__init__(name, column_type, primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0, column_type = 'real'):
        super().__init__(name, column_type, primary_key, default)

class TextField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type = 'text'):
        super().__init__(name, column_type, primary_key, default)
    
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        
        #获取table名称
        table_name = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, table_name))
        mappings = dict()
        fields = []
        primary_key_new = None
        
        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primary_key_new:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primary_key_new = k
                    
                else:
                    fields.append(k)
                    
        if not primary_key_new:
            raise RuntimeError('Primary key not found.')
        
        for k in mappings.keys():
            attrs.pop(k)
        
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        attrs['__table__'] == table_name
        # 主键属性名
        attrs['__primary_key__'] = primary_key_new 
        # 除主键外的属性名
        attrs['__fields__'] = fields 
        # 构造数据库操作语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primary_key_new, ', '.join(escaped_fields), table_name)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (table_name, ', '.join(escaped_fields), primary_key_new, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (table_name, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primary_key_new)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (table_name, primary_key_new)
        
        return type.__new__(cls, name, bases, attrs)  
                  
class Model(dict, metaclass = ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
        
    def __getaddr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
        
    def __setattr__(self, key, value):
        self[key] = value
    
    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
    
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]
    
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']
    
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
    
    async def save(self):
        args = list(map(self.get_value_or_default, self.__fields__))
        args.append(self.get_value_or_default(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)
    
    async def update(self):
        args = list(map(self.get_value, self.__fields__))
        args.append(self.get_value(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
    
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)


if __name__ == '__main__':
    class User(Model):
        __table__ = 'users'

        id = IntegerField(primary_key=True)
        name = StringField()
    # 创建实例:
    user = User(id=1, name='leo')
    # 查询所有User对象:
    users = User.findAll()