

import plyvel

db = None


class tType():
    def _py_value(self, value):
        if value[0] == int.from_bytes(b's', byteorder='little'):
            value = value[1:].decode('utf-8')
        elif value[0] == int.from_bytes(b'i', byteorder='little'):
            value = int.from_bytes(value[1:], byteorder='little', signed=True)
        return value

    def _byte_value(self, value):
        if type(value) is str:
            value = b's' + value.encode('utf-8')
        elif type(value) is int:
            value = b'i' + value.to_bytes((value.bit_length() + 7) // 8 + 1, byteorder='little', signed=True)
        else:
            raise ValueError(value)
        return value

    def _wrap_key(self, key):
        return (self.name + '_' + key).encode('utf-8')


class tList(tType):
    def __init__(self, name, _list=None):
        print('__init__')
        self.name = name
        if _list is None:
            return
        count = self._get_count()
        if count != 0 and _list is not None:
            raise TypeError(_list)
        index = count - 1
        with db.write_batch(transaction=True) as wb:
            for item in _list:
                if item is None:
                    raise ValueError(item)
                index += 1
                key = self._wrap_key(str(index))
                wb.put(key, self._byte_value(item))
            self._set_count(count + len(_list), wb)

    def __len__(self):
        print('__init__')
        return self._get_count()

    def __getitem__(self, index):
        print('__getitem__')
        if type(index) is int:
            key = self._wrap_key(str(index))
            value = db.get(key)
            if value is None:
                raise IndexError(index)
            else:
                return self._py_value(value)
        else:
            raise TypeError(index)

    def __setitem__(self, index, value):
        print('__setitem__')
        count = self._get_count()
        if type(index) is int:
            if index < count:
                key = self._wrap_key(str(index))
                value = self._byte_value(value)
                db.put(key, value)
            else:
                raise IndexError(index)
        else:
            raise TypeError(index)

    def pop(self):
        count = self._get_count()
        last_index = self._wrap_key(str(count - 1))
        value = db.get(last_index)
        with db.write_batch(transaction=True) as wb:
            wb.delete(self._wrap_key(str(count - 1)))
            self._set_count(count - 1, wb)
        return self._py_value(value)

    def append(self, value):
        count = self._get_count()
        index = count
        with db.write_batch(transaction=True) as wb:
            wb.put(self._wrap_key(str(index)), self._byte_value(value))
            self._set_count(count + 1)

    def _get_count(self):
        key = self.name + '_' + 'count'
        value = db.get(key.encode('utf-8'))
        if value is None:
            return 0
        return int.from_bytes(value, byteorder='little', signed=True)

    def _set_count(self, value, wb=None):
        key = self.name + '_' + 'count'
        _t = db
        if wb is not None:
            _t = wb
        _t.put(key.encode('utf-8'), value.to_bytes((value.bit_length() + 7) // 8 + 1, byteorder='little', signed=True))


class tDict(tType):
    def __init__(self, name, _dict=None):
        print('__init__')
        self.name = name
        if _dict is None:
            return
        tile_dict = self._dict_tile(self.name, _dict)
        with db.write_batch(transaction=True) as wb:
            for k, v in tile_dict.items():
                wb.put(k.encode('utf-8'), v)

    def _dict_tile(self, prefix_key, _dict):
        rtv = {}
        print(type(_dict))
        for k, v in _dict.items():
            if type(v) is dict:
                rtv[prefix_key + '_' + k] = b'd'
                rtv.update(self._dict_tile(prefix_key + '_' + k, v))
            else:
                rtv[prefix_key + '_' + k] = self._byte_value(v)
        return rtv

    def __setitem__(self, key, value):
        print('__setitem__')
        key = self._wrap_key(key)
        value = self._byte_value(value)
        db.put(key, value)

    def __getitem__(self, key):
        print('__getitem__')
        key = self._wrap_key(key)
        value = db.get(key)
        if value is None:
            raise KeyError(key)
        else:
            if value == b'd':
                return tDict(key.decode('utf-8'))
            else:
                return self._py_value(value)

    def __contains__(self, key):
        print('__contains__')
        key = self._wrap_key(key)
        value = db.get(key)
        return value != None

    def pop(self, key, d=None):
        print('pop')
        key = self._wrap_key(key)
        value = db.get(key)
        if value is None:
            if d is None:
                raise KeyError(key)
            else:
                return d
        else:
            value = self._py_value(value)
            db.delete(key)
            return value


if __name__ == '__main__':

    db = plyvel.DB('./testleveldb/', create_if_missing=True)

    with db.write_batch() as wb:
        for key, value in db:
            wb.delete(key)

    test_data = tList("test_list", [1, 2, 3, 4, 5, 6, 7])

    for key, value in db:
        print(key, value)

    db.close()
