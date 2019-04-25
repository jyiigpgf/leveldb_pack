

import plyvel

db = None

DICT_FORMAT_C = b'd'
STR_FORMAT_C = b's'
INT_FORMAT_C = b'i'
LIST_FORMAT_C = b'l'
BOOL_FORMAT_C = b'b'
NONE_FORMAT_C = b'n'


class _TType:
    def __init__(self, name):
        if type(name) is str:
            self.name = name
        elif type(name) is bytes:
            self.name = name.decode('utf-8')
        else:
            raise TypeError(name)

    def _to_py_value(self, db_key, db_value):
        if db_value == DICT_FORMAT_C:
            return TDict(db_key)
        elif db_value == LIST_FORMAT_C:
            return TList(db_key)
        else:
            return self._py_value(db_value)

    def _py_value(self, value):
        if value[0] == int.from_bytes(STR_FORMAT_C, byteorder='little'):
            value = value[1:].decode('utf-8')
        elif value[0] == int.from_bytes(INT_FORMAT_C, byteorder='little'):
            value = int.from_bytes(value[1:], byteorder='little', signed=True)
        elif value[0] == int.from_bytes(BOOL_FORMAT_C, byteorder='little'):
            value = bool.from_bytes(value[1:], byteorder='little')
        elif value[0] == int.from_bytes(NONE_FORMAT_C, byteorder='little'):
            value = None
        return value

    def _byte_value(self, value):
        if type(value) is str:
            value = STR_FORMAT_C + value.encode('utf-8')
        elif type(value) is int:
            value = INT_FORMAT_C + value.to_bytes((value.bit_length() + 7) // 8 + 1, byteorder='little', signed=True)
        elif type(value) is bool:
            value = BOOL_FORMAT_C + value.to_bytes(length=1, byteorder='little')
        elif value is None:
            value = NONE_FORMAT_C
        else:
            raise TypeError(value)
        return value

    def _wrap_key(self, key):
        return (self.name + '_' + key).encode('utf-8')

    def clear(self):
        with db.write_batch(transaction=True) as wb:
            wb.delete(self.name.encode('utf-8'))
            for key, value in db.iterator(prefix=(self.name + '_').encode('utf-8')):
                wb.delete(key)


class TList(_TType):
    def __init__(self, name, _list=None):
        super().__init__(name)
        db.put(self.name.encode('utf-8'), LIST_FORMAT_C)
        if _list is not None:
            self.extend(_list)

    def __len__(self):
        return self._get_count()

    def __getitem__(self, index):
        if type(index) is int:
            db_key = self._wrap_key(str(index))
            db_value = db.get(db_key)
            if db_value is None:
                raise IndexError(index)
            else:
                return self._to_py_value(db_key, db_value)
        else:
            raise TypeError(index)

    def __setitem__(self, index, value):
        count = self._get_count()
        if type(index) is int:
            if index < count:
                key = self._wrap_key(str(index))
                old_value = db.get(key)
                if old_value == LIST_FORMAT_C:
                    TList(key).clear()
                elif old_value == DICT_FORMAT_C:
                    TDict(key).clear()

                if type(value) is list:
                    TList(key, value)
                elif type(value) is dict:
                    TDict(key, value)
                else:
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
        if value == LIST_FORMAT_C:
            with db.write_batch(transaction=True) as wb:
                TList(last_index).clear()
                self._set_count(count - 1, wb)
        elif value == DICT_FORMAT_C:
            with db.write_batch(transaction=True) as wb:
                TDict(last_index).clear()
                self._set_count(count - 1, wb)
        else:
            with db.write_batch(transaction=True) as wb:
                wb.delete(self._wrap_key(str(count - 1)))
                self._set_count(count - 1, wb)
            return self._py_value(value)

    def append(self, value):
        count = self._get_count()
        index = count
        if type(value) is list:
            with db.write_batch(transaction=True) as wb:
                TList(self._wrap_key(str(index)), value)
                self._set_count(count + 1)
        elif type(value) is dict:
            with db.write_batch(transaction=True) as wb:
                TDict(self._wrap_key(str(index)), value)
                self._set_count(count + 1)
        else:
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

    def extend(self, _list):
        count = self._get_count()
        index = count - 1
        with db.write_batch(transaction=True) as wb:
            for item in _list:
                index += 1
                key = self._wrap_key(str(index))
                if item is None:
                    raise ValueError(item)
                elif type(item) is list:
                    TList(key, item)
                elif type(item) is dict:
                    TDict(key, item)
                else:
                    wb.put(key, self._byte_value(item))
            self._set_count(count + len(_list), wb)


class TDict(_TType):
    def __init__(self, name, _dict=None):
        super().__init__(name)
        db.put(self.name.encode('utf-8'), DICT_FORMAT_C)
        if _dict is not None:
            self._set_dict(self.name, _dict)

    def _set_dict(self, key, _dict):
        tile_dict = self._dict_tile(key, _dict)
        with db.write_batch(transaction=True) as wb:
            for k, v in tile_dict.items():
                wb.put(k.encode('utf-8'), v)

    def _dict_tile(self, prefix_key, _dict):
        rtv = {prefix_key: DICT_FORMAT_C}
        for k, v in _dict.items():
            if type(v) is dict:
                rtv.update(self._dict_tile(prefix_key + '_' + k, v))
            elif type(v) is list:
                TList(prefix_key + '_' + k, v)
            else:
                rtv[prefix_key + '_' + k] = self._byte_value(v)
        return rtv

    def __setitem__(self, key, value):
        old_value = db.get(self._wrap_key(key))
        if old_value == DICT_FORMAT_C:
            self[key].clear()
        if old_value == LIST_FORMAT_C:
            self[key].clear()

        if type(value) is dict:
            TDict(self.name + '_' + key, value)
        elif type(value) is list:
            TList(self.name + '_' + key, value)
        else:
            key = self._wrap_key(key)
            value = self._byte_value(value)
            db.put(key, value)

    def __getitem__(self, key):
        db_key = self._wrap_key(key)
        db_value = db.get(db_key)
        if db_value is None:
            raise KeyError(key)
        else:
            return self._to_py_value(db_key, db_value)

    def __contains__(self, key):
        key = self._wrap_key(key)
        value = db.get(key)
        return value is not None

    def __delitem__(self, key):
        self.pop(key)

    def pop(self, key, d=None):
        key = self._wrap_key(key)
        value = db.get(key)
        if value is None:
            if d is None:
                raise KeyError(key)
            else:
                return d
        elif value == DICT_FORMAT_C:
            TDict(key).clear()
        elif value == LIST_FORMAT_C:
            TList(key).clear()
        else:
            value = self._py_value(value)
            db.delete(key)
            return value

    def __iter__(self):
        self.db_iter = db.iterator(prefix=(self.name + '_').encode('utf-8'), reverse=True)
        self.db_iter.seek_to_start()
        return self

    def __next__(self):
        item = self.db_iter.prev()
        key = item[0].decode('utf-8')
        value = item[1]
        # RecursionError: maximum recursion depth exceeded while calling a Python object
        # if key.find('_', len(self.name) + 1) == -1:
        #     return key[len(self.name) + 1:]
        # else:
        #     return self.__next__()
        while key.find('_', len(self.name) + 1) != -1:
            item = self.db_iter.prev()
            key = item[0].decode('utf-8')
            value = item[1]
        return key[len(self.name) + 1:]


if __name__ == '__main__':

    db = plyvel.DB('./testleveldb/', create_if_missing=True)

    with db.write_batch(transaction=True) as wb:
        for key, value in db:
            wb.delete(key)

    # 0
    # test_data = TList('TestList', ['a', 'b', 'c', 'd', 'e', 'f', ['g1', 'g2']])
    # test_data.extend([1, 2])
    #
    # test_data.append(True)
    # print(test_data.pop())
    # test_data.append(False)
    #
    # test_data.append(None)
    # print(test_data.pop())
    # test_data.append(None)

    # 1
    # test_data = TDict('TestDict', {'a': 0, 'b': 1, 'c': [1, 2], 'd': {'d1': 0, 'd2': 1}})
    # for key in test_data:
    #     print(key)
    #     print(test_data[key])
    # for key in test_data['d']:
    #     print(key)
    #     print(test_data['d'][key])

    # 2
    # test_data = TDict('TestDict')
    # test_data['a'] = 'hello'
    # test_data['b'] = 'world'
    # test_data['a'] = ''
    # test_data['c'] = {'c1': 0, 'c2': 1}

    # 3
    # test_data = TList('TestList', [1, 2, 3, 4, 5, [61, 62, 63]])
    # for item in test_data:
    #     print(item)





    for key, value in db:
        print(key, value)

    db.close()
