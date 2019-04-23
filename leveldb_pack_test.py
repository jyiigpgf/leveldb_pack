
import unittest

import plyvel

import leveldb_pack
from leveldb_pack import TDict, TList

# TODO: 增加查看DB数据的测试用例


class TestTDict(unittest.TestCase):

    def test_init(self):
        leveldb_pack.db = plyvel.DB('./testleveldb/', create_if_missing=True)

        with leveldb_pack.db.write_batch(transaction=True) as wb:
            for key, value in leveldb_pack.db:
                wb.delete(key)

        self.test_dict()

        self.test_list()

        leveldb_pack.db.close()

    def test_dict(self):
        test_data = TDict('TestDict', {
            'a': 4,
            'b': 'abc',
            'c': {
                'c1': 1,
                'c2': 'd',
                'c3': {
                    'd1': 'h w'
                }
            }
        })

        # __setitem__
        test_data['a'] = 10
        self.assertEqual(test_data['a'], 10)
        with self.assertRaises(TypeError):
            test_data['a'] = None
        test_data['g'] = {
            "d2": "hello world"
        }
        self.assertEqual(test_data['g']['d2'], 'hello world')
        test_data['g'] = 'hello'
        with self.assertRaises(TypeError):
            test_data['g']['d2']

        # __getitem__
        self.assertEqual(test_data['a'], 10)
        self.assertEqual(test_data['c']['c2'], 'd')
        test_data['c']['c3']['d1'] = 'hello world'
        self.assertEqual(test_data['c']['c3']['d1'], 'hello world')

        # __contains__
        self.assertTrue('a' in test_data)
        self.assertFalse('d' in test_data)

        # pop
        self.assertEqual(test_data.pop('b'), 'abc')
        with self.assertRaises(KeyError):
            print(test_data['b'])
        self.assertEqual(test_data.pop('b', 'hello'), 'hello')
        test_data.pop('c')

        # __init__
        test_data = TDict('TestDict')
        self.assertEqual(test_data['a'], 10)

        # clear
        test_data.clear()

        #
        test_data = TDict('TestDict', {'c': {'c1': 1, 'c2': 2}, 'a': [1, 2, 3]})
        test_data['b'] = [1, 2, 3]
        self.assertEqual(test_data['a'][1], 2)
        test_data.pop('a')

    def test_list(self):
        test_data = TList('TestList', ['a', 'b', 'c', 'd', 'e', 'f', ['g1', 'g2']])
        with self.assertRaises(TypeError):
            TList('TestList', ['a', 'b', 'c', 'd', 'e', 'f', ['g1', 'g2']])
        test_data = TList('TestList')

        # __len__
        self.assertEqual(len(test_data), 7)

        # __getitem__
        self.assertEqual(test_data[3], 'd')
        self.assertEqual(type(test_data[6]), TList)

        # __setitem__
        test_data[0] = 'c'
        self.assertEqual(test_data[0], 'c')
        with self.assertRaises(IndexError):
            test_data[10000] = 'c'
        test_data[6] = 'g'
        self.assertEqual(test_data[6], 'g')

        # append & pop
        test_data.append('hello')
        self.assertEqual(test_data.pop(), 'hello')
        test_data.append(['h1', 'h2', 'h3', 'h4'])
        self.assertEqual(type(test_data[7]), TList)
        test_data.pop()

        # clear
        test_data.clear()

        #
        test_data = TList('TestList', ['a', {'b': 'hello'}])
        self.assertEqual(type(test_data[1]), TDict)
        test_data[0] = [1, 2, 3]
        self.assertEqual(type(test_data[0]), TList)
        test_data[0] = {'a': 'world'}
        self.assertEqual(type(test_data[0]), TDict)
        test_data[0] = 'a'
        self.assertEqual(test_data[0], 'a')
        test_data.append({'c': 'world'})
        self.assertEqual(test_data[2]['c'], 'world')

