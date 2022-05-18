import lmdb


class LMDB:
    def __init__(self, path, map_size):
        map_size = int(map_size)
        self.env = lmdb.open(path, map_size=map_size)

    def __setitem__(self, key, value):
        key = self.encode(key)
        value = self.encode(value)
        txn = self.env.begin(write=True)
        txn.put(key=key, value=value)
        txn.commit()

    def __getitem__(self, key):
        key = self.encode(key)
        txn = self.env.begin()
        out = txn.get(key=key)
        return self.decode(out)

    def __iter__(self):
        txn = self.env.begin()
        for k, v in txn.cursor():
            yield self.decode(k), self.decode(v)

    def delete(self, key):
        key = str(key).encode()
        txn = self.env.begin(write=True)
        txn.delete(key=key)
        txn.commit()

    def __del__(self):
        self.env.close()

    @staticmethod
    def encode(x):
        return str(x).encode()

    @staticmethod
    def decode(x):
        return x.decode()

    def __len__(self):
        txn = self.env.begin()
        return txn.stat()['entries']


if __name__ == '__main__':
    db = LMDB(path='../data/lmdb_test', map_size=1e6)
    db[1] = 1
    db[2] = 2
    db[3] = 3
    db[3] = 4
    db.delete(2)
    d = iter(db)

    print(f'{db[1]=}')
    print('iter')
    for _ in d:
        print(_)
