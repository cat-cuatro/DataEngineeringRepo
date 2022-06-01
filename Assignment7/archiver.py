import zlib
import sys
import io
import os
import pandas as pd
class Archiver():
    def __init__(self):
        pass

    def receiveWriteCompress(self, data, fname):
        print('Starting compress + write')
        with open(fname+'-compressed', mode="wb") as out:
            for datum in data:
                #print(type(datum))
                datum = str.encode(datum)#bytes(datum)
                #print('after', type(datum))
                compress = zlib.compress(datum, zlib.Z_BEST_COMPRESSION)
                out.write(compress)
        out.close()        
        print('done')    

if __name__ == "__main__":
    file_archiver = Archiver()
    test_data = ['here','is','some','data']
    file_archiver.receiveWriteCompress(test_data, 'sample')
