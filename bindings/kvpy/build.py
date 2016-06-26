from distutils.core import setup, Extension

module1 = Extension('kvspool',
                    sources = ['kvspool.c'], 
                    include_dirs=['../include'],
                    library_dirs = ['../src'], 
                    libraries = ['kvspool'])

setup (name = 'kvspool',
       version = '1.0',
       description = 'Python interface to kvspool',
       ext_modules = [module1])
