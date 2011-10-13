from distutils.core import setup, Extension

module1 = Extension('kvpy',
                    sources = ['kvpymodule.c'], 
                    include_dirs=['../'],
                    library_dirs = ['../'], 
                    libraries = ['kvspool'])

setup (name = 'KvPyModule',
       version = '1.0',
       description = 'Python interface to kvspool',
       ext_modules = [module1])
