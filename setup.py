import distutils
from distutils.core import setup
import os,sys

# The main call
setup(name='despydb',
      version ='3.0.0',
      license = "GPL",
      description = "Provide a dialect-neutral interface to DES databases",
      author = "The National Center for Supercomputing Applications (NCSA)",
      packages = ['despydb'],
      package_dir = {'': 'python'},
      data_files=[('ups',['ups/despydb.table'])],
      scripts = ['bin/query.py'],
      )

#testing()


