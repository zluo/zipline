'''
Created on Feb 8, 2016

@author: zluo
'''
from distutils.core import setup
from Cython.Build import cythonize
from Cython.Build.Dependencies import cythonize
import numpy
setup(
      name = 'zipline',
      ext_modules = cythonize(['zipline/assets/*.pyx', 'zipline/lib/*.pyx', 'zipline/data/*.pyx']),
      include_dirs=[numpy.get_include()]
      )