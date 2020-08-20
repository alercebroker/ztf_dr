from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize

module_name = "dr_parser"
pyx_path = [f"{module_name}/{module_name}_wrapper.pyx"]#, f"{module_name}/lib/parser.cpp"]
# headers_path = [f"{module_name}/lib/parser.hpp"]
# lib_path = f"{module_name}/lib"

import pyarrow as pa

extension = Extension(
    name=f"{module_name}.{module_name}_wrapper",
    sources=pyx_path,
    # depends=headers_path,
    include_dirs=[pa.get_include()],#lib_path],
    # language="c++"
)

setup(
    name=module_name,
    ext_modules=cythonize([extension]),
    install_requires=[],
    # build_requires=["Cython>=0.20"],
    packages=[module_name],
    author='ALeRCE',
    version='0.0.1',
)
