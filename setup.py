#!/usr/bin/env python

import setuptools

setuptools.setup(name='daskutils',
      version='0.1',
      description='Utils on top of dask bags',
      long_description="""# daskutils
Utils on top of dask bags
      """,
      long_description_content_type="text/markdown",
      author='Egil Moeller',
      author_email='egil@innovationgarage.no',
      url='https://github.com/innovationgarage/daskutils',
      packages=setuptools.find_packages(),
      install_requires=[
          'dask>=0.19.2',
      ],
      include_package_data=True
  )
