from setuptools import setup, find_packages
import sys, os

version = '0.2'

setup(name='webstore-client',
      version=version,
      description="Python client for WebStore",
      long_description="""\
WebStore is a RESTful web table service. This package offers a few convenience options to access the system remotely.""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='webstore rest api tables csv json',
      author='Open Knowledge Foundation',
      author_email='info@okfn.org',
      url='http://okfn.org',
      license='GPLv3',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
