#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-google-drive',
    version='0.0.2',
    description='hotglue tap for importing files from Google Drive',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_google_drive'],
    install_requires=[
        'google-api-python-client',
        'google-auth-httplib2',
        'hotglue-singer-sdk',
        'hotglue-etl-exceptions'
    ],
    entry_points='''
        [console_scripts]
        tap-google-drive=tap_google_drive.tap:GoogleDriveTap.cli
    ''',
    packages=['tap_google_drive']
)
