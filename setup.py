#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-google-drive',
    version='0.0.1',
    description='hotglue tap for importing files from Google Drive',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_google_drive'],
    install_requires=[
        'argparse==1.4.0',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib'
    ],
    entry_points='''
        [console_scripts]
        tap-google-drive=tap_google_drive:main
    ''',
    packages=['tap_google_drive']
)
