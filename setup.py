#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='pg_notify',
    version='0.1.0',
    description="A commandline utility to set up postgres NOTIFY/LISTEN actions.",
    long_description=readme + '\n\n' + history,
    author="Nahuel Defossé",
    author_email='nahuel dot defosse at gmail',
    url='https://github.com/d3f0/pg_notify',
    packages=[
        'pg_notify',
    ],
    package_dir={'pg_notify':
                 'pg_notify'},
    entry_points={
        'console_scripts': [
            'pg_notify=pg_notify.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='pg_notify',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
