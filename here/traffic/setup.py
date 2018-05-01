from setuptools import setup

setup(
    name='here_api',
    version='0.1',
    py_modules=['here_api'],
    install_requires=[
        'click',
        'requests-oauthlib',
        'requests'
    ],
    python_requires='>=3',
    entry_points='''
        [console_scripts]
        here_api=here_api:main
    ''',
)