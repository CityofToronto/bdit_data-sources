from setuptools import setup

setup(
    name='here_api',
    version='0.1',
    py_modules=['here_api'],
    install_requires=[
        'click',
        'requests-oauthlib',
        'requests',
        'email_notifications'
    ],
    dependency_links=[
        'git+https://github.com/CityofToronto/bdit_python_utilities.git#egg=email_notifications-0.1&subdirectory=email_notifications'],
        #https://stackoverflow.com/questions/18026980/python-setuptools-how-can-i-list-a-private-repository-under-install-requires
    python_requires='>=3',
    entry_points='''
        [console_scripts]
        here_api=here_api:main
    ''',
)