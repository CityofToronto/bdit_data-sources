from setuptools import setup

setup(
    name='pull_cis_data',
    version='0.1',
    py_modules=['pull_cis_data'],
    install_requires=[
        'click',
        'requests-oauthlib',
        'requests',
        'email_notifications'
    ],
    dependency_links=[
        'git+https://github.com/CityofToronto/bdit_python_utilities.git#egg=email_notifications-0.2&subdirectory=email_notifications'],
        #https://stackoverflow.com/questions/18026980/python-setuptools-how-can-i-list-a-private-repository-under-install-requires
    python_requires='>=3',
    entry_points='''
        [console_scripts]
        pull_cis_data=pull_cis_data:main
    ''',
)
