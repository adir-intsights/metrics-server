import setuptools


setuptools.setup(
    name='metrics_server',
    packages=setuptools.find_packages(),
    install_requires=[
        'fastapi',
        'google-cloud-monitoring',
        'tabulate',
        'termcolor',
        'uvicorn',
    ],
)
