from setuptools import find_packages, setup

setup(
    name="quickstart_etl",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster==1.5.13",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "tushare"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
