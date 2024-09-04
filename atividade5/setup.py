from setuptools import find_packages, setup

setup(
    name="atividade5",
    packages=find_packages(exclude=["atividade5_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "SQLAlchemy",
        "psycopg2-binary",
        "pyarrow"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
