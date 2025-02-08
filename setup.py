from setuptools import setup, find_packages

setup(
    name="load_csv_to_postgres",  # Package name
    version="1.0.0",  # Version number
    packages=find_packages(),  # Automatically find packages
    install_requires=[
        "pandas",
        "psycopg2",
        "sqlalchemy",
        "configparser"
    ],
    entry_points={
        "console_scripts": [
            "load_csv=load_csv_to_postgres.load_csv_to_postgres:main"
        ]
    },
    author="Siva",
    author_email="siva@cygnussoftwares.com",
    description="A script to load CSV data into a PostgreSQL table under the bronze schema",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    #url="https://github.com/yourusername/load_csv_to_postgres",  # GitHub repo (if applicable)
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)