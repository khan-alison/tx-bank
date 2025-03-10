from setuptools import setup, find_packages

setup(
    name="tx_bank",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="An ETL pipeline for processing customer and account data using Apache Spark.",
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-repo/tx-bank",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pyspark",
        "delta-spark",
        "boto3",
        "python-dotenv",
        "setuptools",
        "wheel"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "tx-transform=tx_bank.jobs.s2i.main:main",
        ],
    },
)
