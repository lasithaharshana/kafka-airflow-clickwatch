"""Setup configuration for e-commerce clickstream project."""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ecommerce-clickstream",
    version="0.1.0",
    author="Lasitha Harshana",
    description="E-commerce clickstream analytics with Kafka, Spark, and Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lasithaharshana/biodata",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "kafka-python>=2.0.2",
        "pyspark>=3.5.0",
        "apache-airflow>=2.10.4",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "isort>=5.13.2",
            "pre-commit>=3.6.0",
        ],
    },
)
