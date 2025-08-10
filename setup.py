"""
Setup script for market-data-etl package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

# Read requirements
requirements = [
    "yfinance>=0.2.61",
    "yahooquery>=2.3.7", 
    "sqlalchemy>=2.0.23",
    "pandas>=2.1.4",
    "requests>=2.31.0"
]

setup(
    name="market-data-etl",
    version="2.0.0",
    author="Generated with Claude Code",
    author_email="noreply@anthropic.com",
    description="A Python package for extracting and storing market data from Yahoo Finance",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/market-data-etl",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10", 
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "market-data-etl=market_data_etl.cli.main:main",
            "mdetl=market_data_etl.cli.main:main",  # Short alias
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="finance, stocks, market data, yahoo finance, etl, data extraction",
    project_urls={
        "Bug Reports": "https://github.com/your-username/market-data-etl/issues",
        "Source": "https://github.com/your-username/market-data-etl",
        "Documentation": "https://github.com/your-username/market-data-etl#readme",
    },
)