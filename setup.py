from setuptools import setup, find_packages
import os

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

version = os.environ.get("HAZELCAST_CLIENT_VERSION", "0.1.0")

setup(
    name="hazelcast-python-client",
    version=version,
    author="Hazelcast, Inc.",
    author_email="info@hazelcast.com",
    description="Hazelcast Python Client - A Python client for Hazelcast in-memory data grid",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hazelcast/HazelPython",
    project_urls={
        "Documentation": "https://docs.hazelcast.com/",
        "Source": "https://github.com/hazelcast/HazelPython",
        "Issue Tracker": "https://github.com/hazelcast/HazelPython/issues",
        "Changelog": "https://github.com/hazelcast/HazelPython/blob/main/CHANGELOG.md",
    },
    packages=find_packages(exclude=["tests", "tests.*", "examples", "examples.*", "docs"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
    ],
    keywords=[
        "hazelcast",
        "distributed",
        "cache",
        "in-memory",
        "data-grid",
        "nosql",
        "cluster",
        "streaming",
        "jet",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-timeout>=2.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "mypy>=1.0.0",
            "flake8>=6.0.0",
        ],
        "docs": [
            "sphinx>=6.0.0",
            "sphinx-rtd-theme>=1.2.0",
            "sphinx-autodoc-typehints>=1.22.0",
        ],
        "ssl": [
            "pyOpenSSL>=23.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "hazelcast-client-info=hazelcast.cli:client_info",
        ],
    },
    package_data={
        "hazelcast": ["py.typed"],
    },
    include_package_data=True,
    zip_safe=False,
)
