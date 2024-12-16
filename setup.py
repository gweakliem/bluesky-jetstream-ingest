from setuptools import setup, find_packages

setup(
    name="jetstream_ingestion",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "websockets>=12.0",
        "pandas>=2.1.3",
        "pyarrow>=14.0.1",
        "google-cloud-storage>=2.13.0",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "jetstream-ingest=jetstream_ingestion.ingestor:main",
        ],
    },
)
