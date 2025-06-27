from setuptools import find_packages, setup

setup(
    name="ccfc_yt_dags",
    packages=find_packages(exclude=["dags_tests"]),
    install_requires=[
        "aiohttp",
        "boto3",
        "dagster",
        "dagster-cloud",
        "pandas",
        "pydantic",
        "pytest",
        "s3fs"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
