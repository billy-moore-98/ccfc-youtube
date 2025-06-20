from setuptools import find_packages, setup

setup(
    name="ccfc_yt_dags",
    packages=find_packages(exclude=["dags_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pydantic",
        "pytest"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
