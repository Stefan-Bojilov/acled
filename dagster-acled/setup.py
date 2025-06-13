from setuptools import find_packages, setup

setup(
    name="dagster_acled",
    packages=find_packages(exclude=["dagster_acled_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
