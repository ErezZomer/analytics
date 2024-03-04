import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = [
    "boto3==1.34.53",
    "pandas==2.0.3",
    "pydantic==2.6.3"
]


setuptools.setup(
    name="analytics",
    version="1.0.0-0",
    author="Erez Zomer",
    author_email="erezzomer@gmail.com",
    description="Vehicle Data Analytics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ErezZomer/analytics.git",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=["Programming Language :: Python :: 3"],
    install_requires=[requirements],
    entry_points={},
)
