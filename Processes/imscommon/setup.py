from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='imscommon',
    version='2.0.0',
    author='Reza Adibnia',
    author_email="reza.adibnia@futurewei.com",
    packages=find_packages(),
    url="https://github.com/Futurewei-io/blue-marlin",
    license="Apache License 2.0",
    description="All the packages required for running predictor",
    long_description=long_description,
    long_description_content_type="",
    platforms="linux",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Apache License 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
