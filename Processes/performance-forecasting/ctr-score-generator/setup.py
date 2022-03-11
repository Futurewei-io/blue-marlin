import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pf_ctr_score_generator", # Replace with your own username
    version="1.0.0",
    author="Reza Adibnia",
    author_email="radibnia@futurewei.com",
    description="All the packages required for running PF-CTR-Score-Generator process as part of Performance Forecasting.",
    long_description=long_description,
    long_description_content_type="",
    url="https://github.com/Futurewei-io/blue-marlin",
    packages=setuptools.find_packages(exclude=[]),
    license="Apache License 2.0",
    classifiers=[
        "Programming Language :: Python :: 2",
        "License :: Apache License 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)
