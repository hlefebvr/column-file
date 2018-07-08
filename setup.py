import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ColumnFile",
    version="0.0.4",
    author="Henri Lefebvre",
    author_email="henri.pasdecalais@yahoo.com",
    description="Simple library to manage local file storage within an application based on the concept of local partition and sort keys",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hlefebvr/column-file",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
