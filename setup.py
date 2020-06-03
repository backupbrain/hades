import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name='hades',
    version='0.1',
    scripts=[],
    author="Adonis Gaitatzis",
    author_email="backupbrain@gmail.com",
    description="A python-based database to object factory",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/backupbrain/hades",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
    ],
)
