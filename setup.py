import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eqasim",
    version="1.0.0",
    author="Sebastian Hörl",
    author_email="sebastian.hoerl@ivt.baug.ethz.ch",
    description="Python part of eqasim",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eqasim-org/eqasim-python",
    packages=setuptools.find_packages(),
#    classifiers=[
#        "Programming Language :: Python :: 3",
#        "License :: OSI Approved :: MIT License",
#        "Operating System :: OS Independent",
#    ],
    install_requires = [
        "pandas>=0.24.2",
        "numpy>=1.15.1"
    ]
)
