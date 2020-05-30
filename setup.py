from setuptools import setup
with open("README.md", "r") as fh:
    long_description = fh.read()
setup(name='recoWheel',
      version='0.0.1',
      author="Saikat Bhattacharjee",
      author_email="bhattacharjee.saikat3@hotmail.com",
      description="A PySpark application to reconcile data by generating sha1 hash of all the fields in a dataframe ",
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/saikat-bh/recoWheel",
      packages=['recoWheel'],
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent",
      ],
      zip_safe=False)