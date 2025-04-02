from setuptools import setup, find_packages

setup(
    name="aiva-vrs",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    author="Mamidi HealthTech Solutions",
    author_email="info@mamidi.co.in",
    description="VRS Generator for mapping variants between different Variant Database Tables",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Mamidi-HealthTech-Solutions/aiva-vrs",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."
    ],
    python_requires=">=3.6",
)
