from setuptools import setup, find_packages

setup(
    name='vendor_invoice_validator',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # Add your dependencies here
        'pandas>=2.3.0',
        'numpy>=1.26.0',
        'openpyxl>=3.1.1',
        'selenium>=4.21.0',
        # Add more dependencies as needed
    ],
)
