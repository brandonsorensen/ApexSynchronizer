import pathlib
import setuptools


HERE = pathlib.Path(__file__).parent

README = (HERE/'README.md').read_text()

setuptools.setup(
    name='apex_synchronizer',
    version='1.0',
    description='A client that acts as a go-between for PowerSchool and '
                'Apex Learning.',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Brandon Sorensen',
    author_email='sorensen.12@gmail.com',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python'
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.8"
)
