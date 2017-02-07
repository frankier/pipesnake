from setuptools import setup

setup(
    name='PipeSnake',
    version='0.0dev0',
    packages=['pipesnake'],
    license='Apache v3',
    long_description=open('README.md').read(),
    install_requires=[
        'multipledispatch',
    ]
)
