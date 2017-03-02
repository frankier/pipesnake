from setuptools import setup

setup(
    name='PipeSnake',
    version='0.0dev0',
    packages=['pipesnake'],
    license='Apache v2',
    long_description=open('README.md').read(),
    install_requires=[
        'multipledispatch', 'curio',
    ],
    extras_require={
        'shdsl': ["sarge"],
        'instrument': ["pymux"],
    },
)
