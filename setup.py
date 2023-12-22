from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(name='dynauto',
      version='1.0',
      description='Control package for dynamic automation system.',
      long_description=long_description,
      url='https://github.com/simonepigazzini/dynamic-automation',
      author='Simone Pigazzini',
      author_email='simone.pigazzini@gmail.com',
      license='GPLv3',
      packages=[
          'dynauto'
      ],
      # scripts=[
      #     'bin/ecalautomation.py',
      #     'bin/ecaldiskmon.py',
      #     'bin/ecalrunctrl.py',
      #     'bin/copysts.py'
      # ],
      classifiers=[
          "Programming Language :: Python :: 3",
          "Operating System :: Linux",
      ],
      python_requires=">=3.6",
      install_requires=[
          'influxdb',
      ]
)
