""" Setup.py for packaging log-anomaly-detector as library """
from setuptools import setup, find_packages

long_description = "Log Aggregator - Machine learning to aggregate abnormal events logs"


REQUIRED_PKG = [
    "log-anomaly-detector @ git+http://git.solidex.by:3000/nhryshalevich/log-anomaly-detector",
]

setup(
    name="log-aggregator",
    version="1.0.1",
    py_modules=['aggr_app'],
    packages=find_packages(),
    zip_safe=False,
    classifiers=(
        "Development Status :: 1 - Planning",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
    ),
    python_requires=">3.5",
    url="https://github.com/AICoE/log-anomaly-detector",
    author="Nadzeya Hryshalevich",
    author_email="info@solidex.by",
    description="Log aggregator for anomaly logs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=REQUIRED_PKG,
    entry_points="""
        [console_scripts]
        log-aggregator=aggr_app:cli
    """,
)
