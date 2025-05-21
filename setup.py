from setuptools import setup, find_packages

setup(
    name="spotify-mlops-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # List your main dependencies here
        "pandas",
        "numpy",
        "scikit-learn",
        "xgboost",
        "mlflow",
        "dagster",
    ],
)
