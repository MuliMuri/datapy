import setuptools

setuptools.setup(
    name="datapy",
    packages=setuptools.find_namespace_packages("src"),
    package_dir={"": "src"},
    author="MuliMuli",
    author_email="mulimuri@outlook.com",
    requires=[
        'pandas',
        'pymysql',
        'cryptography'
    ]
)
