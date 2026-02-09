from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="remote_exporter",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Unified Prometheus exporter for remote node and arcus/memcached metrics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/remote_exporter",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Monitoring",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=[
        "prometheus-client>=0.14.0",
        "asyncssh>=2.13.0",
        "kazoo>=2.8.0",
    ],
    entry_points={
        "console_scripts": [
            "remote-exporter=remote_exporter.__main__:main",
        ],
    },
)
