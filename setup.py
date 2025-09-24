from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="django-jaeger-middleware-plus",
    version="0.2.0",
    author="zhaishuaishuai",
    author_email="zhaishuaishuai001@gmail.com",
    description="A Django middleware for distributed tracing with Jaeger",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lcoolcool/django-jaeger-middleware-plus",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Framework :: Django",
        "Framework :: Django :: 2.2",
        "Framework :: Django :: 3.2",
        "Framework :: Django :: 4.0",
        "Framework :: Django :: 4.1",
        "Framework :: Django :: 4.2",
    ],
    python_requires=">=3.7",
    install_requires=[
        "Django>=2.2",
        "jaeger-client>=4.6.1",
        "opentracing>=2.4.0",
        "requests>=2.25.1",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-django>=2.2",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.812",
        ],
    },
    keywords=["django", "django-jaeger-middleware", "jaeger" "opentracing" "microservice"],
    project_urls={
        "Bug Reports": "https://github.com/lcoolcool/django-jaeger-middleware-plus/issues",
        "Source": "https://github.com/lcoolcool/django-jaeger-middleware-plus",
        "Documentation": "https://github.com/lcoolcool/django-jaeger-middleware-plus",
    },
)
