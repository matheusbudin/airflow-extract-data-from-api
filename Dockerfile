FROM quay.io/astronomer/astro-runtime:8.0.0

COPY requirements.txt .
RUN pip install -r requirements.txt