FROM python:3.12.8

RUN pip install --upgrade pip
RUN pip install pandas 
#sqlalchemy psycopg2

WORKDIR /app 

# COPY pipeline.py pipeline.py
# COPY ingest_data.py ingest_data.py

ENTRYPOINT ["bash"]
# ENTRYPOINT ["python", "ingest_data.py"]
# ENTRYPOINT ["python", "pipeline.py"]


