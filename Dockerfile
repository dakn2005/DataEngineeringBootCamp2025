FROM python:3.12.8

RUN pip install --upgrade pip
RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipelinec.py

# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "pipelinec.py"]

