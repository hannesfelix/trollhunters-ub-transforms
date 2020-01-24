FROM jupyter/pyspark-notebook

RUN pip install --quiet clize

COPY write_table.py .

COPY mysql-connector-java-5.1.48.jar ./work/
COPY gcs-connector-hadoop2-latest.jar ./work/
COPY key.json ./work/

CMD ['python', 'write_table.py']
