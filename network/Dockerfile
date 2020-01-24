FROM jupyter/pyspark-notebook

RUN pip install --quiet clize langdetect networkx gcsfs

COPY *.py .

COPY gcs-connector-hadoop2-latest.jar ./work/
COPY key.json ./work/

CMD ['python', 'network.py']
