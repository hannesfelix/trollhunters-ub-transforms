FROM jupyter/pyspark-notebook

RUN pip install --quiet clize tweepy orjson pymongo gcsfs

COPY *.py .

COPY gcs-connector-hadoop2-latest.jar ./work/
COPY key.json ./work/

CMD ['python', 'rehydrate.py']
