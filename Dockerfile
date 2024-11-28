FROM jupyter/pyspark-notebook:python-3.11

ENV PYARROW_IGNORE_TIMEZONE=1

USER root

# Set working directory
WORKDIR /home/jovyan/work

# Copy and install requirements
COPY ./requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt --no-cache-dir

# Copy entrypoint and ensure it's executable
COPY ./scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER jovyan

EXPOSE 8888 4040 8000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]