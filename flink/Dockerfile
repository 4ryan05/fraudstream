# Apache Flink 1.20 with Python, so PyFlink jobs can run on the cluster.
FROM flink:1.20.1-scala_2.12-java17

ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir apache-flink==1.20.1 "setuptools<70"

COPY fraud_job.py submit.sh /opt/flink/usrlib/
# Strip Windows line endings in case the project was edited on Windows.
RUN sed -i 's/\r$//' /opt/flink/usrlib/submit.sh
