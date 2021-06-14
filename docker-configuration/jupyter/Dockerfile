FROM spark-base

RUN apt-get update
RUN apt-get -y install python3-pip
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata
RUN apt-get -y install python3-bs4
RUN pip3 install --upgrade pip
RUN pip3 install jupyterlab
RUN pip3 install pandas
RUN pip3 install requests
RUN pip3 install pymysql
RUN cd spark/python && python3 setup.py install
RUN cd && rm -rf spark
EXPOSE 8888
VOLUME /app/
CMD jupyter lab --ip 0.0.0.0 --allow-root


