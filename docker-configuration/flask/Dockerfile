FROM spark-base

RUN apt-get update
RUN apt update
RUN apt-get -y install python3-pip
RUN pip3 install --upgrade pip
RUN pip3 install pandas
RUN pip3 install requests
RUN pip3 install Flask
RUN pip3 install pymysql
WORKDIR /app
EXPOSE 5000
VOLUME /app/
CMD python3 api.py


