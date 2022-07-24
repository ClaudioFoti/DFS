FROM ubuntu:22.04
COPY . /M13
WORKDIR /M13
EXPOSE 8080
RUN apt update
RUN apt -y install python3-pip
RUN python3 -m pip install -r requirements.txt
CMD python3 storage.py