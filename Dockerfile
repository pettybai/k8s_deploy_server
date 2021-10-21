FROM python:3.7.2

MAINTAINER yanlihua<yanlihua@inspur.com>

COPY ./requirements.txt ./
RUN pip install  -r ./requirements.txt  -i http://mirrors.aliyun.com/pypi/simple --trusted-host mirrors.aliyun.com


ADD . /hcm-devops
WORKDIR /hcm-devops
VOLUME ["/var/data"]

RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
&& echo 'Asia/Shanghai' >/etc/timezone
CMD python application.py

EXPOSE 8000