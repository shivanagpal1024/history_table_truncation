FROM docker.repo1.uhc.com/ecap/python:latest

RUN mkdir /history-tbl-truncation
COPY F6744HIS.py /history-tbl-truncation
COPY ecapEnum.py /history-tbl-truncation
COPY logger.py /history-tbl-truncation
COPY ness_logging.py /history-tbl-truncation

RUN chmod -R 775 /history-tbl-truncation/*

RUN pip3 install mysql.connector
RUN pip3 install requests

WORKDIR /history-tbl-truncation
RUN chown -R 1001 /history-tbl-truncation

USER 1001

CMD [ "sleep", "86400" ]