FROM python:3.7
RUN pip3 install --upgrade pip 
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
COPY requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt
RUN pip install --upgrade Flask
RUN python3 -m spacy download en_core_web_sm
RUN airflow initdb
RUN airflow resetdb -y
COPY entrypoint.sh /root/entrypoint.sh
EXPOSE 8080 
COPY dags /root/airflow/dags
#ENTRYPOINT ["airflow","webserver"]
#RUN chmod +x /root/entrypoint.sh
ENTRYPOINT ["bash","/root/entrypoint.sh"]
