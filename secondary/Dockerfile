FROM python:3.8-slim-buster
ARG PORT_UVICORN
ENV PORT_UVICORN ${PORT_UVICORN}
COPY ./ ./
#COPY ./requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt

EXPOSE ${PORT_UVICORN}
RUN echo ${PORT_UVICORN}
CMD ["uvicorn", "secondary:app", "--host", "0.0.0.0", "--port", "${PORT_UVICORN}"]
# CMD export PORT_UVICORN=${PORT_UVICORN} && ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "${PORT_UVICORN}"]