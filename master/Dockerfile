FROM python:3.8-slim-buster

ARG PORT_UVICORN
ENV PORT_UVICORN ${PORT_UVICORN}

COPY ./master ./
COPY ./common ./common

RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt

EXPOSE ${PORT_UVICORN}
CMD uvicorn master:app --host 0.0.0.0 --port ${PORT_UVICORN}