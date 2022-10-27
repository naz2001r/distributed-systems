FROM python:3.8-slim-buster

ARG PORT_UVICORN
ENV PORT_UVICORN ${PORT_UVICORN}

ARG NODE_TYPE
ENV NODE_TYPE ${NODE_TYPE}


COPY ./${NODE_TYPE} ./
COPY ./common ./common
COPY ./requirements.txt ./requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt

EXPOSE ${PORT_UVICORN}
CMD uvicorn ${NODE_TYPE}:app --host 0.0.0.0 --port ${PORT_UVICORN}