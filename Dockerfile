# syntax = docker/dockerfile:1.0-experimental
FROM public.ecr.aws/lambda/python:3.9

# copy files from current system to docker directory
COPY . ${LAMBDA_TASK_ROOT}

RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "feed_cleaner.handler" ]
