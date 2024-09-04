FROM public.ecr.aws/lambda/python:3.12.2024.08.09.13

# Copy requirements.txt
COPY ./requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy lambda sources
COPY ./src ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]