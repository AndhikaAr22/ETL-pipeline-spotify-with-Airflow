FROM my_airflow_image:latest

# Install the packages listed in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /opt/airflow

# Set the entrypoint and default command
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
