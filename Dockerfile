# Use an official Python runtime
FROM python:3.10-slim

# Install RabbitMQ and Supervisor for process management
RUN apt-get update && apt-get install -y rabbitmq-server supervisor && rm -rf /var/lib/apt/lists/*

# Set the working directory in the cloud server
WORKDIR /app

# Copy your requirements and install them
COPY requirements.txt .
RUN pip install flask flask-cors pika groq requests

# Copy all your scripts and folders into the cloud server
COPY . .

# Copy the traffic controller settings
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create the data folder so the JSON files don't crash
RUN mkdir -p /app/data

# Open port 7860 for the internet
EXPOSE 7860

# Command to start the traffic controller (which starts everything else)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]