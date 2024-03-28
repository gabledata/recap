# Use the official Python base image
FROM python:3.11

ARG RECAP_VERSION

# Set the working directory
WORKDIR /app

# Install the package
COPY . /app
RUN pip install "recap-core[all]==$RECAP_VERSION"

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["recap", "serve"]
