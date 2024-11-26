# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy project files
COPY . /app

# Copy the Google Sheets credentials file
COPY google_sheet_cred.json /app/google_sheet_cred.json

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port for Flask
EXPOSE 8080

# Start the Flask app
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app", "--timeout", "3000"]

