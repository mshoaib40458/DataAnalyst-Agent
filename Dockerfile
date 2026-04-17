FROM python:3.10-slim

WORKDIR /app

# Upgrade pip and install dependencies safely
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the entire project code into the container
COPY . .

# Expose port 7860 which is the standard default for Hugging Face Spaces
EXPOSE 7860

# Command to run FastAPI server natively on Space port
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
