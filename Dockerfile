FROM selenium/standalone-chrome:latest

WORKDIR /app

USER root

RUN apt-get update && apt-get install -y \
    python3-setuptools \
    wget \
    unzip \
    libnss3 \
    libxss1 \
    libappindicator3-1 \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libgbm-dev \
    python3-venv 

RUN python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]