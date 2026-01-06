# GeM Tender Automation System

## Overview
This system is an automated, high-performance scraping and relevancy analysis pipeline for the GeM (Government e-Marketplace) portal. It features real-time tender extraction, ML-based relevancy scoring, keyword matching, and automated PDF document processing.

## System Components
1.  **Scraper (`run.py`)**: The core engine that scrapes the portal, performs relevancy analysis, and manages the database.
2.  **Worker Fleet (Docker)**: Scalable background workers that handle heavy tasks like PDF downloading and text extraction.
3.  **Database (MySQL)**: Stores all tender data, relevancy scores, and file paths.
4.  **Message Broker (Redis)**: Manages the task queue between the scraper and workers.

## Prerequisites
Before setting up, ensure you have the following installed:
-   **Python 3.10+**
-   **Docker & Docker Compose** (Docker Desktop for Windows/Mac, or Docker Engine for Linux)
-   **MySQL Server** (8.0+)
-   **Git**
-   **Google Chrome / Chromium** (Managed by Playwright, but good to have)

## Installation Guide

### 1. Clone the Repository
```bash
git clone <repository-url>
cd new_Scrapper
```

### 2. Database Setup
1.  Install and start MySQL Server.
2.  Log in to MySQL and create the database and user:
    ```sql
    CREATE DATABASE tender_automation_with_ai;
    CREATE USER 'tender_user'@'%' IDENTIFIED BY 'StrongPassword@123';
    GRANT ALL PRIVILEGES ON tender_automation_with_ai.* TO 'tender_user'@'%';
    FLUSH PRIVILEGES;
    ```
    *(Note: Replace 'StrongPassword@123' with a secure password in production)*
3.  Ensure MySQL is listening on all interfaces (or at least accessible from Docker). On Linux, you might need to edit `my.cnf` to set `bind-address = 0.0.0.0`.

### 3. Python Environment (Host)
This setup is for running the main scraper (`run.py`).

1.  Create a virtual environment:
    ```bash
    python -m venv venv
    ```
2.  Activate the environment:
    -   **Windows**: `.\venv\Scripts\activate`
    -   **Linux/Mac**: `source venv/bin/activate`
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Install Playwright browsers:
    ```bash
    playwright install chromium
    ```

### 4. Create Database Tables
Run the initialization script to set up the necessary tables:
```bash
python create_missing_table.py
```

## Running the Application

### Step 1: Start Background Services (Redis & Workers)
Use Docker Compose to start the Redis broker and the Worker fleet.

```bash
docker-compose up -d --scale worker=5
```
*   `--scale worker=5`: Starts 5 concurrent worker containers. Adjust based on your system's resources (CPU/RAM).
*   Check status: `docker-compose ps`

### Step 2: Start the Main Scraper
Run the main Python script from your host machine (assuming `venv` is active).

```bash
python run.py
```

## Configuration

### Environment Variables
You can customize behavior by setting environment variables in your terminal or a `.env` file (if supported) before running `run.py`.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DB_HOST` | `localhost` | Database host address. |
| `DB_USER` | `root` | Database user. |
| `DB_PASSWORD` | *(empty)* | Database password. |
| `DB_NAME` | `tender_automation_with_ai` | Database name. |
| `SCRAPER_INTERVAL` | `300` | Wait time (seconds) between full scrape cycles. |
| `DB_WORKER_THREADS` | `6` | Number of threads for DB upsert operations. |

### Docker Configuration
To change Worker configuration, edit `docker-compose.yml`:
-   **Database Connection**: By default, it connects to `host.docker.internal` to access the host's MySQL. Update `DB_HOST`, `DB_USER`, `DB_PASSWORD` in the `worker` service section if needed.

## Directory Structure
-   `OUTPUT/`: Contains generated JSON files (Representation/Corrigendum data).
-   `PDF/`: Contains downloaded tender PDFs.
-   `app/`: Core application logic (Keywords, Matcher).
-   `relevency/`: Scripts for global relevancy analysis.
-   `extractor/`: Standalone PDF extraction scripts.

## Troubleshooting
-   **Modules Missing**: Ensure you ran `pip install -r requirements.txt` in your active venv.
-   **Docker Connection Refused**: Check if your Firewall allows connections from Docker containers to the Host MySQL port (3306). 
-   **Playwright Errors**: Try running `playwright install --with-deps` to ensure all system dependencies are met.
