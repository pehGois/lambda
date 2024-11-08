# QuickSight Analysis and Dataset Migration

This project is designed to handle the migration of Amazon QuickSight analyses and datasets. It includes various components to streamline the migration process, error logging, and handling templates in AWS QuickSight.

## Table of Contents

- [QuickSight Analysis and Dataset Migration](#quicksight-analysis-and-dataset-migration)
  - [Table of Contents](#table-of-contents)
  - [Project Structure](#project-structure)
  - [Installation](#installation)
  - [Usage](#usage)

## Project Structure

Here's an overview of the folder structure for the project:

```
├── .dockerignore            # Specifies files to ignore when building Docker images
├── .env                     # Environment variables for configuration
├── .gitignore               # Specifies files to ignore in Git
├── Dockerfile               # Docker image configuration
├── README.md                # Project documentation
├── compose.yaml             # Docker Compose configuration
├── debug.py                 # Debugging script for testing
├── fast_api.py              # FastAPI application entry point
├── lambda_function.py       # AWS Lambda handler function
├── requirements.txt         # Python dependencies
├── src/                     # Main source code directory
│   ├── classes/             # Core classes for handling migration
│   ├── logs/                # Log files
│   ├── static/              # Static files (e.g., CSS, images)
│   ├── templates/           # HTML templates for rendering
│   └── utils/               # Utility functions and helpers
```

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/pehGois/lambda.git
   cd lambda
   ```

2. **Install dependencies:**

   Use the following command to install the necessary Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**

   Make sure to configure the `.env` file with the necessary AWS credentials and other configuration settings required for QuickSight integration.
   It's highly recommendable to install the AWS CLI, otherwise you'll have to include your credentians manually in the client solicitacion

## Usage

1. **Running the FastAPI Server:**

   To start the FastAPI server, run:

   ```bash
   uvicorn lambda_function:app --reload
   ```