# The Compliance Clerk

## Project Overview

The Compliance Clerk is an automated document processing system designed to extract, audit, and organize critical information from various PDF documents, specifically focusing on NA (Non-Agricultural) orders/leases and eChallans. It leverages a combination of heuristic (regex-based) extraction, Optical Character Recognition (OCR), and Large Language Models (LLMs) to accurately capture structured data from unstructured or semi-structured documents.

The system aims to streamline compliance-related tasks by automating the data extraction process, reducing manual effort, and improving data accuracy.

## Features

-   **PDF Document Processing**: Extracts text directly from PDF documents and performs OCR on pages with sparse or image-based text to ensure comprehensive data capture.
-   **Document Classification**: Automatically classifies incoming PDF documents into types such as `NA Order`, `NA Lease`, or `EChallan`.
-   **Heuristic Data Extraction**: Utilizes a robust set of regular expressions to identify and extract key fields from document text.
-   **Intelligent Document Grouping**: Groups related documents (e.g., an NA Order and its corresponding NA Lease) into `ProcessingCluster`s based on common identifiers like survey numbers or challan numbers.
-   **LLM-Powered Auditing and Refinement**: Integrates with Large Language Models (LLMs) to audit and refine heuristically extracted data, fill missing fields, and correct inaccuracies based on the document's context. This feature enhances the accuracy and completeness of the extracted information.
-   **Configurable Field Keywords**: Allows for easy modification and extension of keywords used in heuristic extraction for different document types.
-   **Structured Data Export**: Exports the extracted and audited data into organized Excel (`.xlsx`) and CSV (`.csv`) formats, including separate exports for NA and eChallan records.
-   **Comprehensive Logging**: Logs LLM interactions and schema validation errors for transparency, debugging, and continuous improvement.

## Getting Started

Follow these instructions to set up and run the project on your local machine.

### Prerequisites

-   **Python 3.9+**: The project is developed using Python.
-   **pip**: Python's package installer, usually comes with Python.
-   **Tesseract OCR**: An open-source OCR engine. You need to install Tesseract on your system and ensure it's in your system's PATH.
    -   **macOS**: `brew install tesseract`
    -   **Ubuntu/Debian**: `sudo apt-get install tesseract-ocr`
    -   **Windows**: Download the installer from Tesseract-OCR GitHub.

### Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/vivek-chandan/The-Compliance-Clerk.git
    cd The-Compliance-Clerk
    ```

2.  **Create and activate a virtual environment**:
    It's highly recommended to use a virtual environment to manage project dependencies.
    ```bash
    python -m venv venv
    source venv/bin/activate # On macOS/Linux
    # venv\Scripts\activate # On Windows
    ```

3.  **Install project dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### API Key Setup for LLM (Optional)

To enable LLM-powered auditing, you need to provide an API key for your chosen LLM provider (e.g., OpenAI, OpenRouter).

1.  Create a `.env` file in the root directory of the project.
2.  Add your API key to the `.env` file. For example:
    -   For OpenAI:
        ```
        OPENAI_API_KEY="your_openai_api_key_here"
        ```
    -   For OpenRouter:
        ```
        LLM_PROVIDER="openrouter"
        OPENROUTER_API_KEY="your_openrouter_api_key_here"
        ```
    You can also specify the LLM model to use (e.g., `LLM_MODEL="gpt-4.1-mini"`). If no API key is found, the system will run in heuristic-only mode.

## Usage

1.  **Place your PDF documents**:
    Put all the PDF files you want to process into the `data/raw_pdfs/` directory. The system will recursively search this directory.

2.  **Run the main script**:
    Execute the `main.py` script from the project root directory.
    ```bash
    python main.py
    ```

3.  **View Results**:
    After execution, the extracted data will be saved in the `output/` directory:
    -   `results.xlsx`: A comprehensive Excel file with all extracted records.
    -   `results.csv`: A comprehensive CSV file with all extracted records.
    -   `na_results.xlsx` / `na_results.csv`: Separate files for NA document types.
    -   `echallan_results.xlsx` / `echallan_results.csv`: Separate files for eChallan document types.

    Log files for LLM interactions and schema errors will be found in the `logs/` directory.

## Project Structure

```
The-Compliance-Clerk/
├── data/
│   └── raw_pdfs/             # Directory to place input PDF documents
├── output/                   # Directory for output Excel and CSV files
├── logs/                     # Directory for LLM interaction and error logs
├── src/
│   ├── __init__.py
│   ├── exporter.py           # Handles saving processed data to Excel/CSV
│   ├── grouper.py            # Classifies and groups related PDF documents
│   ├── llm_handler.py        # Manages interactions with Large Language Models
│   ├── logger.py             # Provides utility functions for logging
│   ├── ocr.py                # Performs Optical Character Recognition on PDFs
│   ├── parser.py             # Implements heuristic (regex-based) data extraction
│   ├── schema.py             # Defines data models (Pydantic) and field keywords
│   └── validator.py          # Cleans and validates JSON output from LLMs
├── main.py                   # Main entry point of the application
└── requirements.txt          # Lists all Python dependencies

