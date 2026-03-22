import subprocess         # For running the Tesseract OCR command
import tempfile           # For creating temporary files
from pathlib import Path  # For handling file paths

import fitz                # For working with PDF files


def _ocr_image(image_path):
    result = subprocess.run(
        ["tesseract", image_path,  "stdout"],
        capture_output=True,  # Capture the output of the command
        text=True,            # Return the output as a string instead of bytes
         check = False,        # check=True will raise a CalledProcessError if the command returns a non-zero exit code
    )
    return result.stdout

def ocr_selected_pages(pdf_path, page_numbers, zoom = 1.75):
    texts = {}
    # Open the PDF file
    document = fitz.open(pdf_path)
    temp_root = Path(tempfile.gettempdir()) / "ocr_temp"
    temp_root.mkdir(exist_ok=True)
    
    # Create a temporary directory to store the images
    with tempfile.TemporaryDirectory() as temp_dir:
       for page_number in page_numbers:
            if page_number < 0 or page_number >= document.page_count:
               continue  # Skip invalid page numbers

            page = document[page_number]
            image_path = Path(temp_dir) / f"page_{page_number+1}.png"
            page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False).save(image_path)  # Save the page as an image
            text = _ocr_image(image_path)  # Perform OCR on the image
            texts[page_number] = text      # Store the OCR result in the dictionary
    return texts


# This function returns a list of page numbers that are
#  commonly found in lease deeds
# . It includes the first few pages and the last page of the document, as these often contain important information such as the terms of the lease, signatures, and other relevant details.
def default_lease_deed_pages(pdf_path):  
    document = fitz.open(pdf_path)
    candidates = [0, 1, 2, 3, document.page_count - 1]
    return sorted({page for page in candidates if 0 <= page < document.page_count})
