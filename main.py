import os
import sys
import pypdf
import psycopg
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(override=True)

DB_HOST = "127.0.0.1"
DB_NAME = "rag_tools"
DB_USER = "rag_tools"
DB_PASSWORD = os.getenv("PG_PASSWORD")
DB_PORT = "5432"

def read_pdf(file_path):
    """
    Reads a PDF file and returns a list of dictionaries, where each dictionary
    contains information about a page (page number, text, character count, word count).

    Args:
        file_path (str): The path to the PDF file.

    Returns:
        list: A list of dictionaries, each representing a page with its details.
              Returns an empty list if the file cannot be opened or is not a valid PDF.
    """
    pages_data = []
    try:
        with open(file_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            num_pages = len(reader.pages)

            for i in tqdm(range(num_pages), desc="Reading document", unit=" page"):
                page = reader.pages[i]
                text = page.extract_text()
                
                # Handle cases where text extraction might return None or empty string
                if text is None:
                    text = ""

                text = text.replace("\x00", "\ufffd")
                
                num_characters = len(text)
                num_words = len(text.split())

                pages_data.append({
                    "pageNumber": i + 1,
                    "text": text,
                    "numCharacters": num_characters,
                    "numWords": num_words
                })
    except pypdf.errors.PdfReadError:
        print(f"Error: Could not read PDF file at {file_path}. It might be corrupted or not a valid PDF.")
        return []
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

    return pages_data

def store_documents(file_name, pages):
    """
    Stores document metadata in the 'documents' table and then
    stores each page's content in the 'raw_pages' table.

    Args:
        file_name (str): The name of the document file (e.g., "my_report.pdf").
        pages (list): A list of dictionaries, each containing page data
                      (from read_pdf function).
    """
    if not pages:
        print("No pages provided for storage. Rolling back document insertion.")
        return
    conn = None
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()

        created_at = datetime.now()

        print(f"Storing document '{file_name}'...")
        insert_document_query = """
        INSERT INTO documents (name, created_at)
        VALUES (%s, %s) RETURNING document_id;
        """
        cur.execute(insert_document_query, (file_name, created_at))
        document_id = cur.fetchone()[0] 
        print(f"Document '{file_name}' stored with document_id: {document_id}")

        print(f"Storing {len(pages)} pages into 'raw_pages' table...")
        insert_page_query = """
        INSERT INTO raw_pages (document_id, page_text, page_number, number_words, number_characters)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        page_records = []
        for page_info in pages:
            if page_info['text']:
                page_records.append((
                    document_id,
                    page_info['text'],
                    page_info['pageNumber'],
                    page_info['numWords'],
                    page_info['numCharacters']
                ))
        
        for record in tqdm(page_records, desc="Storing pages to DB", unit="page"):
             cur.execute(insert_page_query, record)

        conn.commit()
        print(f"Successfully stored document '{file_name}' and all its pages.")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback() 
            print("Transaction rolled back.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback() # Rollback for other errors too
            print("Transaction rolled back.")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("You should pass a name of a valid pdf file")
        sys.exit(1)

    pdf_file_path = sys.argv[1]
    
    if os.path.exists(pdf_file_path): 
        document_name = os.path.basename(pdf_file_path)

        print(f"\n--- Starting PDF processing for '{document_name}' ---")
        
        # Read the PDF pages
        extracted_pages = read_pdf(pdf_file_path)

        if extracted_pages:
            # Store the document and its pages
            store_documents(document_name, extracted_pages)
        else:
            print(f"No pages extracted from '{pdf_file_path}'. Nothing to store.")
        
        print(f"--- Finished processing PDF: '{document_name}' ---")

    else:
        print(f"\nError: The specified PDF file '{pdf_file_path}' was not found. Please ensure the path is correct.")
