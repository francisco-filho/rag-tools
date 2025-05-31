import os
import sys
import pypdf
import psycopg
import argparse
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

                # postgresql does not accept this 'null' character
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
        print("No pages provided for storage. Exiting...")
        return
    conn = None


    with psycopg.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            created_at = datetime.now()

            print(f"Storing document '{file_name}'...")
            insert_document_query = """
            INSERT INTO documents (name, created_at)
            VALUES (%s, %s) RETURNING document_id;
            """
            cur.execute(insert_document_query, (file_name, created_at))
            document_id = cur.fetchone()[0] 
            print(f"Document stored with document_id: {document_id}")

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

                #conn.commit() # with context, commit are automatic
    print(f"Successfully stored document")


def query(q, *args):
    with psycopg.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(q, args)
            return cur.fetchall()
    
    
def retrieve_pages_from_db(document_id: int):
    """
    Retrieves all pages for a given document_id from the 'raw_pages' table.

    Args:
        document_id (int): The ID of the document to retrieve pages for.

    Returns:
        list: A list of dictionaries, where each dictionary contains page data
              (text_id, document_id, page_text, page_number, number_words, number_characters).
              Returns an empty list if no pages are found or on error.
    """
    pages_data = []
    select_pages_query = """
    SELECT text_id, document_id, page_text, page_number, number_words, number_characters
    FROM raw_pages
    WHERE document_id = %s
    ORDER BY page_number;
    """
    
    rows = query(select_pages_query, document_id)
    
    for row in rows:
        pages_data.append({
            "text_id": row[0],
            "document_id": row[1],
            "page_text": row[2],
            "page_number": row[3],
            "number_words": row[4],
            "number_characters": row[5]
        })
    
    return pages_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PDF processing and database interaction script."
    )

    # Create a mutually exclusive group for --parse and --retrieve
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--parse",
        metavar="filepath",
        help="Path to the PDF file to read and store in the database."
    )

    group.add_argument(
        "--retrieve",
        metavar="document_id",
        type=int,
        help="Document ID to retrieve all pages from the database."
    )

    args = parser.parse_args()

    if args.parse:
        pdf_file_path = args.parse
        if os.path.exists(pdf_file_path): 
            document_name = os.path.basename(pdf_file_path)
            
            print(f"\n--- Starting PDF processing for '{document_name}' ---")
            extracted_pages = read_pdf(pdf_file_path)

            if extracted_pages:
                stored_doc_id = store_documents(document_name, extracted_pages)
                if stored_doc_id:
                    print(f"Document and pages stored successfully with Document ID: {stored_doc_id}")
                else:
                    print("Failed to store document and pages.")
            else:
                print(f"No pages extracted from '{pdf_file_path}'. Nothing to store.")
            print(f"--- Finished processing PDF: '{document_name}' ---")
        else:
            print(f"\nError: The specified PDF file '{pdf_file_path}' was not found. Please ensure the path is correct.")
            sys.exit(1)

    elif args.retrieve:
        doc_id_to_retrieve = args.retrieve
        print(f"\n--- Retrieving Pages for Document ID {doc_id_to_retrieve} ---")
        retrieved_data = retrieve_pages_from_db(doc_id_to_retrieve)
        if retrieved_data:
            print(f"\n--- Retrieved Pages for Document ID {doc_id_to_retrieve} ---")
            for page_info in retrieved_data[:3]:
                print(f"  Page {page_info['page_number']} (Text ID: {page_info['text_id']}):")
                print(f"    Chars: {page_info['number_characters']}, Words: {page_info['number_words']}")
                print("-" * 20)
        else:
            print(f"No pages found for Document ID {doc_id_to_retrieve} or an error occurred during retrieval.")

