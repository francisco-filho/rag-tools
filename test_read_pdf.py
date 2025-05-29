import pytest
from main import read_pdf
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

@pytest.fixture(scope="module")
def sample_pdf(tmp_path_factory):
    """
    Creates a temporary PDF file for testing read_pdf function.
    The PDF will have two pages with known text.
    """
    pdf_path = tmp_path_factory.mktemp("data") / "test_document.pdf"
    
    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    
    # Page 1
    c.drawString(100, 750, "This is the first page.")
    c.drawString(100, 730, "Hello world!")
    c.showPage()
    
    # Page 2
    c.drawString(100, 750, "Second page content.")
    c.drawString(100, 730, "Pytest example.")
    c.save()
    
    yield pdf_path
    

def test_read_pdf_basic(sample_pdf):
    """
    Tests the basic functionality of read_pdf with a known sample PDF.
    Verifies the number of pages, character count, and word count.
    """
    # Use the path from the fixture
    pdf_data = read_pdf(str(sample_pdf))

    # Assert that data was extracted
    assert pdf_data is not None
    assert isinstance(pdf_data, list)
    assert len(pdf_data) == 2  # Expecting 2 pages from our sample_pdf

    # Verify data for Page 1
    page1 = pdf_data[0]
    assert page1["pageNumber"] == 1
    assert "This is the first page." in page1["text"]
    assert "Hello world!" in page1["text"]
    
    # Simple word count check for page 1
    # Note: text extraction can sometimes include extra whitespace/newlines.
    # We'll use a more robust check for word count.
    assert page1["numCharacters"] > 0
    assert page1["numWords"] >= len("This is the first page. Hello world!".split())

    # Verify data for Page 2
    page2 = pdf_data[1]
    assert page2["pageNumber"] == 2
    assert "Second page content." in page2["text"]
    assert "Pytest example." in page2["text"]

    assert page2["numCharacters"] > 0
    assert page2["numWords"] >= len("Second page content. Pytest example.".split())

def test_read_pdf_non_existent_file():
    """
    Tests read_pdf with a path to a non-existent file.
    It should return an empty list.
    """
    non_existent_path = "non_existent_file.pdf"
    data = read_pdf(non_existent_path)
    assert data == []

def test_read_pdf_invalid_file_type(tmp_path):
    """
    Tests read_pdf with a file that is not a valid PDF (e.g., a text file).
    It should return an empty list.
    """
    invalid_file_path = tmp_path / "invalid_file.txt"
    with open(invalid_file_path, "w") as f:
        f.write("This is not a PDF.")
    
    data = read_pdf(str(invalid_file_path))
    assert data == []
