#!/usr/bin/env python3
import fitz
import os

input_pdf = r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria.pdf"
output_dir = r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte"
base_name = "projecte_memoria"
max_pages = 30

# Open the PDF
doc = fitz.open(input_pdf)
total_pages = doc.page_count

print(f"Total pages: {total_pages}")

# Calculate number of chunks needed
num_chunks = (total_pages + max_pages - 1) // max_pages
pages_per_chunk = total_pages // num_chunks

print(f"Creating {num_chunks} chunks with ~{pages_per_chunk} pages each")

# Split the PDF
for i in range(num_chunks):
    start_page = i * pages_per_chunk
    if i == num_chunks - 1:
        # Last chunk gets remaining pages
        end_page = total_pages
    else:
        end_page = (i + 1) * pages_per_chunk
    
    # Create new PDF with subset of pages
    new_doc = fitz.open()
    for page_num in range(start_page, end_page):
        new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
    
    # Save with numbered suffix
    output_name = f"{base_name}_part{i+1:02d}.pdf"
    output_path = os.path.join(output_dir, output_name)
    new_doc.save(output_path)
    new_doc.close()
    
    actual_pages = end_page - start_page
    print(f"  ✓ {output_name}: pages {start_page+1}-{end_page} ({actual_pages} pages)")

doc.close()
print(f"Done! Files created in: {output_dir}")
