#!/usr/bin/env python3
import fitz
import os

def remove_images_from_pdf(input_path, output_path):
    """Remove all images from a PDF file by extracting and recreating without image objects."""
    try:
        doc = fitz.open(input_path)
        new_doc = fitz.open()
        total_images = 0
        
        print(f"Processing: {os.path.basename(input_path)}")
        
        for page_num in range(doc.page_count):
            page = doc[page_num]
            
            # Get image count
            images = page.get_images()
            image_count = len(images) if images else 0
            
            if image_count > 0:
                total_images += image_count
                print(f"  Page {page_num + 1}: Found {image_count} image(s)")
            
            # Create new page with same dimensions
            rect = page.rect
            new_page = new_doc.new_page(-1, width=rect.width, height=rect.height)
            
            # Extract text blocks and redraw (excluding images)
            text_dict = page.get_text("dict")
            
            for block in text_dict.get("blocks", []):
                # Skip image blocks
                if block.get("type") == 1:  # 1 = image block
                    continue
                
                # Process text blocks
                if block.get("type") == 0 and "lines" in block:  # 0 = text block
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            text_content = span.get("text", "")
                            if text_content and text_content.strip():
                                bbox = span["bbox"]
                                fontsize = span.get("size", 11)
                                font_name = span.get("font", "helv")
                                
                                # Insert text at position
                                new_page.insert_text(
                                    (bbox[0], bbox[1] + fontsize * 0.75),
                                    text_content,
                                    fontsize=fontsize,
                                    color=(0, 0, 0),
                                )
        
        # Save the modified PDF
        new_doc.save(output_path)
        new_doc.close()
        doc.close()
        
        print(f"  ✓ Saved without images: {os.path.basename(output_path)}")
        print(f"  Total images removed: {total_images}\n")
        
    except Exception as e:
        print(f"ERROR processing {input_path}: {e}\n")

# Process files
files_to_process = [
    (
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria.pdf",
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria_no_images.pdf"
    ),
    (
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria_part01.pdf",
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria_part01_no_images.pdf"
    ),
    (
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria_part02.pdf",
        r"C:\Users\algoa\OneDrive\Escritorio\Supervisio\release_splitter\parts_projecte\projecte_memoria_part02_no_images.pdf"
    ),
]

for input_file, output_file in files_to_process:
    if os.path.exists(input_file):
        remove_images_from_pdf(input_file, output_file)
    else:
        print(f"File not found: {input_file}\n")

print("Done!")
