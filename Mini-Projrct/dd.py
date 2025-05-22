import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell

def databricks_py_to_ipynb(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    cells = []
    current_cell = []
    in_md = False

    for line in lines:
        if line.strip().startswith("# COMMAND"):
            if current_cell:
                if in_md:
                    cells.append(new_markdown_cell("\n".join(current_cell)))
                else:
                    cells.append(new_code_cell("\n".join(current_cell)))
                current_cell = []
                in_md = False
            continue
        elif "# MAGIC %md" in line:
            in_md = True
            continue
        elif "# MAGIC" in line:
            current_cell.append(line.replace("# MAGIC ", ""))
        else:
            current_cell.append(line)

    if current_cell:
        if in_md:
            cells.append(new_markdown_cell("\n".join(current_cell)))
        else:
            cells.append(new_code_cell("\n".join(current_cell)))

    nb = new_notebook(cells=cells)
    with open(output_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)

# 사용 예시
databricks_py_to_ipynb("Exercise 02 - Batch Ingestion.py", "Exercise 02 - Batch Ingestion.ipynb")