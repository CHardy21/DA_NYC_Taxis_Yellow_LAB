import ipywidgets as widgets
from IPython.display import display
import subprocess

# Widgets
mode_selector = widgets.Dropdown(options=["rows", "dynamic"], value="rows", description="Modo:")
mb_slider = widgets.IntSlider(value=100, min=10, max=200, step=10, description="Max MB:")
format_selector = widgets.Dropdown(options=["csv", "parquet"], value="csv", description="Formato:")
output_dir_text = widgets.Text(value="splits", description="Output dir:")
base_name_text = widgets.Text(value="part", description="Base name:")
input_path_text = widgets.Text(value="dataset.csv", description="Input file:")
run_button = widgets.Button(description="Dividir Dataset", button_style="success")
out = widgets.Output()

def on_run_clicked(b):
    out.clear_output()
    cmd = [
        "python", "scripts/splitter.py",
        "--input", input_path_text.value,
        "--mode", mode_selector.value,
        "--max-mb", str(mb_slider.value),
        "--output-dir", output_dir_text.value,
        "--base-name", base_name_text.value,
        "--fmt", format_selector.value
    ]
    with out:
        process = subprocess.Popen(cmd, cwd=project_root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line.strip())
run_button.on_click(on_run_clicked)
# Mostrar interfaz
display(input_path_text, mode_selector, mb_slider, format_selector, output_dir_text, base_name_text, run_button, out)