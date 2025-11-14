import ipywidgets as widgets
from IPython.display import display
from scripts.splitter import run_split

def splitter_ui():
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
        with out:
            try:
                run_split(
                    input_path=input_path_text.value,
                    mode=mode_selector.value,
                    max_mb=mb_slider.value,
                    output_dir=output_dir_text.value,
                    base_name=base_name_text.value,
                    fmt=format_selector.value
                )
            except Exception as e:
                print("❌ Error al dividir dataset:", e)

    run_button.on_click(on_run_clicked)

    # Mostrar interfaz
    display(
        input_path_text, mode_selector, mb_slider, format_selector,
        output_dir_text, base_name_text, run_button, out
    )
