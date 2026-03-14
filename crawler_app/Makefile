OUT_DIR = outputs

run: build_dir
	python app.py

build_dir:
	mkdir -p $(OUT_DIR)

install:
	pip install -r requirements.txt

clean:
	rm -rf $(OUT_DIR)/*.csv