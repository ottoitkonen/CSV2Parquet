build-lnx:
	@echo "Building the project..."
	@rm -rf build dist
	@pyinstaller app/main.py --name CSV2Parquet --onefile --clean --optimize 2
	@echo "Build complete. The executable is located in the dist directory."

build-win:
	@echo "Building the project..."
	@rmdir /s /q "build" "dist"
	@pyinstaller app/main.py --name CSV2Parquet --onefile --clean --optimize 2
	@echo "Build complete. The executable is located in the dist directory."

install:
	@echo "Installing Poetry..."
	@pip install poetry==1.8.3
	@echo "Installing dependencies..."
	@poetry install
	@echo "Dependencies installed."

run:
	@poetry run python app/main.py
